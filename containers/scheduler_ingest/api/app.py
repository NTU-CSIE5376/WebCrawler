from __future__ import annotations

import os
import errno
import logging

import psycopg2
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, StrictStr
from starlette.responses import JSONResponse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, TimeoutError as PoolTimeoutError
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml
from libs.obslog import configure
from libs.urls import MAX_URL_LEN
from .service import SubmissionService, validate_urls
from .middleware import RequestLimitsMiddleware


logger = logging.getLogger("scheduler_ingest_api")


class CrawlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    urls: list[StrictStr]


def temporary_storage_error(error: Exception) -> bool:
    if isinstance(error, PoolTimeoutError):
        return True
    if isinstance(error, OSError):
        return error.errno in {
            errno.ENOSPC, errno.EDQUOT, errno.EMFILE, errno.ENFILE,
            errno.EAGAIN, errno.EBUSY, errno.ETIMEDOUT,
            errno.ECONNREFUSED, errno.ECONNRESET,
        }
    underlying = getattr(error, "orig", error)
    code = getattr(underlying, "pgcode", None)
    if code:
        return code.startswith(("08", "53")) or code in {
            "40001", "40P01", "55P03", "57014", "57P01", "57P02", "57P03",
        }
    return isinstance(underlying, (psycopg2.OperationalError, psycopg2.InterfaceError))


def create_app(config_path: str | None = None, *, service=None) -> FastAPI:
    app = FastAPI(title="scheduler_ingest API", root_path="/scheduler_ingest")
    config_path = config_path or os.environ.get(
        "INGEST_CONFIG_PATH", "/app/containers/scheduler_ingest/config/ingest.yaml"
    )
    config = load_yaml(config_path)
    api_config = config.get("api", {})
    max_body_size = int(api_config.get("max_content_length", 1_048_576))
    app.add_middleware(RequestLimitsMiddleware, max_body_size=max_body_size)
    max_urls = int(os.environ.get("CRAWL_REQUEST_MAX_URLS", api_config.get("max_urls", 1000)))
    max_url_length = int(os.environ.get(
        "CRAWL_REQUEST_MAX_URL_LENGTH", api_config.get("max_url_length", MAX_URL_LEN),
    ))
    if max_urls < 1:
        raise ValueError("CRAWL_REQUEST_MAX_URLS must be positive")
    if not 1 <= max_url_length <= MAX_URL_LEN:
        raise ValueError(f"CRAWL_REQUEST_MAX_URL_LENGTH must be between 1 and {MAX_URL_LEN}")

    if service is None:
        configure(service="scheduler_ingest_api")
        engine = create_engine(
            config["postgres"]["dsn"], pool_pre_ping=True,
            pool_size=2, max_overflow=1, pool_timeout=10,
            connect_args={"connect_timeout": 5},
        )
        service = SubmissionService(sessionmaker(bind=engine), config, config_path)
        app.state.submission_engine = engine
    app.state.submission_service = service

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.post("/v1/crawl-requests", summary="Create a crawl request", responses={
        400: {"description": "Invalid request"},
        413: {"description": "Request body too large"},
        415: {"description": "JSON content type required"},
        500: {"description": "Internal error; no URLs accepted"},
        503: {"description": "Temporarily unavailable; no URLs accepted"},
        "default": {"description": "Error response"},
    })
    def create_crawl_request(payload: CrawlRequest):
        """Accept a batch of URLs through the crawl-request API."""
        try:
            urls = validate_urls(payload.model_dump(), max_urls, max_url_length)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        try:
            service.submit(urls)
            return {"status": "ok"}
        except (SQLAlchemyError, psycopg2.Error, OSError) as exc:
            logger.exception("submission.storage_failed")
            if temporary_storage_error(exc):
                return JSONResponse({"error": "Submission unavailable; no URLs accepted"}, status_code=503)
            return JSONResponse({"error": "Submission failed; no URLs accepted"}, status_code=500)
        except Exception:
            logger.exception("submission.failed")
            return JSONResponse({"error": "Submission failed; no URLs accepted"}, status_code=500)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, error):
        return JSONResponse({"error": "Invalid request body"}, status_code=400)

    return app

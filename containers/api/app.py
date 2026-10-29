"""FastAPI entry point for the crawler's external API."""
from __future__ import annotations

import errno
import logging

import psycopg2
from fastapi import Depends, FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from sqlalchemy.exc import SQLAlchemyError, TimeoutError as PoolTimeoutError
from starlette.exceptions import HTTPException

from libs.obslog import configure
from libs.urls import MAX_URL_LEN
from libs.ipc.manual_bulk_crawl import (
    DEFAULT_TTL_DAYS, MAX_TTL_DAYS,
)
from .body_limit import RequestBodyLimitMiddleware
from .config_loader import api_config_path, load_api_config
from .routing import ManualBulkCrawlRouter
from .service import JobNotCompleted, ManualBulkCrawlService, validate_manual_bulk_crawl_request


logger = logging.getLogger("api")


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


class ManualBulkCrawlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    urls: list[StrictStr]
    ttl_days: StrictInt = Field(default=DEFAULT_TTL_DAYS, ge=1, le=MAX_TTL_DAYS)


def require_json(request: Request):
    media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if not (media_type == "application/json"
            or (media_type.startswith("application/") and media_type.endswith("+json"))):
        raise HTTPException(415, "Content-Type must be application/json")


def create_app(config_path: str | None = None, *, service=None) -> FastAPI:
    app = FastAPI(title="Crawler API", redirect_slashes=False)
    config_path = api_config_path(config_path)
    config = load_api_config(config_path)
    max_content_length = config.get("max_content_length", 300 * 1024 * 1024)
    if isinstance(max_content_length, bool) or not isinstance(max_content_length, int) or max_content_length < 1:
        raise ValueError("max_content_length must be a positive integer")
    app.state.max_content_length = max_content_length
    app.add_middleware(RequestBodyLimitMiddleware, max_bytes=max_content_length)
    crawl_config = config["manual_bulk_crawl"]
    max_urls = int(crawl_config.get("max_urls", 10000))
    max_url_length = int(crawl_config.get("max_url_length", MAX_URL_LEN))
    if max_urls < 1:
        raise ValueError("manual_bulk_crawl.max_urls must be positive")
    if not 1 <= max_url_length <= MAX_URL_LEN:
        raise ValueError(f"manual_bulk_crawl.max_url_length must be between 1 and {MAX_URL_LEN}")

    if service is None:
        configure(service="api")
        service = ManualBulkCrawlService(
            crawl_config, route_urls=ManualBulkCrawlRouter(config),
        )
    app.state.manual_bulk_crawl = service

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.post("/v1/manual-bulk-crawls", dependencies=[Depends(require_json)])
    def create_manual_bulk_crawl(payload: ManualBulkCrawlRequest):
        # Synchronous handlers run in FastAPI's thread pool, including routing
        # and file operations. Shared validation runs before any work is published.
        payload = payload.model_dump()
        try:
            urls, ttl_days = validate_manual_bulk_crawl_request(payload, max_urls, max_url_length)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        try:
            return {"job_id": service.submit(urls, ttl_days=ttl_days)}
        except (OSError, SQLAlchemyError, psycopg2.Error) as exc:
            logger.exception("submission.storage_failed")
            if temporary_storage_error(exc):
                return JSONResponse({"error": "Submission unavailable; no URLs accepted"}, status_code=503)
            return JSONResponse({"error": "Submission failed; no URLs accepted"}, status_code=500)
        except Exception:
            logger.exception("submission.failed")
            return JSONResponse({"error": "Submission failed; no URLs accepted"}, status_code=500)

    @app.delete("/v1/manual-bulk-crawls/{job_id}", status_code=204)
    def delete_manual_bulk_crawl(job_id: str):
        service.delete(job_id)
        return Response(status_code=204)

    @app.get("/v1/manual-bulk-crawls/{job_id}")
    def manual_bulk_crawl_status(job_id: str):
        try:
            status = service.status(job_id)
            status.pop("result_dir", None)
            status["results_url"] = (
                str(app.url_path_for("list_manual_bulk_crawl_results", job_id=job_id))
                if status["status"] == "completed" else None
            )
            return status
        except FileNotFoundError:
            return JSONResponse({"error": "Job not found"}, status_code=404)
        except OSError:
            logger.exception("manual_bulk_crawl.status_unavailable")
            return JSONResponse({"error": "Job status unavailable"}, status_code=503)

    @app.get("/v1/manual-bulk-crawls/{job_id}/results")
    def list_manual_bulk_crawl_results(job_id: str):
        files = service.result_files(job_id)
        for file in files:
            file["download_url"] = str(app.url_path_for(
                "download_manual_bulk_crawl_result", job_id=job_id, filename=file["name"],
            ))
        return {"job_id": job_id, "files": files}

    @app.get("/v1/manual-bulk-crawls/{job_id}/results/{filename}")
    def download_manual_bulk_crawl_result(job_id: str, filename: str):
        path = service.result_file(job_id, filename)
        return FileResponse(path.resolve(), filename=filename,
                            media_type="application/octet-stream",
                            headers={"X-Content-Type-Options": "nosniff"})

    @app.exception_handler(JobNotCompleted)
    def incomplete_job(request: Request, error):
        return JSONResponse({"error": "Job results are not ready"}, status_code=409)

    @app.exception_handler(FileNotFoundError)
    def missing_result(request: Request, error):
        return JSONResponse({"error": "Job or result file not found"}, status_code=404)

    @app.exception_handler(OSError)
    def unavailable_result(request: Request, error):
        logger.error("manual_bulk_crawl.results_unavailable", exc_info=error)
        return JSONResponse({"error": "Job results unavailable"}, status_code=503)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, error):
        return JSONResponse({"error": "Invalid request body"}, status_code=400)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error):
        messages = {400: "Invalid request body", 413: "Request body too large",
                    415: "Content-Type must be application/json"}
        return JSONResponse({"error": messages.get(error.status_code, error.detail)},
                            status_code=error.status_code, headers=error.headers)

    return app

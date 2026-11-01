"""FastAPI entry point for the crawler's external API."""
from __future__ import annotations

import asyncio
import errno
import hmac
import logging
from typing import Any

import psycopg2
from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from libs.config.loader import load_yaml
from libs.obslog import configure
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StrictStr
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException

from .body_limit import RequestBodyLimitMiddleware
from .config_loader import api_config_path, load_api_config
from .crawl_requests import (
    CrawlRequestService,
    RequestClosed,
    RequestNotFound,
    Unavailable,
)
from .routing import ManualBulkCrawlRouter

logger = logging.getLogger("api")

SQLITE_INT_MIN, SQLITE_INT_MAX = -(2 ** 63), 2 ** 63 - 1


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


class CreateCrawlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: StrictStr = Field(min_length=1)
    result_quota_gb: StrictInt | StrictFloat | None = Field(default=None, gt=0)
    expires_days: StrictInt | None = Field(default=None, ge=1)


class UrlItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: StrictStr
    # Any value: a bad URL skips its item as "invalid" instead of failing the batch.
    url: Any
    priority: StrictInt = Field(ge=SQLITE_INT_MIN, le=SQLITE_INT_MAX)


class UrlBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: StrictStr = Field(min_length=1)
    urls: list[UrlItem]


class AckSegments(BaseModel):
    model_config = ConfigDict(extra="forbid")

    segments: list[StrictStr]


def require_json(request: Request):
    media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if not (media_type == "application/json"
            or (media_type.startswith("application/") and media_type.endswith("+json"))):
        raise HTTPException(415, "Content-Type must be application/json")


def load_tokens(path: str) -> dict[str, str]:
    tokens = load_yaml(path)
    if not isinstance(tokens, dict) or not tokens or not all(
            isinstance(token, str) and token and isinstance(tenant, str) and tenant
            for token, tenant in tokens.items()):
        raise ValueError("crawl_requests.tokens_file must map non-empty tokens to tenant names")
    return tokens


def create_app(config_path: str | None = None, *, service=None, tokens=None) -> FastAPI:
    app = FastAPI(title="Crawler API", redirect_slashes=False)
    config_path = api_config_path(config_path)
    config = load_api_config(config_path)
    max_content_length = config.get("max_content_length", 300 * 1024 * 1024)
    if isinstance(max_content_length, bool) or not isinstance(max_content_length, int) or max_content_length < 1:
        raise ValueError("max_content_length must be a positive integer")
    app.state.max_content_length = max_content_length
    app.add_middleware(RequestBodyLimitMiddleware, max_bytes=max_content_length)
    crawl_config = config["crawl_requests"]
    if tokens is None:
        tokens = load_tokens(crawl_config["tokens_file"])
    if service is None:
        configure(service="api")
        service = CrawlRequestService(crawl_config, route_urls=ManualBulkCrawlRouter(config))
    app.state.crawl_requests = service
    retry_after = {"Retry-After": str(service.retry_after_s)}

    def authenticate(request: Request) -> str:
        scheme, _, token = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() == "bearer" and token:
            for known, name in tokens.items():
                if hmac.compare_digest(known.encode(), token.strip().encode()):
                    return name
        raise HTTPException(401, "Missing or unknown bearer token", headers={"WWW-Authenticate": "Bearer"})

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.post("/v1/crawl-requests", status_code=201, dependencies=[Depends(require_json)])
    def create_crawl_request(payload: CreateCrawlRequest, tenant: str = Depends(authenticate)):
        return service.create(tenant, payload.name, payload.result_quota_gb, payload.expires_days)

    @app.get("/v1/crawl-requests")
    def list_crawl_requests(tenant: str = Depends(authenticate)):
        return {"requests": service.list(tenant)}

    @app.post("/v1/crawl-requests/{request_id}/urls", status_code=202, dependencies=[Depends(require_json)])
    def submit_urls(request_id: str, payload: UrlBatch, tenant: str = Depends(authenticate)):
        # Synchronous handlers run in FastAPI's thread pool, including routing and file writes.
        if not 1 <= len(payload.urls) <= service.max_urls:
            return JSONResponse({"error": f'"urls" must contain 1 to {service.max_urls} items'},
                                status_code=400)
        return service.submit(tenant, request_id, payload.batch_id,
                              [item.model_dump() for item in payload.urls])

    @app.post("/v1/crawl-requests/{request_id}/seal")
    def seal_crawl_request(request_id: str, tenant: str = Depends(authenticate)):
        service.seal(tenant, request_id)
        return {"sealed": True}

    @app.delete("/v1/crawl-requests/{request_id}", status_code=202)
    def cancel_crawl_request(request_id: str, tenant: str = Depends(authenticate)):
        service.cancel(tenant, request_id)
        return {"cancelled": True}

    @app.get("/v1/crawl-requests/{request_id}")
    def crawl_request_status(request_id: str, tenant: str = Depends(authenticate)):
        return service.status(tenant, request_id)

    @app.get("/v1/crawl-requests/{request_id}/segments")
    async def list_segments(request_id: str, wait: float = Query(0, ge=0), tenant: str = Depends(authenticate)):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + min(wait, service.long_poll_max_s)
        while True:
            segments = await run_in_threadpool(service.list_segments, tenant, request_id)
            remaining = deadline - loop.time()
            if segments or remaining <= 0:
                return {"segments": segments}
            # Sleep on the event loop so a long poll holds no worker thread.
            await asyncio.sleep(min(service.long_poll_interval_s, remaining))

    @app.get("/v1/crawl-requests/{request_id}/segments/{name}")
    def download_segment(request_id: str, name: str, tenant: str = Depends(authenticate)):
        return FileResponse(service.segment_path(tenant, request_id, name).resolve(),
                            media_type="application/gzip", filename=name,
                            headers={"X-Content-Type-Options": "nosniff"})

    @app.post("/v1/crawl-requests/{request_id}/ack", dependencies=[Depends(require_json)])
    def ack_segments(request_id: str, payload: AckSegments, tenant: str = Depends(authenticate)):
        return {"deleted": service.ack(tenant, request_id, payload.segments)}

    @app.exception_handler(RequestNotFound)
    def request_not_found(request: Request, error):
        return JSONResponse({"error": "Crawl request or segment not found"}, status_code=404)

    @app.exception_handler(RequestClosed)
    def request_closed(request: Request, error):
        return JSONResponse({"error": "Crawl request is sealed, cancelled or expired"}, status_code=409)

    @app.exception_handler(Unavailable)
    def unavailable(request: Request, error):
        return JSONResponse({"error": "Submission unavailable; retry later", "reason": str(error)},
                            status_code=503, headers=retry_after)

    @app.exception_handler(OSError)
    def storage_error(request: Request, error):
        logger.error("crawl_requests.storage_failed", exc_info=error)
        if temporary_storage_error(error):
            return JSONResponse({"error": "Storage unavailable; retry later"}, status_code=503,
                                headers=retry_after)
        return JSONResponse({"error": "Storage failed"}, status_code=500)

    @app.exception_handler(SQLAlchemyError)
    def database_error(request: Request, error):
        logger.error("crawl_requests.database_failed", exc_info=error)
        return JSONResponse({"error": "Routing unavailable; retry later"}, status_code=503,
                            headers=retry_after)

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

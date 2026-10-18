"""Flask entry point for the crawler's external API."""
from __future__ import annotations

import errno
import logging

import psycopg2
from flask import Flask, jsonify, request, send_file, url_for
from sqlalchemy.exc import SQLAlchemyError, TimeoutError as PoolTimeoutError
from werkzeug.exceptions import HTTPException

from libs.obslog import configure
from libs.urls import MAX_URL_LEN
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


def create_app(config_path: str | None = None, *, service=None) -> Flask:
    app = Flask(__name__, static_folder=None)
    config_path = api_config_path(config_path)
    config = load_api_config(config_path)
    max_content_length = config.get("max_content_length", 300 * 1024 * 1024)
    if isinstance(max_content_length, bool) or not isinstance(max_content_length, int) or max_content_length < 1:
        raise ValueError("max_content_length must be a positive integer")
    app.config["MAX_CONTENT_LENGTH"] = max_content_length
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
    app.extensions["manual_bulk_crawl"] = service

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.post("/v1/manual-bulk-crawls")
    def create_manual_bulk_crawl():
        # Flask handles JSON parsing and media types; shared validation checks
        # the entire batch before routing or publishing any work.
        payload = request.get_json()
        try:
            urls, ttl_days = validate_manual_bulk_crawl_request(payload, max_urls, max_url_length)
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        try:
            return {"job_id": service.submit(urls, ttl_days=ttl_days)}
        except (OSError, SQLAlchemyError, psycopg2.Error) as exc:
            logger.exception("submission.storage_failed")
            if temporary_storage_error(exc):
                return jsonify(error="Submission unavailable; no URLs accepted"), 503
            return jsonify(error="Submission failed; no URLs accepted"), 500
        except Exception:
            logger.exception("submission.failed")
            return jsonify(error="Submission failed; no URLs accepted"), 500

    @app.delete("/v1/manual-bulk-crawls/<job_id>")
    def delete_manual_bulk_crawl(job_id):
        service.delete(job_id)
        return "", 204

    @app.get("/v1/manual-bulk-crawls/<job_id>")
    def manual_bulk_crawl_status(job_id):
        try:
            status = service.status(job_id)
            status["results_url"] = (
                url_for("list_manual_bulk_crawl_results", job_id=job_id)
                if status["status"] == "completed" else None
            )
            return status
        except FileNotFoundError:
            return jsonify(error="Job not found"), 404
        except OSError:
            logger.exception("manual_bulk_crawl.status_unavailable")
            return jsonify(error="Job status unavailable"), 503

    @app.get("/v1/manual-bulk-crawls/<job_id>/results")
    def list_manual_bulk_crawl_results(job_id):
        files = service.result_files(job_id)
        for file in files:
            file["download_url"] = url_for(
                "download_manual_bulk_crawl_result", job_id=job_id, filename=file["name"],
            )
        return {"job_id": job_id, "files": files}

    @app.get("/v1/manual-bulk-crawls/<job_id>/results/<filename>")
    def download_manual_bulk_crawl_result(job_id, filename):
        path = service.result_file(job_id, filename)
        response = send_file(path.resolve(), as_attachment=True, download_name=filename,
                             mimetype="application/octet-stream", conditional=True)
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.errorhandler(JobNotCompleted)
    def incomplete_job(error):
        return jsonify(error="Job results are not ready"), 409

    @app.errorhandler(FileNotFoundError)
    def missing_result(error):
        return jsonify(error="Job or result file not found"), 404

    @app.errorhandler(OSError)
    def unavailable_result(error):
        logger.error("manual_bulk_crawl.results_unavailable", exc_info=error)
        return jsonify(error="Job results unavailable"), 503

    @app.errorhandler(HTTPException)
    def http_error(error):
        response = error.get_response()
        messages = {400: "Invalid request body", 413: "Request body too large",
                    415: "Content-Type must be application/json"}
        response.data = app.json.dumps({"error": messages.get(error.code, error.name)})
        response.content_type = "application/json"
        return response

    return app

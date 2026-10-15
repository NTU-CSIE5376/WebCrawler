from __future__ import annotations

import os
import errno

import psycopg2
from flask import Flask, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, TimeoutError as PoolTimeoutError
from sqlalchemy.orm import sessionmaker
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge, UnsupportedMediaType

from libs.config.loader import load_yaml
from libs.obslog import configure
from .service import SubmissionService, validate_urls


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
    app = Flask(__name__)
    config_path = config_path or os.environ.get(
        "INGEST_CONFIG_PATH", "/app/containers/scheduler_ingest/config/ingest.yaml"
    )
    config = load_yaml(config_path)
    api_config = config.get("api", {})
    app.config["MAX_CONTENT_LENGTH"] = int(api_config.get("max_content_length", 1_048_576))
    max_urls = int(api_config.get("max_urls", 1000))

    if service is None:
        configure(service="url_submission_api")
        engine = create_engine(
            config["postgres"]["dsn"], pool_pre_ping=True,
            pool_size=2, max_overflow=1, pool_timeout=10,
            connect_args={"connect_timeout": 5},
        )
        service = SubmissionService(sessionmaker(bind=engine), config, config_path)
        app.extensions["submission_engine"] = engine
    app.extensions["submission_service"] = service

    @app.get("/healthz")
    def health():
        return jsonify(status="ok")

    @app.post("/v1/urls")
    def submit_urls():
        try:
            urls = validate_urls(request.get_json(), max_urls)
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        try:
            service.submit(urls)
            return jsonify(status="ok"), 200
        except (SQLAlchemyError, psycopg2.Error, OSError) as exc:
            app.logger.exception("submission.storage_failed")
            if temporary_storage_error(exc):
                return jsonify(error="Submission unavailable; no URLs accepted"), 503
            return jsonify(error="Submission failed; no URLs accepted"), 500
        except Exception:
            app.logger.exception("submission.failed")
            return jsonify(error="Submission failed; no URLs accepted"), 500

    @app.errorhandler(BadRequest)
    def bad_json(error):
        return jsonify(error="Invalid JSON body"), 400

    @app.errorhandler(UnsupportedMediaType)
    def unsupported_media(error):
        return jsonify(error="Content-Type must be application/json"), 415

    @app.errorhandler(RequestEntityTooLarge)
    def too_large(error):
        return jsonify(error="Request body too large"), 413

    return app

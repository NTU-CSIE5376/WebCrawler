"""Bound request bodies before FastAPI parses JSON, including chunked uploads."""
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse


class RequestBodyLimitMiddleware:
    def __init__(self, app, *, max_bytes):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        headers = dict(scope["headers"])
        try:
            length = int(headers.get(b"content-length", b"0"))
            if length < 0:
                raise ValueError
        except ValueError:
            response = JSONResponse({"error": "Invalid request body"}, status_code=400)
            return await response(scope, receive, send)
        if length > self.max_bytes:
            response = JSONResponse({"error": "Request body too large"}, status_code=413)
            return await response(scope, receive, send)

        received = 0

        async def bounded_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise HTTPException(413, "Request body too large")
            return message

        await self.app(scope, bounded_receive, send)

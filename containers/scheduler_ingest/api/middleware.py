"""Bound request bodies before parsing JSON or performing any submission work."""
from collections import deque

from starlette.datastructures import Headers
from starlette.responses import JSONResponse


class RequestLimitsMiddleware:
    def __init__(self, app, max_body_size: int):
        self.app = app
        self.max_body_size = max_body_size

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] not in {"POST", "PUT", "PATCH"}:
            return await self.app(scope, receive, send)
        content_type = Headers(scope=scope).get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json" and not (
            content_type.startswith("application/") and content_type.endswith("+json")
        ):
            return await JSONResponse({"error": "Content-Type must be application/json"}, status_code=415)(scope, receive, send)
        messages = deque()
        size = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            size += len(message.get("body", b""))
            if size > self.max_body_size:
                return await JSONResponse({"error": "Request body too large"}, status_code=413)(scope, receive, send)
            messages.append(message)
            if not message.get("more_body", False):
                break

        async def replay():
            return messages.popleft() if messages else await receive()

        await self.app(scope, replay, send)

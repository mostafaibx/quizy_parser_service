"""
GCP Cloud Function entry point - Optimized for cold starts
"""
import functions_framework
import json
import traceback
import time
import logging

# Configure logging at module level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Lazy-loaded FastAPI app
_app = None

def get_app():
    """Lazy load FastAPI app only when needed"""
    global _app  # pylint: disable=global-statement
    if _app is None:
        from app.api import create_app as _create_app
        _app = _create_app()
    return _app


@functions_framework.http
def parse_service(request):
    """
    Main entry point for GCP Cloud Function
    Optimized for minimal cold start time
    """
    start_time = time.time()

    try:
        # Quick health check - no FastAPI needed
        if request.path == "/health":
            return handle_health_check(start_time)

        # For all other routes (including OPTIONS), use FastAPI
        # FastAPI's CORS middleware handles OPTIONS automatically
        fastapi_app = get_app()
        return handle_fastapi_request(fastapi_app, request)

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Request failed: %s\n%s", str(e), traceback.format_exc())
        return json.dumps({"error": str(e)}), 500, {"Content-Type": "application/json"}


def handle_health_check(start_time: float) -> tuple:
    """Quick health check without loading FastAPI"""
    return json.dumps({
        "status": "healthy",
        "response_time_ms": int((time.time() - start_time) * 1000),
        "service": "quizy-parser",
        "version": "1.0.0"
    }), 200, {"Content-Type": "application/json"}


def handle_fastapi_request(fastapi_app, request) -> tuple:
    """Process request through FastAPI using ASGI"""
    import asyncio
    
    async def _call_app():
        # Convert Cloud Function request to ASGI scope
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "scheme": request.scheme or "https",
            "path": request.path or "/",
            "query_string": request.query_string.encode() if request.query_string else b"",
            "root_path": "",
            "headers": [(k.lower().encode(), v.encode()) for k, v in request.headers.items()],
            "server": (request.host.split(":")[0] if request.host else "localhost", 
                      int(request.host.split(":")[1]) if request.host and ":" in request.host else 443),
        }
        
        # Create request body
        body = request.get_data() if request.method != "GET" else b""
        
        async def receive():
            return {"type": "http.request", "body": body}
        
        # Capture response
        status_code = 200
        response_headers = []
        response_body = []
        
        async def send(message):
            nonlocal status_code, response_headers, response_body
            
            if message["type"] == "http.response.start":
                status_code = message["status"]
                response_headers = message.get("headers", [])
            elif message["type"] == "http.response.body":
                response_body.append(message.get("body", b""))
        
        # Call the ASGI app
        await fastapi_app(scope, receive, send)
        
        return b"".join(response_body), status_code, dict(response_headers)
    
    # Run async function
    body, status, headers = asyncio.run(_call_app())
    
    return body, status, headers


# For local development
if __name__ == "__main__":
    import uvicorn
    from app.api import create_app

    local_app = create_app()
    uvicorn.run(local_app, host="0.0.0.0", port=8080, log_level="info")
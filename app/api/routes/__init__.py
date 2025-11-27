"""API routes module"""

from app.api.routes.health import router as health_router
from app.api.routes.parser import router as parser_router
from app.api.routes.qstash_parser import router as qstash_router

__all__ = ["health_router", "parser_router", "qstash_router"]
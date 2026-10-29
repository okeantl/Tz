from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from src.core.config import get_settings
from src.core.database import engine
from src.core.exceptions import AppError
from src.core.logging import configure_logging
from src.modules.payments.router import router as payments_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


settings = get_settings()
configure_logging(settings.LOG_LEVEL)
app = FastAPI(title="Payments", lifespan=lifespan)


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


app.include_router(payments_router)


@app.get("/health")
async def get_health():
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as e:
        return JSONResponse(
            content={"postgres": f"error: {e}"}, status_code=503
        )
    return {"postgres": "ok"}

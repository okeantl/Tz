import asyncio
import os
import sys

os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://test:test@localhost:5432/test")
os.environ.setdefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
os.environ.setdefault("API_KEY", "test-api-key")

import pytest
import pytest_asyncio
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from alembic import command

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16", driver="psycopg") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def apply_migrations(postgres_container):
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_container.get_connection_url())
    command.upgrade(config, "head")
    yield


@pytest_asyncio.fixture
async def session_factory(postgres_container, apply_migrations):
    engine = create_async_engine(
        postgres_container.get_connection_url(driver="psycopg")
    )
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(session_factory):
    async with session_factory() as session:
        yield session

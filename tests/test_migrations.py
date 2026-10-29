from sqlalchemy import create_engine, inspect


def test_migrations_create_tables(postgres_container, apply_migrations):
    engine = create_engine(postgres_container.get_connection_url())
    tables = inspect(engine).get_table_names()
    assert "payments" in tables
    assert "outbox" in tables

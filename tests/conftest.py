import pytest

from pipeline.session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark(app_name="test", master="local")
    yield session
    session.stop()

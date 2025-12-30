import pytest
import sys
from pathlib import Path

# Add test_server directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from server import TestServer


@pytest.fixture(scope="session")
def test_server():
    """
    Pytest fixture that starts a simple HTTP server serving files from test_server/content directory.
    Returns the server URL.
    """
    server = TestServer()
    server_url = server.start_background()

    yield server_url

    server.stop()


@pytest.fixture(scope="session")
def server_url(test_server):
    """Alias fixture for convenience"""
    return test_server

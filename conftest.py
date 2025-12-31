import pytest
import sys
from pathlib import Path
import tempfile
import yaml

# Add test_server directory to path for imports
test_server_path = Path(__file__).parent / "test_server"
sys.path.insert(0, str(test_server_path))

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


@pytest.fixture(scope="session")
def test_sources_yml(test_server):
    """
    Create a temporary test_sources.yml file with the correct server URL.
    """
    # Read the template
    template_path = Path(__file__).parent / "test_server" / "test_sources.yml"
    with open(template_path, "r") as f:
        content = f.read()

    # Replace localhost:8888 with the actual server URL
    content = content.replace("http://localhost:8888", test_server)

    # Write to a temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False)
    temp_file.write(content)
    temp_file.close()

    yield temp_file.name

    # Cleanup
    Path(temp_file.name).unlink()

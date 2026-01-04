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


@pytest.fixture(autouse=True)
def cleanup_generated_test_files():
    """
    Auto-use fixture that cleans up generated test files after each test.
    This runs after every test to ensure test isolation.
    Removes all files starting with 'test_' prefix and test run records from database.
    """
    from datetime import datetime
    from fetch.dispatch import RunTracker

    # Setup: nothing needed before test
    yield

    # Teardown: clean up after test completes
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        root_dir = Path(__file__).parent

        # Remove raw data JSON files for test sources (files starting with test or test_)
        raw_data_dir = root_dir / "data" / "raw" / today
        if raw_data_dir.exists():
            for json_file in raw_data_dir.glob("test*.json"):
                json_file.unlink()
            # Remove directory if empty
            if not any(raw_data_dir.iterdir()):
                raw_data_dir.rmdir()

        # Remove job pickle files for test sources (files starting with test or test_)
        jobs_dir = root_dir / "data" / "jobs" / today
        if jobs_dir.exists():
            for pkl_file in jobs_dir.glob("test*.pkl"):
                pkl_file.unlink()
            # Remove directory if empty
            if not any(jobs_dir.iterdir()):
                jobs_dir.rmdir()

        # Remove test run records from database
        tracker = RunTracker()
        import sqlite3
        with sqlite3.connect(tracker.db_path) as conn:
            conn.execute("DELETE FROM fetched_urls WHERE source_name LIKE 'test%'")
            conn.commit()
    except Exception as e:
        # Don't fail tests if cleanup fails
        print(f"Warning: Test cleanup failed: {e}")


@pytest.fixture
def dispatch_all_sources(test_sources_yml):
    """
    Fixture that runs the whole dispatch flow for all sources (no specific source_name).
    Returns a Dispatcher instance with all sources executed.
    """
    from fetch.dispatch import Dispatcher

    dispatcher = Dispatcher(path=test_sources_yml, source_name=None)
    dispatcher.execute_jobs()

    return dispatcher

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
    from xwebetl.extract.dispatch import RunTracker

    # Setup: nothing needed before test
    yield

    # Teardown: clean up after test completes
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        root_dir = Path.cwd()

        # Remove raw data JSON files for test sources (files starting with test or test_)
        raw_data_dir = root_dir / "data" / "raw" / today
        if raw_data_dir.exists():
            for json_file in raw_data_dir.glob("test*.json"):
                json_file.unlink()
            # Remove directory if empty
            if not any(raw_data_dir.iterdir()):
                raw_data_dir.rmdir()

        # Remove silver data JSON files for test sources (files starting with test or test_)
        silver_data_dir = root_dir / "data" / "silver" / today
        if silver_data_dir.exists():
            for json_file in silver_data_dir.glob("test*.json"):
                json_file.unlink()
            # Remove directory if empty
            if not any(silver_data_dir.iterdir()):
                silver_data_dir.rmdir()

        # Remove gold data files for test sources (files starting with test or test_)
        gold_data_dir = root_dir / "data" / "gold" / today
        if gold_data_dir.exists():
            for file in gold_data_dir.glob("test*.json"):
                file.unlink()
            for file in gold_data_dir.glob("test*.xml"):
                file.unlink()
            # Remove directory if empty
            if not any(gold_data_dir.iterdir()):
                gold_data_dir.rmdir()

        # Note: Job pickle files are no longer created (jobs loaded from YAML)

        # Remove test run records from database
        from xwebetl.extract.dispatch import RunTracker
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
    from xwebetl.extract.dispatch import Dispatcher

    dispatcher = Dispatcher(path=test_sources_yml, source_name=None)
    dispatcher.execute_jobs()

    return dispatcher


@pytest.fixture
def dispatch_transform_all_sources(test_sources_yml):
    """
    Fixture that runs both dispatch and transform for all sources.
    Returns a tuple of (Dispatcher, Transform) instances.
    """
    from xwebetl.extract.dispatch import Dispatcher
    from xwebetl.transform.transform import Transform

    # Run dispatch
    dispatcher = Dispatcher(path=test_sources_yml, source_name=None)
    dispatcher.execute_jobs()
    dispatcher.save_results()

    # Run transform
    transform = Transform(path=test_sources_yml)
    transform.process_jobs()

    return dispatcher, transform

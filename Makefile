.PHONY: server test clean test-server-kill last-fetches has-fetched delete-source delete-url delete-all-fetches reset-all

test-server:
	@python -m test_server.server

test-server-kill:
	@echo "Killing test server on port 8888..."
	@lsof -ti:8888 | xargs kill -9 2>/dev/null && echo "Test server killed" || echo "No server running on port 8888"

tests:
	@python -m pytest source/tests/test_source_manager.py fetch/tests/test_dispatch.py -v analyze/tests/test_analyze.py

clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned up cache directories"

# URL tracking commands
fetches:
	@python manage_runs.py last-fetches

fetches-%:
	@python manage_runs.py last-fetches $*

has-fetched:
	@python manage_runs.py has-fetched $(URL)

delete-source:
	@python manage_runs.py delete-source $(SOURCE)

delete-url:
	@python manage_runs.py delete-url $(URL)

delete-all-fetches:
	@echo "WARNING: This will delete ALL fetched URL history."
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] && python manage_runs.py delete-all || echo "Cancelled"

reset-all:
	@echo "WARNING: This will delete ALL data (raw files, jobs, fetched URLs database)."
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] && rm -rf data/* && echo "All data deleted" || echo "Cancelled"

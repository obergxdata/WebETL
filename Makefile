.PHONY: server test clean test-server-kill last-runs has-run-today delete-source delete-date delete-all-runs reset-all

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

# Run tracking commands
last-runs:
	@python manage_runs.py last-runs

last-runs-%:
	@python manage_runs.py last-runs $*

has-run-today:
	@python manage_runs.py has-run-today $(SOURCE)

delete-source:
	@python manage_runs.py delete-source $(SOURCE)

delete-date:
	@python manage_runs.py delete-date $(DATE)

delete-all-runs:
	@echo "WARNING: This will delete ALL run history."
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] && python manage_runs.py delete-all || echo "Cancelled"

reset-all:
	@echo "WARNING: This will delete ALL data (raw files, jobs, runs database)."
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] && rm -rf data/* && echo "All data deleted" || echo "Cancelled"

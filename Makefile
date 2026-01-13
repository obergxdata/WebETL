.PHONY: install install-dev test clean test-server test-server-kill build help

help:
	@echo "WebETL - Developer Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install package in editable mode"
	@echo "  make install-dev      Install with dev dependencies"
	@echo ""
	@echo "Development:"
	@echo "  make test             Run all tests"
	@echo "  make test-server      Start test server on port 8888"
	@echo "  make test-server-kill Kill test server"
	@echo "  make clean            Clean cache directories"
	@echo ""
	@echo "Package:"
	@echo "  make build            Build the package"
	@echo ""
	@echo "User Commands (via CLI):"
	@echo "  webetl fetches        Show recent fetches"
	@echo "  webetl --help         See all CLI commands"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	python -m pytest xwebetl/source/tests/test_source_manager.py xwebetl/extract/tests/test_dispatch.py xwebetl/transform/tests/test_transform.py xwebetl/load/tests/test_load.py

test-server:
	python -m test_server.server

test-server-kill:
	@echo "Killing test server on port 8888..."
	@lsof -ti:8888 | xargs kill -9 2>/dev/null && echo "Test server killed" || echo "No server running on port 8888"

clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "dist" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "build" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned up cache and build directories"

build:
	python -m build

# Dangerous commands (use with caution)
reset-all:
	@echo "WARNING: This will delete ALL data (raw files, jobs, fetched URLs database)."
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ] && rm -rf data/* && echo "All data deleted" || echo "Cancelled"

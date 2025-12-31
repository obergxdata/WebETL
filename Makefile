.PHONY: server test clean test-server-kill

test-server:
	@python -m test_server.server

test-server-kill:
	@echo "Killing test server on port 8888..."
	@lsof -ti:8888 | xargs kill -9 2>/dev/null && echo "Test server killed" || echo "No server running on port 8888"

test:
	@python -m pytest job/test_gen_jobs.py fetch/tests/test_dispatch.py -v

clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned up cache directories"

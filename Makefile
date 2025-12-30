.PHONY: server test clean

test-server:
	@python -m test_server.server

test:
	@python -m pytest job/test_gen_jobs.py fetch/tests/test_dispatch.py -v

clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned up cache directories"

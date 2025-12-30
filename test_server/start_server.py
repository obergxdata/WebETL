#!/usr/bin/env python
"""
Simple script to start the test server manually for local development.
Run: python test_server/start_server.py
"""
from server import TestServer

if __name__ == "__main__":
    server = TestServer()
    server.start_foreground()

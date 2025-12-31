"""
Test server module that can be used both for pytest fixtures and standalone execution.
"""
import http.server
import socketserver
import threading
import os
from pathlib import Path


class ReusableTCPServer(socketserver.TCPServer):
    """TCPServer that allows address reuse to avoid 'Address already in use' errors."""
    allow_reuse_address = True


class TestServer:
    """Simple HTTP server for serving test content."""

    def __init__(self, port=8888):
        self.port = port
        self.content_dir = Path(__file__).parent / "content"
        self.httpd = None
        self.server_thread = None
        self.original_dir = None

    def start(self, quiet=True):
        """Start the server in a background thread (for pytest) or foreground."""
        self.original_dir = os.getcwd()
        os.chdir(self.content_dir)

        if quiet:
            class QuietHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
                """HTTP handler that suppresses log messages"""
                def log_message(self, format, *args):
                    pass
            handler = QuietHTTPRequestHandler
        else:
            handler = http.server.SimpleHTTPRequestHandler

        self.httpd = ReusableTCPServer(("", self.port), handler)
        return f"http://localhost:{self.port}"

    def start_background(self):
        """Start server in background thread (for pytest)."""
        url = self.start(quiet=True)
        self.server_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.server_thread.start()
        return url

    def start_foreground(self):
        """Start server in foreground (for manual testing)."""
        url = self.start(quiet=False)
        print(f"Server running at {url}/")
        print(f"Serving files from: {self.content_dir}")
        print(f"\nTest URLs:")
        print(f"  Home page: {url}/html/home.html")
        print(f"  RSS feed:  {url}/rss/feed.xml")
        print(f"\nPress Ctrl+C to stop the server")
        try:
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")
        finally:
            self.stop()

    def stop(self):
        """Stop the server and restore working directory."""
        if self.httpd:
            self.httpd.shutdown()
        if self.original_dir:
            os.chdir(self.original_dir)


if __name__ == "__main__":
    server = TestServer()
    server.start_foreground()

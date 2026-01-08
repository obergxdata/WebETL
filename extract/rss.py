import feedparser
import logging

logger = logging.getLogger(__name__)


def visit_rss(url: str):
    try:
        feed = feedparser.parse(url)
        # feedparser doesn't raise exceptions, but check if parsing was successful
        if feed.bozo and hasattr(feed, 'bozo_exception'):
            # Only fail on fatal errors, not encoding warnings
            exception_type = type(feed.bozo_exception).__name__
            if exception_type in ['URLError', 'HTTPError', 'SAXParseException']:
                logger.error(f"Failed to parse RSS feed {url}: {feed.bozo_exception}")
                return None
            else:
                # Log warning for non-fatal issues like encoding mismatches
                logger.warning(f"RSS feed {url} has non-fatal issue: {feed.bozo_exception}")
        return feed
    except Exception as e:
        logger.error(f"Failed to fetch RSS feed {url}: {e}")
        return None

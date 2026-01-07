import feedparser
import logging

logger = logging.getLogger(__name__)


def visit_rss(url: str):
    try:
        feed = feedparser.parse(url)
        # feedparser doesn't raise exceptions, but check if parsing was successful
        if feed.bozo and hasattr(feed, 'bozo_exception'):
            logger.error(f"Failed to parse RSS feed {url}: {feed.bozo_exception}")
            return None
        return feed
    except Exception as e:
        logger.error(f"Failed to fetch RSS feed {url}: {e}")
        return None

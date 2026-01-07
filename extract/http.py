import requests
import logging

logger = logging.getLogger(__name__)


def visit_html(url, text=True):
    try:
        response = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "RSSIFY/1.0"},
        )
        response.raise_for_status()
        if text:
            return response.text
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None

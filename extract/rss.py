import feedparser


def visit_rss(url: str):
    feed = feedparser.parse(url)
    return feed

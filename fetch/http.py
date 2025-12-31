import requests


def visit_html(url):
    response = requests.get(url, timeout=10, headers={"User-Agent": "RSSIFY/1.0"})
    response.raise_for_status()
    if ".xml" in url:
        return response.content
    else:
        return response.text

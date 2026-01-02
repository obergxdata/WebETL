import requests


def visit_html(url, text=True):
    response = requests.get(
        url,
        timeout=10,
        headers={"User-Agent": "RSSIFY/1.0"},
    )
    response.raise_for_status()
    if text:
        return response.text
    return response.content

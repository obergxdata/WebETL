import requests


def visit(url):

    response = requests.get(url)
    response.raise_for_status()
    return response.text

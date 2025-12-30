from fetch.dispatch import Navigate, Dispatcher
from job.gen_jobs import Nav, Field


def test_navigate():
    d = Navigate(path="test_server/test_sources.yml")
    d.start()

    job = d.jobs[0]

    assert len(d.jobs) == 1
    assert job.name == "test"
    assert job.extract == [Field(name="title", selector="/html/body/p[1]")]
    assert job.nav == [
        Nav(url="http://localhost:8888/html/home.html", selector="//ul/li/a/@href"),
        Nav(url=None, selector="/html/body/p[3]/a/@href"),
    ]
    assert set(job.urls) == {
        "http://localhost:8888/html/article_1_appendix.html",
        "http://localhost:8888/html/article_2_appendix.html",
        "http://localhost:8888/html/article_3_appendix.html",
    }


def test_dispatcher():
    d = Dispatcher(path="test_server/test_sources.yml")
    d.execute_jobs()

    # Should have 1 SourceResult (one per source)
    assert len(d.results) == 1

    source_result = d.results[0]
    assert source_result.source_name == "test"
    assert len(source_result.results) == 3

    # Check that we have page results from all 3 appendix pages
    urls = {page_result.url for page_result in source_result.results}
    assert urls == {
        "http://localhost:8888/html/article_1_appendix.html",
        "http://localhost:8888/html/article_2_appendix.html",
        "http://localhost:8888/html/article_3_appendix.html",
    }

    # Check that each page result has the correct structure
    for page_result in source_result.results:
        assert len(page_result.fields) == 1
        assert page_result.fields[0].name == "title"
        assert len(page_result.fields[0].data) > 0


def test_source_result_to_json():
    d = Dispatcher(path="test_server/test_sources.yml")
    d.execute_jobs()

    source_result = d.results[0]
    json_data = source_result.to_json()

    # Check JSON structure
    assert json_data["source"] == "test"
    assert "result" in json_data
    assert len(json_data["result"]) == 3

    # Check that each URL has the extracted fields
    for url, fields in json_data["result"].items():
        assert url.startswith("http://localhost:8888/html/article_")
        assert "title" in fields
        assert len(fields["title"]) > 0

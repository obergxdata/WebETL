from fetch.dispatch import Navigate, Dispatcher
from job.gen_jobs import Nav, Field


def test_navigate(test_server, test_sources_yml):
    d = Navigate(path=test_sources_yml)
    d.start()

    job = d.jobs[0]
    assert len(d.jobs) == 2
    assert job.name == "test"
    assert job.extract == [Field(name="title", selector="/html/body/h1")]
    assert job.nav == [
        Nav(
            url=f"{test_server}/html/home.html",
            selector="//ul/li/a/@href",
            ftype="html",
        ),
        Nav(
            url=None,
            selector="/html/body/p[3]/a/@href",
            ftype="html",
        ),
    ]
    assert set(job.urls) == {
        f"{test_server}/html/article_1_appendix.html",
        f"{test_server}/html/article_2_appendix.html",
        f"{test_server}/html/article_3_appendix.html",
    }

    job_rss_html = d.jobs[1]
    assert job_rss_html.name == "test_rss_html"
    assert job_rss_html.extract == [Field(name="title", selector="/html/body/h1")]
    assert job_rss_html.nav == [
        Nav(
            url=f"{test_server}/rss/feed.xml",
            selector="link",
            ftype="rss",
        ),
        Nav(
            url=None,
            selector="/html/body/p[3]/a/@href",
            ftype="html",
        ),
    ]
    assert set(job_rss_html.urls) == {
        f"{test_server}/html/article_1_appendix.html",
        f"{test_server}/html/article_2_appendix.html",
        f"{test_server}/html/article_3_appendix.html",
    }


def test_dispatcher(test_server, test_sources_yml):
    d = Dispatcher(path=test_sources_yml)
    d.execute_jobs()

    # Should have 1 SourceResult (one per source)
    assert len(d.results) == 2

    source_result = d.results[0]
    source_result_rss_html = d.results[1]
    assert source_result.source_name == "test"
    assert source_result_rss_html.source_name == "test_rss_html"
    assert len(source_result.results) == 3
    assert len(source_result_rss_html.results) == 3

    # Check that we have page results from all 3 appendix pages
    urls = {page_result.url for page_result in source_result.results}
    urls_rss_html = {page_result.url for page_result in source_result_rss_html.results}
    assert urls == {
        f"{test_server}/html/article_1_appendix.html",
        f"{test_server}/html/article_2_appendix.html",
        f"{test_server}/html/article_3_appendix.html",
    }

    assert urls_rss_html == {
        f"{test_server}/html/article_1_appendix.html",
        f"{test_server}/html/article_2_appendix.html",
        f"{test_server}/html/article_3_appendix.html",
    }

    # Check that each page result has the correct structure
    for page_result in source_result.results:
        assert len(page_result.fields) == 1
        assert page_result.fields[0].name == "title"
        assert len(page_result.fields[0].data) > 0


def test_source_result_to_json(test_server, test_sources_yml):
    d = Dispatcher(path=test_sources_yml)
    d.execute_jobs()

    source_result = d.results[0]
    json_data = source_result.to_json()

    # Check JSON structure
    assert json_data["source"] == "test"
    assert "result" in json_data
    assert len(json_data["result"]) == 3

    # Check that each URL has the extracted fields
    for url, fields in json_data["result"].items():
        assert url.startswith(f"{test_server}/html/article_")
        assert "title" in fields
        assert len(fields["title"]) > 0

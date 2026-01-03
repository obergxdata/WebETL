from fetch.dispatch import Navigate, Dispatcher
from job.gen_jobs import Nav, Field


def test_navigate_test(test_server, test_sources_yml):
    """Test navigation for 'test' source (HTML -> HTML)."""
    d = Navigate(path=test_sources_yml)
    d.start()

    job = d.jobs[0]
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


def test_navigate_test_rss_html(test_server, test_sources_yml):
    """Test navigation for 'test_rss_html' source (RSS -> HTML)."""
    d = Navigate(path=test_sources_yml)
    d.start()

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


def test_navigate_test_only_rss(test_server, test_sources_yml):
    """Test navigation for 'test_only_rss' source (RSS only)."""
    d = Navigate(path=test_sources_yml)
    d.start()

    job_rss = d.jobs[2]
    assert job_rss.name == "test_only_rss"
    assert job_rss.extract == [Field(name="description", selector="description")]
    assert job_rss.nav == []
    assert len(job_rss.urls) == 1
    assert job_rss.urls[0] == f"{test_server}/rss/feed.xml"


def test_dispatcher_test(test_server, test_sources_yml):
    """Test dispatcher execution for 'test' source (HTML -> HTML)."""
    d = Dispatcher(path=test_sources_yml)
    d.execute_jobs()

    source_result = [r for r in d.results if r.source_name == "test"][0]
    assert source_result.source_name == "test"
    assert len(source_result.results) == 3

    urls = {page_result.url for page_result in source_result.results}
    assert urls == {
        f"{test_server}/html/article_1_appendix.html",
        f"{test_server}/html/article_2_appendix.html",
        f"{test_server}/html/article_3_appendix.html",
    }

    for page_result in source_result.results:
        assert len(page_result.fields) == 1
        assert page_result.fields[0].name == "title"
        assert len(page_result.fields[0].data) > 0


def test_dispatcher_test_rss_html(test_server, test_sources_yml):
    """Test dispatcher execution for 'test_rss_html' source (RSS -> HTML)."""
    d = Dispatcher(path=test_sources_yml)
    d.execute_jobs()

    source_result_rss_html = [r for r in d.results if r.source_name == "test_rss_html"][0]
    assert source_result_rss_html.source_name == "test_rss_html"
    assert len(source_result_rss_html.results) == 3

    urls_rss_html = {page_result.url for page_result in source_result_rss_html.results}
    assert urls_rss_html == {
        f"{test_server}/html/article_1_appendix.html",
        f"{test_server}/html/article_2_appendix.html",
        f"{test_server}/html/article_3_appendix.html",
    }


def test_dispatcher_test_only_rss(test_server, test_sources_yml):
    """Test dispatcher execution for 'test_only_rss' source (RSS only)."""
    d = Dispatcher(path=test_sources_yml)
    d.execute_jobs()

    source_result_rss = [r for r in d.results if r.source_name == "test_only_rss"][0]
    assert source_result_rss.source_name == "test_only_rss"
    assert len(source_result_rss.results) == 1

    page_result = source_result_rss.results[0]
    assert page_result.url == f"{test_server}/rss/feed.xml"
    assert len(page_result.fields) == 3
    for field in page_result.fields:
        assert field.name == "description"
        assert len(field.data) > 0


def test_source_result_to_json_test(test_server, test_sources_yml):
    """Test JSON serialization for 'test' source."""
    d = Dispatcher(path=test_sources_yml)
    d.execute_jobs()

    source_result = [r for r in d.results if r.source_name == "test"][0]
    json_data = source_result.to_json()

    assert json_data["source"] == "test"
    assert "result" in json_data
    assert len(json_data["result"]) == 3

    for url, fields in json_data["result"].items():
        assert url.startswith(f"{test_server}/html/article_")
        assert "title" in fields
        assert len(fields["title"]) > 0

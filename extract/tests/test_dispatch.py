from extract.dispatch import Navigate, Dispatcher
from source.source_manager import Nav, Field
import pytest


def test_navigate_test(test_server, test_sources_yml):
    """Test navigation for 'test' source (HTML -> HTML)."""
    d = Navigate(path=test_sources_yml, source_name="test")
    d.start()

    job = d.jobs[0]
    assert job.name == "test"
    assert job.extract == [
        Field(name="title", selector="/html/body/h1"),
        Field(name="body", selector="//div[@id='article-body']"),
    ]
    assert job.nav == [
        Nav(
            url=f"{test_server}/html/home.html",
            selector="//ul/li/a/@href",
            ftype="html",
        ),
        Nav(
            url=None,
            selector="//div[@id='article-body']/p[4]/a/@href",
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
    d = Navigate(path=test_sources_yml, source_name="test_rss_html")
    d.start()

    job_rss_html = d.jobs[0]
    assert job_rss_html.name == "test_rss_html"
    assert job_rss_html.extract == [
        Field(name="title", selector="/html/body/h1"),
        Field(name="body", selector="//div[@id='article-body']"),
    ]
    assert job_rss_html.nav == [
        Nav(
            url=f"{test_server}/rss/feed.xml",
            selector="link",
            ftype="rss",
        ),
        Nav(
            url=None,
            selector="//div[@id='article-body']/p[4]/a/@href",
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
    d = Navigate(path=test_sources_yml, source_name="test_only_rss")
    d.start()

    job_rss = d.jobs[0]
    assert job_rss.name == "test_only_rss"
    assert job_rss.extract == [
        Field(name="title", selector="title"),
        Field(name="description", selector="description"),
    ]
    assert job_rss.nav == []
    assert len(job_rss.urls) == 1
    assert job_rss.urls[0] == f"{test_server}/rss/feed.xml"


def test_dispatcher_test(test_server, test_sources_yml):
    """Test dispatcher execution for 'test' source (HTML -> HTML)."""
    d = Dispatcher(path=test_sources_yml, source_name="test")
    d.execute_jobs()

    source_result = d.results[0]
    assert source_result.source_name == "test"
    assert len(source_result.results) == 3

    urls = {page_result.url for page_result in source_result.results}
    assert urls == {
        f"{test_server}/html/article_1_appendix.html",
        f"{test_server}/html/article_2_appendix.html",
        f"{test_server}/html/article_3_appendix.html",
    }

    for page_result in source_result.results:
        assert len(page_result.fields) == 2
        assert page_result.fields[0].name == "title"
        assert len(page_result.fields[0].data) > 0
        assert page_result.fields[1].name == "body"
        assert len(page_result.fields[1].data) > 0


def test_dispatcher_test_rss_html(test_server, test_sources_yml):
    """Test dispatcher execution for 'test_rss_html' source (RSS -> HTML)."""
    d = Dispatcher(path=test_sources_yml, source_name="test_rss_html")
    d.execute_jobs()

    source_result_rss_html = d.results[0]
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
    d = Dispatcher(path=test_sources_yml, source_name="test_only_rss")
    d.execute_jobs()

    source_result_rss = d.results[0]
    assert source_result_rss.source_name == "test_only_rss"
    assert len(source_result_rss.results) == 1

    page_result = source_result_rss.results[0]
    assert page_result.url == f"{test_server}/rss/feed.xml"
    assert len(page_result.fields) == 6  # 3 items * 2 fields (title + description)
    # Check that we have alternating title and description fields
    for i in range(0, len(page_result.fields), 2):
        assert page_result.fields[i].name == "title"
        assert len(page_result.fields[i].data) > 0
        assert page_result.fields[i + 1].name == "description"
        assert len(page_result.fields[i + 1].data) > 0


def test_dispatcher_test_rss_html_pdf(test_server, test_sources_yml):
    """Test dispatcher execution for 'test_rss_html_pdf' source (RSS -> HTML -> PDF)."""
    d = Dispatcher(path=test_sources_yml, source_name="test_rss_html_pdf")
    d.execute_jobs()

    source_result_rss_html_pdf = d.results[0]
    assert source_result_rss_html_pdf.source_name == "test_rss_html_pdf"
    assert len(source_result_rss_html_pdf.results) == 1

    urls_rss_html_pdf = {
        page_result.url for page_result in source_result_rss_html_pdf.results
    }
    for url in urls_rss_html_pdf:
        assert url.endswith(".pdf")

    for page_result in source_result_rss_html_pdf.results:
        assert len(page_result.fields) == 1
        assert "This is a pdf" in page_result.fields[0].data


def test_source_result_to_json_test(test_server, test_sources_yml):
    """Test JSON serialization for 'test' source."""
    d = Dispatcher(path=test_sources_yml, source_name="test")
    d.execute_jobs()

    source_result = d.results[0]
    json_data = source_result.to_json()

    assert json_data["source"] == "test"
    assert "result" in json_data
    assert len(json_data["result"]) == 3

    for url, fields in json_data["result"].items():
        assert url.startswith(f"{test_server}/html/article_")
        assert "title" in fields
        assert len(fields["title"]) > 0


def test_run_tracker_prevents_duplicate_urls(test_server, test_sources_yml):
    """Test that URLs are only fetched once, ever."""
    from extract.dispatch import RunTracker

    # First run - should execute normally
    d1 = Dispatcher(path=test_sources_yml, source_name="test")
    d1.execute_jobs()

    # Verify first run completed and recorded 3 URLs
    source_result_1 = d1.results[0]
    assert len(source_result_1.results) == 3

    # Verify URLs are tracked
    tracker = RunTracker()
    for page_result in source_result_1.results:
        assert tracker.has_been_fetched(page_result.url) is True

    # Second run - should skip fetching the same URLs
    d2 = Dispatcher(path=test_sources_yml, source_name="test")
    d2.execute_jobs()

    # Verify second run produced no results (all URLs already fetched)
    assert len(d2.results) == 0

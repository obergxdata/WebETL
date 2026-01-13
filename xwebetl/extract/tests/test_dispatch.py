from xwebetl.extract.dispatch import Navigate, Dispatcher
from xwebetl.source.source_manager import Nav, Field
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
            selector="//div[@id='article-body']/p[4]/a/@href|//div[@id='lol-body']/p[4]/a/@href",
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
            selector="//div[@id='lol']/p[4]/a/@href|//div[@id='article-body']/p[4]/a/@href",
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
        Field(name="link", selector="link"),
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
    assert (
        len(page_result.fields) == 9
    )  # 3 items * 3 fields (title + description + link)
    # Check that we have title, description, link pattern repeating
    for i in range(0, len(page_result.fields), 3):
        assert page_result.fields[i].name == "title"
        assert len(page_result.fields[i].data) > 0
        assert page_result.fields[i + 1].name == "description"
        assert len(page_result.fields[i + 1].data) > 0
        assert page_result.fields[i + 2].name == "link"
        assert len(page_result.fields[i + 2].data) > 0


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
    from datetime import datetime

    d = Dispatcher(path=test_sources_yml, source_name="test")
    d.execute_jobs()

    source_result = d.results[0]
    json_data = source_result.to_json()

    assert json_data["source"] == "test"
    assert "result" in json_data
    assert "extraction_date" in json_data
    assert len(json_data["result"]) == 3

    # Validate extraction_date is in ISO format
    extraction_date = json_data["extraction_date"]
    assert isinstance(extraction_date, str)
    # Parse to ensure it's valid ISO format
    datetime.fromisoformat(extraction_date)

    for url, entries in json_data["result"].items():
        assert url.startswith(f"{test_server}/html/article_")
        # Each URL should have a list of entries (even if just one for HTML)
        assert isinstance(entries, list)
        assert len(entries) > 0
        # Check the first entry
        assert "title" in entries[0]
        assert len(entries[0]["title"]) > 0


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


def test_dispatcher_handles_broken_urls(caplog):
    """Test that dispatcher handles broken URLs gracefully without crashing."""
    from source.source_manager import Job

    # Create a fake job with broken URLs
    fake_job = Job(
        name="test_broken_urls",
        start="http://this-domain-definitely-does-not-exist-12345.com",
        nav=[],
        extract=[Field(name="title", selector="/html/body/h1")],
        urls=[
            "http://this-domain-definitely-does-not-exist-12345.com/page1.html",
            "http://invalid-url-404.com/nonexistent.html",
        ],
        ftype="html",
        extract_ftype="html",
    )

    # Create dispatcher and manually set up jobs
    d = Dispatcher.__new__(Dispatcher)
    d.navigate = Navigate.__new__(Navigate)
    d.navigate.jobs = [fake_job]
    d.results = []
    from extract.dispatch import RunTracker

    d.run_tracker = RunTracker()
    d.no_track = False

    # Execute jobs - should not raise exceptions
    d.execute_jobs()

    # Verify no results were collected (all URLs failed)
    # Note: With ProcessPoolExecutor, logs from subprocesses aren't captured by caplog,
    # but errors are still logged to stderr. We verify graceful handling by checking outcomes.
    assert len(d.results) == 0 or (
        len(d.results) == 1 and len(d.results[0].results) == 0
    )


def test_dispatcher_handles_broken_rss_feeds(caplog):
    """Test that dispatcher handles broken RSS feeds gracefully without crashing."""
    from source.source_manager import Job

    # Create a fake job with broken RSS URL
    fake_job = Job(
        name="test_broken_rss",
        start="http://this-domain-definitely-does-not-exist-12345.com/feed.xml",
        nav=[],
        extract=[
            Field(name="title", selector="title"),
            Field(name="description", selector="description"),
        ],
        urls=["http://this-domain-definitely-does-not-exist-12345.com/feed.xml"],
        ftype="rss",
        extract_ftype="rss",
    )

    # Create dispatcher and manually set up jobs
    d = Dispatcher.__new__(Dispatcher)
    d.navigate = Navigate.__new__(Navigate)
    d.navigate.jobs = [fake_job]
    d.results = []
    from extract.dispatch import RunTracker

    d.run_tracker = RunTracker()
    d.no_track = False

    # Execute jobs - should not raise exceptions
    d.execute_jobs()

    # Verify no results were collected (all URLs failed)
    # Note: With ProcessPoolExecutor, logs from subprocesses aren't captured by caplog,
    # but errors are still logged to stderr. We verify graceful handling by checking outcomes.
    assert len(d.results) == 0 or (
        len(d.results) == 1 and len(d.results[0].results) == 0
    )


def test_navigate_handles_broken_navigation(caplog, test_sources_yml):
    """Test that navigation handles broken URLs gracefully by logging warnings."""
    from source.source_manager import Nav
    import logging

    # Create Navigate instance with a real source
    n = Navigate(path=test_sources_yml, source_name="test")

    # Override one of the navigation steps with a broken URL
    job = n.jobs[0]
    job.nav = [
        Nav(
            url="http://this-domain-definitely-does-not-exist-12345.com/index.html",
            selector="//a/@href",
            ftype="html",
        )
    ]

    # This should not raise an exception
    with caplog.at_level(logging.WARNING):
        n.start()

    # Verify warning was logged about failed navigation
    assert any(
        "No URLs found during navigation" in record.message for record in caplog.records
    )

    # Verify job.urls is empty or None (navigation failed)
    assert not job.urls or len(job.urls) == 0


def test_must_contain_all_filter(test_server, test_sources_yml):
    """Test must_contain_all filter with AND logic."""
    d = Navigate(path=test_sources_yml, source_name="test_must_contain_all")
    d.start()

    job = d.jobs[0]
    assert job.name == "test_must_contain_all"

    # Verify navigation config has both must_contain and must_contain_all
    assert job.nav[0].must_contain == ["article"]
    assert job.nav[0].must_contain_all == ["_1", ".html"]

    # Should only match article_1.html (has "article" AND "_1" AND ".html")
    # Should NOT match article_2.html or article_3.html (missing "_1")
    assert job.urls == [f"{test_server}/html/article_1.html"]


def test_dispatcher_must_contain_all(test_server, test_sources_yml):
    """Test dispatcher execution with must_contain_all filter."""
    d = Dispatcher(path=test_sources_yml, source_name="test_must_contain_all")
    d.execute_jobs()

    source_result = d.results[0]
    assert source_result.source_name == "test_must_contain_all"
    assert len(source_result.results) == 1

    # Should only have fetched article_1.html
    page_result = source_result.results[0]
    assert page_result.url == f"{test_server}/html/article_1.html"
    assert page_result.fields[0].name == "title"
    assert "Article 1" in page_result.fields[0].data


def test_navigate_test_json_to_html(test_server, test_sources_yml):
    """Test navigation for 'test_json_to_html' source (JSON -> HTML)."""
    d = Navigate(path=test_sources_yml, source_name="test_json_to_html")
    d.start()

    job = d.jobs[0]
    assert job.name == "test_json_to_html"
    assert job.extract == [
        Field(name="title", selector="/html/body/h1"),
        Field(name="body", selector="//div[@id='article-body']"),
    ]
    assert job.nav == [
        Nav(
            url=f"{test_server}/json/test.json",
            selector="items.link",
            ftype="json",
        ),
    ]
    # Should navigate to all 3 article HTML pages from the JSON
    assert set(job.urls) == {
        f"{test_server}/html/article_1.html",
        f"{test_server}/html/article_2.html",
        f"{test_server}/html/article_3.html",
    }


def test_dispatcher_test_json_to_html(test_server, test_sources_yml):
    """Test dispatcher execution for 'test_json_to_html' source (JSON -> HTML)."""
    d = Dispatcher(path=test_sources_yml, source_name="test_json_to_html")
    d.execute_jobs()

    source_result = d.results[0]
    assert source_result.source_name == "test_json_to_html"
    assert len(source_result.results) == 3

    urls = {page_result.url for page_result in source_result.results}
    assert urls == {
        f"{test_server}/html/article_1.html",
        f"{test_server}/html/article_2.html",
        f"{test_server}/html/article_3.html",
    }

    # Verify each page has title and body extracted
    for page_result in source_result.results:
        assert len(page_result.fields) == 2
        assert page_result.fields[0].name == "title"
        assert len(page_result.fields[0].data) > 0
        assert "Article" in page_result.fields[0].data
        assert page_result.fields[1].name == "body"
        assert len(page_result.fields[1].data) > 0


def test_dispatcher_test_json_direct(test_server, test_sources_yml):
    """Test dispatcher execution for 'test_json_direct' source (direct JSON extraction)."""
    d = Dispatcher(path=test_sources_yml, source_name="test_json_direct")
    d.execute_jobs()

    source_result = d.results[0]
    assert source_result.source_name == "test_json_direct"
    assert len(source_result.results) == 1

    page_result = source_result.results[0]
    assert page_result.url == f"{test_server}/json/test.json"

    # Should have 12 fields (3 items * 4 fields each: title, description, date, link)
    assert len(page_result.fields) == 12

    # Check that we have 3 of each field type
    field_names = [field.name for field in page_result.fields]
    assert field_names.count("title") == 3
    assert field_names.count("description") == 3
    assert field_names.count("date") == 3
    assert field_names.count("link") == 3

    # Check that title fields contain "Article"
    titles = [field.data for field in page_result.fields if field.name == "title"]
    for title in titles:
        assert "Article" in title

    # Check that date fields contain "2026"
    dates = [field.data for field in page_result.fields if field.name == "date"]
    for date in dates:
        assert "2026" in date

    # Check that link fields contain "/html/article"
    links = [field.data for field in page_result.fields if field.name == "link"]
    for link in links:
        assert "/html/article" in link


def test_navigate_test_json_max_items(test_server, test_sources_yml):
    """Test navigation with max_items limit for JSON source."""
    d = Navigate(path=test_sources_yml, source_name="test_json_max_items")
    d.start()

    job = d.jobs[0]
    assert job.name == "test_json_max_items"
    assert job.nav[0].max_items == 2

    # Should only have 2 URLs despite JSON having 3 articles
    assert len(job.urls) == 2
    assert all(url.startswith(f"{test_server}/html/article") for url in job.urls)


def test_dispatcher_test_json_max_items(test_server, test_sources_yml):
    """Test dispatcher execution with max_items limit for JSON source."""
    d = Dispatcher(path=test_sources_yml, source_name="test_json_max_items")
    d.execute_jobs()

    source_result = d.results[0]
    assert source_result.source_name == "test_json_max_items"

    # Should only have extracted 2 pages
    assert len(source_result.results) == 2

    for page_result in source_result.results:
        assert len(page_result.fields) == 1
        assert page_result.fields[0].name == "title"
        assert "Article" in page_result.fields[0].data

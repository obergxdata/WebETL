from job.gen_jobs import Source, Nav, Field, Job


def test_generate_jobs_test():
    """Test job generation for 'test' source (HTML -> HTML)."""
    source = Source("test_server/test_sources.yml", source_name="test")
    jobs = source.gen_jobs()

    assert len(jobs) == 1
    job = jobs[0]

    assert job.name == "test"
    assert job.start == "http://localhost:8888/html/home.html"
    assert job.ftype == "html"
    assert job.extract == [
        Field(
            name="title",
            selector="/html/body/h1",
        )
    ]
    assert job.nav == [
        Nav(
            url="http://localhost:8888/html/home.html",
            selector="//ul/li/a/@href",
            ftype="html",
        ),
        Nav(
            url=None,
            selector="/html/body/p[3]/a/@href",
            ftype="html",
        ),
    ]


def test_generate_jobs_test_rss_html():
    """Test job generation for 'test_rss_html' source (RSS -> HTML)."""
    source = Source("test_server/test_sources.yml", source_name="test_rss_html")
    jobs = source.gen_jobs()

    assert len(jobs) == 1
    job = jobs[0]

    assert job.name == "test_rss_html"
    assert job.start == "http://localhost:8888/rss/feed.xml"
    assert job.ftype == "rss"
    assert job.extract == [
        Field(
            name="title",
            selector="/html/body/h1",
        )
    ]
    assert job.nav == [
        Nav(
            url="http://localhost:8888/rss/feed.xml",
            selector="link",
            ftype="rss",
        ),
        Nav(
            url=None,
            selector="/html/body/p[3]/a/@href",
            ftype="html",
        ),
    ]


def test_generate_jobs_test_only_rss():
    """Test job generation for 'test_only_rss' source (RSS only)."""
    source = Source("test_server/test_sources.yml", source_name="test_only_rss")
    jobs = source.gen_jobs()

    assert len(jobs) == 1
    job = jobs[0]

    assert job == Job(
        name="test_only_rss",
        start="http://localhost:8888/rss/feed.xml",
        ftype="rss",
        extract_ftype="rss",
        extract=[
            Field(
                name="description",
                selector="description",
            )
        ],
        nav=[],
    )


def test_generate_jobs_test_rss_html_pdf():
    """Test job generation for 'test_rss_html_pdf' source (RSS -> HTML -> PDF)."""
    source = Source("test_server/test_sources.yml", source_name="test_rss_html_pdf")
    jobs = source.gen_jobs()

    assert len(jobs) == 1
    job = jobs[0]

    assert job.name == "test_rss_html_pdf"
    assert job.start == "http://localhost:8888/rss/feed.xml"
    assert job.ftype == "rss"
    assert job.nav == [
        Nav(
            url="http://localhost:8888/rss/feed.xml",
            selector="link",
            ftype="rss",
            must_contain=["html"],
        ),
        Nav(
            url=None,
            selector="/html/body/p[4]/a/@href",
            ftype="html",
            must_contain=[".pdf"],
        ),
    ]

from xwebetl.source.source_manager import Source, Nav, Field, Job


def test_generate_jobs_test(test_sources_yml):
    """Test job generation for 'test' source (HTML -> HTML)."""
    source = Source(test_sources_yml, source_name="test")
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
        ),
        Field(
            name="body",
            selector="//div[@id='article-body']",
        ),
    ]
    assert job.nav == [
        Nav(
            url="http://localhost:8888/html/home.html",
            selector="//ul/li/a/@href",
            ftype="html",
        ),
        Nav(
            url=None,
            selector="//div[@id='article-body']/p[4]/a/@href|//div[@id='lol-body']/p[4]/a/@href",
            ftype="html",
        ),
    ]


def test_generate_jobs_test_rss_html(test_sources_yml):
    """Test job generation for 'test_rss_html' source (RSS -> HTML)."""
    source = Source(test_sources_yml, source_name="test_rss_html")
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
        ),
        Field(
            name="body",
            selector="//div[@id='article-body']",
        ),
    ]
    assert job.nav == [
        Nav(
            url="http://localhost:8888/rss/feed.xml",
            selector="link",
            ftype="rss",
        ),
        Nav(
            url=None,
            selector="//div[@id='lol']/p[4]/a/@href|//div[@id='article-body']/p[4]/a/@href",
            ftype="html",
        ),
    ]


def test_generate_jobs_test_only_rss(test_sources_yml):
    """Test job generation for 'test_only_rss' source (RSS only)."""
    source = Source(test_sources_yml, source_name="test_only_rss")
    jobs = source.gen_jobs()

    assert len(jobs) == 1
    job = jobs[0]

    assert job == Job(
        name="test_only_rss",
        start="http://localhost:8888/rss/feed.xml",
        ftype="mixed",
        extract_ftype="mixed",
        extract=[
            Field(
                name="title",
                selector="title",
            ),
            Field(
                name="description",
                selector="description",
            ),
            Field(
                name="link",
                selector="link",
            ),
        ],
        nav=[],
        transform={
            "LLM": [
                {
                    "name": "sentiment",
                    "input": ["title"],
                    "output": "title_sentiment",
                    "model": "gpt-4",
                    "prompt": "Analyze the sentiment of the following text and classify it as Positive, Negative, or Neutral.",
                }
            ]
        },
        load={
            "json": {
                "fields": [
                    {"field": "title", "name": "title"},
                    {"field": "title", "name": "description"},
                ]
            }
        },
    )


def test_generate_jobs_test_rss_html_pdf(test_sources_yml):
    """Test job generation for 'test_rss_html_pdf' source (RSS -> HTML -> PDF)."""
    source = Source(test_sources_yml, source_name="test_rss_html_pdf")
    jobs = source.gen_jobs()

    assert len(jobs) == 1
    job = jobs[0]

    assert job.name == "test_rss_html_pdf"
    assert job.start == "http://localhost:8888/rss/feed.xml"
    assert job.ftype == "mixed"
    assert job.nav == [
        Nav(
            url="http://localhost:8888/rss/feed.xml",
            selector="link",
            ftype="mixed",
            must_contain=["html"],
        ),
        Nav(
            url=None,
            selector="//div[@id='article-rody']/p[5]/a/@href|//div[@id='article-body']/p[5]/a/@href",
            ftype="mixed",
            must_contain=[".pdf"],
        ),
    ]

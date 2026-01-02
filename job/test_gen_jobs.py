from job.gen_jobs import Source, Nav, Field, Job


def test_generate_jobs():
    source = Source("test_server/test_sources.yml")
    source.gen_jobs()

    assert source[0].name == "test"
    assert source[1].name == "test_rss_html"
    assert source[0].extract[0] == Field(
        name="title",
        selector="/html/body/h1",
    )
    assert source[0].nav[0] == Nav(
        url="http://localhost:8888/html/home.html",
        selector="//ul/li/a/@href",
        ftype="html",
    )
    assert source[0].nav[1] == Nav(
        url=None,
        selector="/html/body/p[3]/a/@href",
        ftype="html",
    )

    assert source[2] == Job(
        name="test_only_rss",
        start="http://localhost:8888/rss/feed.xml",
        ftype="rss",
        extract=[
            Field(
                name="description",
                selector="description",
            )
        ],
        nav=[],
    )

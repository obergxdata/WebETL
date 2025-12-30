from job.gen_jobs import Source, Nav, Field


def test_generate_jobs():
    source = Source("test_server/test_sources.yml")
    source.gen_jobs()

    assert source[0].name == "test"
    assert source[0].extract[0] == Field(
        name="title",
        selector="/html/body/p[1]",
    )
    assert source[0].nav[0] == Nav(
        url="http://localhost:8888/html/home.html",
        selector="//ul/li/a/@href",
    )
    assert source[0].nav[1] == Nav(
        url=None,
        selector="/html/body/p[3]/a/@href",
    )

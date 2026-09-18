from independent_robots import declared_sitemaps, inspect


class Response:
    status_code = 200
    text = "User-agent: *\nAllow: /\nCrawl-delay: 12\nSitemap: /sitemap.xml\n"
    content = text.encode()
    headers = {}


class Session:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response()


def test_robots_audit_makes_exactly_one_robots_request():
    session = Session()
    result = inspect({
        "operator_id": "example", "name": "Example", "domain": "example.com",
        "representative_url": "https://www.example.com/storage/one",
    }, session=session)
    assert len(session.calls) == 1
    assert session.calls[0][0] == "https://www.example.com/robots.txt"
    assert session.calls[0][1]["allow_redirects"] is False
    assert result["status"] == "reviewed" and result["representative_allowed"] is True
    assert result["crawl_delay"] == 12
    assert result["sitemaps"] == ["https://www.example.com/sitemap.xml"]


def test_declared_sitemaps_deduplicates_absolute_and_relative_urls():
    assert declared_sitemaps(
        "Sitemap: /one.xml\nSITEMAP: https://example.com/two.xml\nSitemap: /one.xml",
        "https://example.com",
    ) == ["https://example.com/one.xml", "https://example.com/two.xml"]

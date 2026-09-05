import unittest

from irs_990_downloader import discover_downloads


class DiscoverDownloadsTests(unittest.TestCase):
    def test_filters_year_type_host_and_duplicates(self):
        html = """
        <a href="https://apps.irs.gov/pub/epostcard/990/xml/2024/a.zip">zip</a>
        <a href="https://apps.irs.gov/pub/epostcard/990/xml/2024/a.zip">duplicate</a>
        <a href="https://apps.irs.gov/pub/epostcard/990/xml/2025/index_2025.csv">csv</a>
        <a href="https://evil.example/pub/epostcard/990/xml/2024/b.zip">wrong host</a>
        <a href="https://apps.irs.gov/pub/epostcard/990/xml/2023/c.zip">wrong year</a>
        <a href="https://apps.irs.gov/pub/epostcard/990/xml/2024/readme.txt">wrong type</a>
        """
        results = discover_downloads(html, {2024, 2025})
        self.assertEqual([(x.year, x.filename) for x in results], [(2024, "a.zip"), (2025, "index_2025.csv")])


if __name__ == "__main__":
    unittest.main()

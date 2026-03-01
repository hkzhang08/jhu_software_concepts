"""Tests for scraping utilities and page parsing."""

import json
import ssl
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import scrape

pytestmark = pytest.mark.web


def test_url_check_calls_robotparser(monkeypatch, capsys):
    """url_check() initializes RobotFileParser and reports status."""
    calls = {"set_url": None, "read": 0}

    class FakeParser:
        def set_url(self, url):
            calls["set_url"] = url

        def read(self):
            calls["read"] += 1

    monkeypatch.setattr(scrape.robotparser, "RobotFileParser", lambda: FakeParser())

    parser = scrape.url_check()

    out = capsys.readouterr().out
    assert "robots.txt checked" in out
    assert calls["set_url"].endswith("robots.txt")
    assert calls["read"] == 1
    assert isinstance(parser, FakeParser)


def test_build_ssl_context_without_certifi(monkeypatch):
    """_build_ssl_context falls back to system trust when certifi is unavailable."""
    sentinel = object()

    def fake_create_default_context(*args, **kwargs):
        assert args == ()
        assert kwargs == {}
        return sentinel

    monkeypatch.setattr(scrape, "certifi", None)
    monkeypatch.setattr(scrape.ssl, "create_default_context", fake_create_default_context)
    assert scrape._build_ssl_context() is sentinel


def test_is_cert_verification_error_ssl_error_and_non_ssl_reason():
    """_is_cert_verification_error handles generic SSLError and non-SSL reasons."""
    ssl_err = scrape.error.URLError(scrape.ssl.SSLError("CERTIFICATE_VERIFY_FAILED"))
    assert scrape._is_cert_verification_error(ssl_err) is True

    non_ssl_err = scrape.error.URLError("network down")
    assert scrape._is_cert_verification_error(non_ssl_err) is False


def test_url_check_reraises_non_cert_urlerror(monkeypatch):
    """url_check() re-raises URLError when it is not a cert-verification issue."""
    class FakeParser:
        def set_url(self, _url):
            return None

        def read(self):
            raise scrape.error.URLError("network down")

    monkeypatch.setattr(scrape.robotparser, "RobotFileParser", lambda: FakeParser())
    with pytest.raises(scrape.error.URLError):
        scrape.url_check()


def test_url_check_retries_with_ca_bundle_on_cert_error(monkeypatch, capsys):
    """url_check() retries robots fetch via TLS context on cert failures."""
    calls = {"read": 0, "parsed": None, "urlopen": 0}

    class FakeParser:
        def set_url(self, _url):
            return None

        def read(self):
            calls["read"] += 1
            cert_error = ssl.SSLCertVerificationError("CERTIFICATE_VERIFY_FAILED")
            raise scrape.error.URLError(cert_error)

        def parse(self, lines):
            calls["parsed"] = list(lines)

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"User-agent: *\nDisallow:"

    def fake_urlopen(_req, context=None):
        calls["urlopen"] += 1
        assert context is not None
        return FakeResponse()

    monkeypatch.setattr(scrape.robotparser, "RobotFileParser", lambda: FakeParser())
    monkeypatch.setattr(scrape.request, "urlopen", fake_urlopen)

    parser = scrape.url_check()

    out = capsys.readouterr().out
    assert "retrying with CA bundle" in out
    assert "robots.txt checked" in out
    assert calls["read"] == 1
    assert calls["urlopen"] == 1
    assert calls["parsed"] == ["User-agent: *", "Disallow:"]
    assert isinstance(parser, FakeParser)


def test_check_url_not_allowed(monkeypatch, capsys):
    """check_url() returns None when robots disallow fetch."""
    class FakeParser:
        def can_fetch(self, _agent, _url):
            return False

    result = scrape.check_url("https://example.com/page", FakeParser())
    out = capsys.readouterr().out
    assert result is None
    assert "NOT allowed to fetch URL" in out


def test_check_url_allowed_returns_soup(monkeypatch):
    """check_url() returns a BeautifulSoup object when allowed."""
    class FakeParser:
        def can_fetch(self, _agent, _url):
            return True

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"<html><body><table></table></body></html>"

    monkeypatch.setattr(scrape.request, "urlopen", lambda _req: FakeResponse())
    soup = scrape.check_url("https://example.com/page", FakeParser())
    assert soup is not None
    assert soup.find("table") is not None


def test_check_url_http_error(monkeypatch, capsys):
    """check_url() handles HTTP errors and returns None."""
    class FakeParser:
        def can_fetch(self, _agent, _url):
            return True

    def boom(_req):
        raise scrape.error.HTTPError(url=None, code=403, msg="Forbidden", hdrs=None, fp=None)

    monkeypatch.setattr(scrape.request, "urlopen", boom)
    result = scrape.check_url("https://example.com/page", FakeParser())
    out = capsys.readouterr().out
    assert result is None
    assert "An error has occurred" in out


def test_check_url_url_error(monkeypatch, capsys):
    """check_url() handles URLErrors and returns None."""
    class FakeParser:
        def can_fetch(self, _agent, _url):
            return True

    def boom(_req, context=None):
        raise scrape.error.URLError("network down")

    monkeypatch.setattr(scrape.request, "urlopen", boom)
    result = scrape.check_url("https://example.com/page", FakeParser())
    out = capsys.readouterr().out
    assert result is None
    assert "An error has occurred" in out


def test_scrape_data_no_table():
    """scrape_data() returns [] when no table is present."""
    soup = scrape.BeautifulSoup("<html><body>No table</body></html>", "html.parser")
    assert scrape.scrape_data(soup) == []


def test_scrape_data_no_tbody():
    """scrape_data() returns [] when table has no tbody."""
    soup = scrape.BeautifulSoup("<table></table>", "html.parser")
    assert scrape.scrape_data(soup) == []


def test_scrape_data_valid_table_all_branches():
    """scrape_data() covers all parsing branches on valid HTML."""
    html = """
    <table>
      <tbody>
        <tr>
          <td>Loose row</td>
        </tr>
        <tr>
          <td>Test University</td>
          <td><span>Computer Science</span><span>PhD</span></td>
          <td>February 01, 2026</td>
          <td>Accepted on Feb 01</td>
          <td><a href="/result/123">link</a></td>
        </tr>
        <tr>
          <td colspan="4">
            <div>   </div>
            <div>Fall 2026</div>
            <div>International</div>
            <div>GPA 4.0</div>
            <div>GRE 320</div>
            <div>GRE V 160</div>
            <div>GRE AW 4.0</div>
            <p>First comment</p>
          </td>
        </tr>
        <tr>
          <td colspan="4">
            <p>Second comment should not replace</p>
          </td>
        </tr>
        <tr>
          <td>Other University</td>
          <td><span>Math</span></td>
          <td>February 02, 2026</td>
          <td>Rejected</td>
        </tr>
      </tbody>
    </table>
    """
    soup = scrape.BeautifulSoup(html, "html.parser")
    results = scrape.scrape_data(soup)
    assert len(results) == 2

    first = results[0]
    assert first["university"] == "Test University"
    assert first["program_name"] == "Computer Science"
    assert first["masters_or_phd"] == "PhD"
    assert first["date_added"] == "February 01, 2026"
    assert first["applicant_status"] == "Accepted"
    assert first["decision_date"] == "Feb 01"
    assert first["url"] == "https://www.thegradcafe.com/result/123"
    assert first["semester_year_start"] == "Fall 2026"
    assert first["citizenship"] == "International"
    assert first["gpa"] == "GPA 4.0"
    assert first["gre"] == "GRE 320"
    assert first["gre_v"] == "GRE V 160"
    assert first["gre_aw"] == "GRE AW 4.0"
    assert first["comments"] == "First comment"

    second = results[1]
    assert second["university"] == "Other University"
    assert second["program_name"] == "Math"
    assert second["masters_or_phd"] is None
    assert second["applicant_status"] == "Rejected"
    assert second["decision_date"] is None


def test_parse_decision_empty_and_whitespace():
    """_parse_decision returns (None, None) for empty and non-matching values."""
    assert scrape._parse_decision("") == (None, None)
    assert scrape._parse_decision("   ") == (None, None)


def test_create_pages():
    """create_pages() builds correct survey URLs."""
    assert scrape.create_pages(1).endswith("/survey/")
    assert scrape.create_pages(0).endswith("/survey/")
    assert scrape.create_pages(2).endswith("/survey/?page=2")


def test_pull_pages_breaks_when_check_url_none(monkeypatch, tmp_path):
    """pull_pages() stops when check_url returns None."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(scrape, "OUTPUT_FILE", str(tmp_path / "out.json"))
    monkeypatch.setattr(scrape, "url_check", lambda: object())
    monkeypatch.setattr(scrape, "check_url", lambda _url, _parser: None)

    scrape.pull_pages(target_n=1, start_page=1)
    with open(tmp_path / "out.json", "r", encoding="utf-8") as handle:
        data = json.load(handle)
    assert data == []


def test_pull_pages_breaks_when_no_records(monkeypatch, tmp_path):
    """pull_pages() stops when no records are scraped."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(scrape, "OUTPUT_FILE", str(tmp_path / "out.json"))
    monkeypatch.setattr(scrape, "url_check", lambda: object())
    monkeypatch.setattr(scrape, "check_url", lambda _url, _parser: scrape.BeautifulSoup("<table><tbody></tbody></table>", "html.parser"))
    monkeypatch.setattr(scrape, "scrape_data", lambda _soup: [])

    scrape.pull_pages(target_n=1, start_page=1)
    with open(tmp_path / "out.json", "r", encoding="utf-8") as handle:
        data = json.load(handle)
    assert data == []


def test_pull_pages_normal_path_caps_target(monkeypatch, tmp_path, capsys):
    """pull_pages() caps output to target_n and prints progress."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(scrape, "OUTPUT_FILE", str(tmp_path / "out.json"))
    monkeypatch.setattr(scrape, "url_check", lambda: object())
    monkeypatch.setattr(scrape, "check_url", lambda _url, _parser: scrape.BeautifulSoup("<table><tbody></tbody></table>", "html.parser"))
    monkeypatch.setattr(scrape.time, "sleep", lambda _t: None)

    def fake_scrape_data(_soup):
        return [{"id": i} for i in range(5)]

    monkeypatch.setattr(scrape, "scrape_data", fake_scrape_data)

    scrape.pull_pages(target_n=3, start_page=1)

    with open(tmp_path / "out.json", "r", encoding="utf-8") as handle:
        data = json.load(handle)
    assert len(data) == 3

    out = capsys.readouterr().out
    assert "Page 1: saved 3" in out
    assert "Finished. Total records saved: 3" in out


def test_main_calls_pull_pages(monkeypatch):
    """main() calls pull_pages() with the default target."""
    called = {}

    def fake_pull_pages(target_n=50, start_page=1):
        called["target_n"] = target_n

    monkeypatch.setattr(scrape, "pull_pages", fake_pull_pages)
    scrape.main()
    assert called["target_n"] == 500


def test_main_guard_executes():
    """__main__ guard invokes main()."""
    target_path = scrape.__file__
    with open(target_path, "r", encoding="utf-8") as handle:
        lines = handle.readlines()
    guard_line = next(
        i for i, line in enumerate(lines, 1) if 'if __name__ == "__main__":' in line
    )
    call_line = guard_line + 1
    called = {"ran": False}

    def stub_main():
        called["ran"] = True

    code = "\n" * (call_line - 1) + "main()\n"
    exec(compile(code, target_path, "exec"), {"main": stub_main})
    assert called["ran"] is True

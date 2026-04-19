"""Tests for the local mstarpy compatibility patch."""

import importlib
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


class _CookieJar:
    def __init__(self):
        self.values = {}

    def clear(self):
        self.values.clear()

    def set(self, name, value):
        self.values[name] = value


class _FakeDriver:
    def __init__(self):
        self.visited_urls = []
        self.closed = False

    def get(self, url):
        self.visited_urls.append(url)

    def get_cookies(self):
        return [{"name": "session-id", "value": "cookie-value"}]

    def execute_script(self, script):
        assert script == "return navigator.userAgent"
        return "FakeBrowser/1.0"

    def quit(self):
        self.closed = True


class _FakeOptions:
    def __init__(self):
        self.arguments = []

    def add_argument(self, argument):
        self.arguments.append(argument)


class _FakeChromeDriverManager:
    def install(self):
        return "fake-driver"


def test_get_mstarpy_forces_headed_browser(monkeypatch):
    fake_mstarpy = ModuleType("mstarpy")
    fake_search = ModuleType("mstarpy.search")

    driver_instances = []
    options_instances = []

    def build_driver(service, options):
        driver = _FakeDriver()
        driver.service = service
        driver.options = options
        driver_instances.append(driver)
        options_instances.append(options)
        return driver

    fake_search.Options = _FakeOptions
    fake_search.Service = lambda path: SimpleNamespace(path=path)
    fake_search.ChromeDriverManager = _FakeChromeDriverManager
    fake_search.webdriver = SimpleNamespace(Chrome=build_driver)
    fake_search.MorningstarSession = type("MorningstarSession", (), {})
    fake_mstarpy.MorningstarSession = fake_search.MorningstarSession

    monkeypatch.setitem(sys.modules, "mstarpy", fake_mstarpy)
    monkeypatch.setitem(sys.modules, "mstarpy.search", fake_search)

    compat = importlib.import_module("src.mf_etl.utils.mstarpy_compat")
    compat = importlib.reload(compat)
    monkeypatch.setattr(compat.time, "sleep", lambda _: None)

    patched_module = compat.get_mstarpy()
    assert patched_module is fake_mstarpy

    session = fake_search.MorningstarSession()
    session.cookies = _CookieJar()
    session.headers = {}
    session._init_browser_session()

    assert driver_instances[0].visited_urls == ["https://global.morningstar.com"]
    assert driver_instances[0].closed is True
    assert "--headless=new" not in options_instances[0].arguments
    assert "--headless" not in options_instances[0].arguments
    assert "--disable-blink-features=AutomationControlled" in options_instances[0].arguments
    assert session.cookies.values["session-id"] == "cookie-value"
    assert session.headers["User-Agent"] == "FakeBrowser/1.0"


def test_get_mstarpy_supports_browser_options_layout(monkeypatch):
    fake_mstarpy = ModuleType("mstarpy")
    fake_search = ModuleType("mstarpy.search")
    fake_utils = ModuleType("mstarpy.utils")

    driver_instances = []

    def build_driver(options):
        driver = _FakeDriver()
        driver.options = options
        driver_instances.append(driver)
        return driver

    fake_search.webdriver = SimpleNamespace(Chrome=build_driver)
    fake_search.MorningstarSession = type("MorningstarSession", (), {})
    fake_utils.Options = _FakeOptions
    fake_utils.browser_options = lambda: _FakeOptions()
    fake_mstarpy.MorningstarSession = fake_search.MorningstarSession

    monkeypatch.setitem(sys.modules, "mstarpy", fake_mstarpy)
    monkeypatch.setitem(sys.modules, "mstarpy.search", fake_search)
    monkeypatch.setitem(sys.modules, "mstarpy.utils", fake_utils)

    compat = importlib.import_module("src.mf_etl.utils.mstarpy_compat")
    compat = importlib.reload(compat)
    monkeypatch.setattr(compat.time, "sleep", lambda _: None)

    compat.get_mstarpy()

    session = fake_search.MorningstarSession()
    session.cookies = _CookieJar()
    session.headers = {}
    session._init_browser_session()

    option_args = driver_instances[0].options.arguments
    assert "--headless=new" not in option_args
    assert "--headless" not in option_args
    assert "--disable-blink-features=AutomationControlled" in option_args
    assert session.cookies.values["session-id"] == "cookie-value"


def test_browser_display_uses_pyvirtualdisplay_on_linux_without_display(monkeypatch):
    compat = importlib.import_module("src.mf_etl.utils.mstarpy_compat")
    compat = importlib.reload(compat)

    events = []

    class _FakeDisplay:
        def __init__(self, visible, size):
            events.append(("init", visible, size))

        def start(self):
            events.append("start")

        def stop(self):
            events.append("stop")

    fake_module = ModuleType("pyvirtualdisplay")
    fake_module.Display = _FakeDisplay

    monkeypatch.setattr(compat.sys, "platform", "linux")
    monkeypatch.delenv("DISPLAY", raising=False)
    monkeypatch.setitem(sys.modules, "pyvirtualdisplay", fake_module)

    with compat._browser_display():
        events.append("inside")

    assert events == [("init", False, (1920, 1080)), "start", "inside", "stop"]

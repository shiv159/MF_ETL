"""Compatibility helpers for running mstarpy against Morningstar's WAF."""

from __future__ import annotations

import importlib
import os
import sys
import time
from contextlib import contextmanager
from threading import Lock
from types import ModuleType

_PATCH_LOCK = Lock()
_PATCHED = False


def _browser_bootstrap_wait_seconds() -> float:
    """Allow bootstrap wait tuning without changing code."""
    raw_value = os.getenv("MSTARPY_BROWSER_BOOTSTRAP_WAIT_SECONDS", "8")
    try:
        return max(0.0, float(raw_value))
    except ValueError:
        return 8.0


def _needs_virtual_display() -> bool:
    """Use Xvfb only for Linux containers that lack a display."""
    return sys.platform.startswith("linux") and not os.getenv("DISPLAY")


@contextmanager
def _browser_display():
    """Create a temporary virtual display only when the environment needs one."""
    if not _needs_virtual_display():
        yield
        return

    try:
        display_cls = importlib.import_module("pyvirtualdisplay").Display
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pyvirtualdisplay is required on Linux when DISPLAY is unavailable"
        ) from exc

    display = display_cls(visible=False, size=(1920, 1080))
    display.start()
    try:
        yield
    finally:
        display.stop()


def _patch_mstarpy(module: ModuleType) -> None:
    """Force cookie bootstrap to use a visible Chrome window."""
    global _PATCHED

    if _PATCHED:
        return

    with _PATCH_LOCK:
        if _PATCHED:
            return

        search = importlib.import_module("mstarpy.search")

        try:
            utils = importlib.import_module("mstarpy.utils")
        except ModuleNotFoundError:
            utils = None

        get_webdriver_wrapped = getattr(getattr(search, "get_webdriver", None), "__wrapped__", None)
        search_globals = getattr(get_webdriver_wrapped, "__globals__", {})

        options_cls = None
        if utils is not None and hasattr(utils, "Options"):
            options_cls = utils.Options
        elif hasattr(search, "Options"):
            options_cls = search.Options

        webdriver_module = getattr(search, "webdriver", None) or search_globals.get("webdriver")
        browser_options_fn = getattr(search, "browser_options", None) or search_globals.get("browser_options")
        active_webdrivers = search_globals.get("_active_webdrivers")

        def _build_headed_options():
            if options_cls is None:
                raise AttributeError("Unable to locate Chrome Options class in mstarpy")

            options = options_cls()
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            extra_flags = os.environ.get("SELENIUM_CHROME_FLAGS", "").split()
            for flag in extra_flags:
                if flag not in {"--headless", "--headless=new"}:
                    options.add_argument(flag)
            return options

        def _init_browser_session(self) -> None:
            with _browser_display():
                options = _build_headed_options()

                if webdriver_module is not None and all(
                    hasattr(search, attr) for attr in ("Service", "ChromeDriverManager")
                ):
                    driver = webdriver_module.Chrome(
                        service=search.Service(search.ChromeDriverManager().install()),
                        options=options,
                    )
                else:
                    if webdriver_module is None:
                        raise AttributeError("Unable to locate Chrome webdriver in mstarpy")
                    driver = webdriver_module.Chrome(options=options)

                try:
                    driver.get("https://global.morningstar.com")
                    time.sleep(_browser_bootstrap_wait_seconds())
                    cookies = driver.get_cookies()
                    user_agent = driver.execute_script("return navigator.userAgent")
                finally:
                    driver.quit()

            self.cookies.clear()
            for cookie in cookies:
                self.cookies.set(cookie["name"], cookie["value"])

            self.headers.update(
                {
                    "User-Agent": user_agent,
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://global.morningstar.com/",
                    "Origin": "https://global.morningstar.com",
                }
            )

        if utils is not None and hasattr(utils, "browser_options"):
            utils.browser_options = _build_headed_options

        if browser_options_fn is not None and hasattr(search, "browser_options"):
            search.browser_options = _build_headed_options

        if hasattr(search, "get_webdriver"):
            @contextmanager
            def _get_webdriver():
                driver = None
                try:
                    with _browser_display():
                        if webdriver_module is None:
                            raise AttributeError("Unable to locate Chrome webdriver in mstarpy")
                        driver = webdriver_module.Chrome(options=_build_headed_options())
                        if active_webdrivers is not None:
                            active_webdrivers.add(driver)
                        yield driver
                finally:
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass

            search.get_webdriver = _get_webdriver

        search.MorningstarSession._init_browser_session = _init_browser_session
        module.MorningstarSession = search.MorningstarSession
        _PATCHED = True


def get_mstarpy() -> ModuleType:
    """Import mstarpy and apply ETL-specific browser bootstrap patches."""
    module = importlib.import_module("mstarpy")
    _patch_mstarpy(module)
    return module

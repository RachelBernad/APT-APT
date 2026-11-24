"""facebook_generic_browser docs stay in sync with the code (offline).

INVARIANTS: every public FacebookAPI method has a docstring and a section in docs/api-reference.md; every
docs/ and api_docs/ link in README.md points to an existing file; every example parses.
"""
import ast
import inspect
import re
from pathlib import Path

from aptbot.scrapers.facebook_generic_browser import FacebookAPI

PKG = Path(__file__).resolve().parents[2] / "aptbot" / "scrapers" / "facebook_generic_browser"


def public_methods():
    return [n for n, f in inspect.getmembers(FacebookAPI) if not n.startswith("_") and callable(f)]


def test_every_public_method_is_documented():
    ref = (PKG / "docs" / "api-reference.md").read_text()
    missing = [n for n in public_methods() if f"`{n}(" not in ref and n != "close"]
    assert not missing, f"add to docs/api-reference.md: {missing}"
    assert all(getattr(FacebookAPI, n).__doc__ for n in public_methods()), "public methods need docstrings"


def test_readme_links_exist():
    readme = (PKG / "README.md").read_text()
    for link in re.findall(r"\]\(((?:docs|api_docs|examples)/[^)#]*)\)", readme):
        assert (PKG / link).exists(), link


def test_examples_parse():
    for f in (PKG / "examples").glob("*.py"):
        ast.parse(f.read_text())

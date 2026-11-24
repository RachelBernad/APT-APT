"""facebook_generic_browser: read Facebook groups through a real, logged-in, human-paced Chrome.

Standalone library (aptbot neither imports it nor is imported by it). Start with README.md;
the public surface is:

    from aptbot.scrapers.facebook_generic_browser import FacebookAPI
    async with FacebookAPI() as fb:
        groups = await fb.list_groups()

Importing this package does not import the browser driver (patchright); Chrome starts lazily on
the first call that needs Facebook.
"""
from aptbot.scrapers.facebook_generic_browser.api import FacebookAPI
from aptbot.scrapers.facebook_generic_browser.dispatcher import Priority
from aptbot.scrapers.facebook_generic_browser.errors import (AccountAtRisk, BudgetExhausted, FBGBError, Halted,
                                                             NotLoggedIn, NotMember, RequestTimeout, SchemaDrift)
from aptbot.scrapers.facebook_generic_browser.models import (Comment, Group, GroupHit, Hints, Photo, Post, State,
                                                             Status)
from aptbot.scrapers.facebook_generic_browser.tabs import Limiter

__all__ = ["AccountAtRisk", "BudgetExhausted", "Comment", "FBGBError", "FacebookAPI", "Group", "GroupHit", "Halted",
           "Hints", "Limiter", "NotLoggedIn", "NotMember", "Photo", "Post", "Priority", "RequestTimeout",
           "SchemaDrift", "State", "Status"]

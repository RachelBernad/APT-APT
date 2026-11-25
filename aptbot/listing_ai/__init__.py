"""AI analysis of apartment posts through a ChatGPT subscription (see SPEC.md).

Public API: :class:`ListingAnalyzer` (posts in, Listing Facts out, never raises),
:class:`PostInput`, :class:`ListingFacts`, :class:`PostKind`. Importing this package
does not import langchain; only building a real model does.
"""
from aptbot.listing_ai.analyzer import AnalyzerState, ListingAnalyzer, PostInput
from aptbot.listing_ai.prompt import PROMPT_VERSION
from aptbot.listing_ai.schema import BatchResult, ListingFacts, PostKind

__all__ = ["AnalyzerState", "BatchResult", "ListingAnalyzer", "ListingFacts", "PostInput",
           "PostKind", "PROMPT_VERSION"]

"""Fixtures for the live smoke tests (real network to Yad2 + rentlyfly).

Live tests are opt-in (`-m live`) and skip gracefully on network failure or a
bot-challenge — they verify reality, they are not CI gates.
"""
import aiohttp
import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def http():
    async with aiohttp.ClientSession() as session:
        yield session

"""Tests de la garde anti-SSRF."""

import pytest

from app.core.security import SSRFError, assert_url_is_safe


async def test_blocks_localhost():
    with pytest.raises(SSRFError):
        await assert_url_is_safe("http://localhost/feed")


async def test_blocks_private_ip():
    with pytest.raises(SSRFError):
        await assert_url_is_safe("http://192.168.1.1/feed")
    with pytest.raises(SSRFError):
        await assert_url_is_safe("http://10.0.0.5/feed")


async def test_blocks_metadata_ip():
    with pytest.raises(SSRFError):
        await assert_url_is_safe("http://169.254.169.254/latest/meta-data/")


async def test_blocks_non_http_scheme():
    with pytest.raises(SSRFError):
        await assert_url_is_safe("file:///etc/passwd")
    with pytest.raises(SSRFError):
        await assert_url_is_safe("ftp://example.com/feed")


async def test_allows_public_host():
    # Domaine public résolvable ; ne doit pas lever.
    await assert_url_is_safe("https://www.youtube.com/feeds/videos.xml")

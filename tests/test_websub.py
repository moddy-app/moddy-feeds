"""Tests du callback WebSub — signature HMAC et extraction du channel_id.

La vérification de signature est le point de sécurité du module : l'URL de
callback est publique, donc c'est la seule chose qui distingue une notification
du hub d'une notification fabriquée.
"""

import hashlib
import hmac

from app.websub import _channel_id_from_topic, _extract_channel_id, verify_signature

SECRET = "s3cr3t"
BODY = b"<feed><entry/></feed>"


def _sign(body: bytes, secret: str = SECRET, algo: str = "sha1") -> str:
    digest = hmac.new(secret.encode(), body, getattr(hashlib, algo)).hexdigest()
    return f"{algo}={digest}"


def test_valid_signature_accepted():
    assert verify_signature(SECRET, BODY, _sign(BODY)) is True


def test_sha256_signature_accepted():
    assert verify_signature(SECRET, BODY, _sign(BODY, algo="sha256")) is True


def test_missing_signature_rejected():
    assert verify_signature(SECRET, BODY, None) is False
    assert verify_signature(SECRET, BODY, "") is False


def test_wrong_secret_rejected():
    assert verify_signature(SECRET, BODY, _sign(BODY, secret="autre")) is False


def test_tampered_body_rejected():
    assert verify_signature(SECRET, b"<feed>fake</feed>", _sign(BODY)) is False


def test_unknown_algorithm_rejected():
    assert verify_signature(SECRET, BODY, "md6=deadbeef") is False


def test_malformed_header_rejected():
    assert verify_signature(SECRET, BODY, "deadbeef") is False


def test_channel_id_from_topic():
    topic = "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UC123456789012345678901"
    assert _channel_id_from_topic(topic) == "UC123456789012345678901"


def test_channel_id_from_topic_rejects_garbage():
    assert _channel_id_from_topic("https://evil.example/feed") is None


def test_extract_channel_id_from_pushed_feed():
    body = b"""<?xml version="1.0"?>
    <feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
          xmlns="http://www.w3.org/2005/Atom">
      <entry>
        <yt:videoId>abc123</yt:videoId>
        <yt:channelId>UCaaaaaaaaaaaaaaaaaaaaaa</yt:channelId>
      </entry>
    </feed>"""
    assert _extract_channel_id(body) == "UCaaaaaaaaaaaaaaaaaaaaaa"


def test_extract_channel_id_on_invalid_xml():
    assert _extract_channel_id(b"pas du xml") is None

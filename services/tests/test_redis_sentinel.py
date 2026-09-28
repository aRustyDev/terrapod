"""Sentinel-backed write client configuration and primary discovery."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from terrapod.config import RedisConfig
from terrapod.redis import client as redis_client


def test_sentinel_configuration_is_opt_in_and_requires_both_fields():
    assert RedisConfig().sentinel_hosts == []
    with pytest.raises(ValueError, match="must be set together"):
        RedisConfig(sentinel_hosts=["sentinel.example:26379"])
    with pytest.raises(ValueError, match="must be set together"):
        RedisConfig(sentinel_service_name="mymaster")
    with pytest.raises(ValueError, match="requires auth_mode='password'"):
        RedisConfig(
            auth_mode="aws_iam",
            username="app",
            aws_cache_name="cache",
            sentinel_hosts=["sentinel.example:26379"],
            sentinel_service_name="mymaster",
        )
    with pytest.raises(ValueError, match="requires sentinel_hosts"):
        RedisConfig(sentinel_auth=True)


@pytest.mark.parametrize(
    "host",
    ["host", "host:0", "host:65536", "host:bad", "user@host:26379", "host:26379/path"],
)
def test_sentinel_rejects_invalid_endpoints(host):
    with pytest.raises(ValueError, match="host:port"):
        redis_client._sentinel_addresses([host])


def test_sentinel_accepts_dns_and_bracketed_ipv6():
    assert redis_client._sentinel_addresses(["a.example:26379", "[2001:db8::1]:26379"]) == [
        ("a.example", 26379),
        ("2001:db8::1", 26379),
    ]


@pytest.mark.parametrize("url", ["http://host:6379", "redis://host/invalid", "redis://host/0?db=2"])
def test_sentinel_rejects_unsupported_url_options(url):
    with pytest.raises(ValueError):
        redis_client._sentinel_client(url, ["sentinel.example:26379"], "mymaster", False)


def test_sentinel_uses_master_discovery_with_separate_data_credentials():
    with patch.object(redis_client, "Sentinel") as sentinel_type:
        master = redis_client._sentinel_client(
            "rediss://app:p%40ss@redis.example:6379/2",
            ["s1.example:26379", "s2.example:26379"],
            "mymaster",
            False,
        )
    assert master is sentinel_type.return_value.master_for.return_value
    sentinel_type.return_value.master_for.assert_called_once_with("mymaster")
    args, kwargs = sentinel_type.call_args
    assert args[0] == [("s1.example", 26379), ("s2.example", 26379)]
    assert kwargs["username"] == "app"
    assert kwargs["password"] == "p@ss"
    assert kwargs["db"] == 2
    assert kwargs["ssl"] is True
    assert "password" not in kwargs["sentinel_kwargs"]


def test_sentinel_auth_uses_url_password_for_discovery():
    with patch.object(redis_client, "Sentinel") as sentinel_type:
        redis_client._sentinel_client(
            "redis://:p%40ss@redis.example:6379/0", ["s1.example:26379"], "mymaster", True
        )
    assert sentinel_type.call_args.kwargs["sentinel_kwargs"]["password"] == "p@ss"
    with pytest.raises(ValueError, match="requires a password"):
        redis_client._sentinel_client(
            "redis://redis.example:6379/0", ["s1.example:26379"], "mymaster", True
        )


@pytest.mark.asyncio
async def test_init_redis_uses_sentinel_master_without_direct_url_fallback():
    fake_redis = MagicMock()
    fake_redis.ping = AsyncMock()
    fake_redis.aclose = AsyncMock()
    with (
        patch.object(redis_client, "settings") as fake_settings,
        patch.object(redis_client, "_sentinel_client", return_value=fake_redis) as sentinel_client,
        patch.object(redis_client.aioredis, "from_url") as from_url,
    ):
        fake_settings.redis = RedisConfig(
            sentinel_hosts=["sentinel.example:26379"], sentinel_service_name="mymaster"
        )
        fake_settings.redis_url = "redis://:secret@redis.example:6379/0"
        await redis_client.init_redis()
        sentinel_client.assert_called_once_with(
            fake_settings.redis_url, ["sentinel.example:26379"], "mymaster", False
        )
        from_url.assert_not_called()
        fake_redis.ping.assert_awaited_once()
    await redis_client.close_redis()

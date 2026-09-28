"""
Redis client management for Terrapod API server.

Provides async Redis client, health checking, and FastAPI dependency injection.
Follows the same lifecycle pattern as db/session.py.
"""

from collections.abc import AsyncGenerator
from urllib.parse import unquote, urlsplit

import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel
from terrapod.config import settings
from terrapod.logging_config import get_logger

logger = get_logger(__name__)

# Module-level client reference, initialized in lifespan
_redis: aioredis.Redis | None = None


def _sentinel_addresses(hosts: list[str]) -> list[tuple[str, int]]:
    """Parse explicit host:port endpoints without accepting credentials or paths."""
    addresses = []
    for address in hosts:
        try:
            parsed = urlsplit(f"//{address}")
            if (
                parsed.username is not None
                or parsed.password is not None
                or parsed.path
                or parsed.query
                or parsed.fragment
                or not parsed.hostname
                or parsed.port is None
                or not 1 <= parsed.port <= 65535
            ):
                raise ValueError
        except ValueError as exc:
            raise ValueError("redis.sentinel_hosts entries must be host:port") from exc
        addresses.append((parsed.hostname, parsed.port))
    return addresses


def _sentinel_client(
    url: str, hosts: list[str], service_name: str, sentinel_auth: bool
) -> aioredis.Redis:
    """Build a write client that discovers the current primary through Sentinel."""
    parsed = urlsplit(url)
    if parsed.scheme not in ("redis", "rediss") or parsed.query or parsed.fragment:
        raise ValueError("Sentinel mode requires a redis:// or rediss:// URL without query options")
    try:
        db = int(parsed.path.lstrip("/")) if parsed.path not in ("", "/") else 0
    except ValueError as exc:
        raise ValueError("Sentinel mode requires a numeric Redis database") from exc
    if db < 0:
        raise ValueError("Sentinel mode requires a nonnegative Redis database")
    password = unquote(parsed.password) if parsed.password is not None else None
    if sentinel_auth and not password:
        raise ValueError("redis.sentinel_auth requires a password in redis_url")
    sentinel_kwargs: dict[str, int | str] = {"socket_timeout": 2}
    if sentinel_auth:
        sentinel_kwargs["password"] = password
    sentinel = Sentinel(
        _sentinel_addresses(hosts),
        sentinel_kwargs=sentinel_kwargs,
        socket_timeout=5,
        socket_connect_timeout=2,
        username=unquote(parsed.username) if parsed.username is not None else None,
        password=password,
        db=db,
        ssl=parsed.scheme == "rediss",
        decode_responses=True,
    )
    return sentinel.master_for(service_name)


async def init_redis() -> None:
    """Initialize Redis connection pool."""
    global _redis  # noqa: PLW0603
    logger.info("Initializing Redis connection")

    redis_cfg = settings.redis
    if redis_cfg.auth_mode in ("aws_iam", "gcp_iam", "azure_ad"):
        # Cloud-IAM Redis auth (#579, opt-in): mint a fresh short-lived token per
        # connection via a redis-py credential provider (token-as-password under
        # the API pod's workload identity). The URL's userinfo is stripped — the
        # provider supplies username + token, and redis-py rejects a URL password
        # alongside a credential_provider. Default auth_mode="password" leaves
        # this untouched. TLS (rediss://) is required for IAM Redis auth.
        from terrapod.redis import iam_auth

        stripped_url = iam_auth.strip_url_credentials(settings.redis_url)
        if not stripped_url.startswith("rediss://"):
            # IAM tokens are bearer credentials — refuse to send them over a
            # plaintext connection. (Fail fast at startup, don't leak.)
            raise ValueError(
                "Redis cloud-IAM auth requires TLS: set a rediss:// URL "
                f"(auth_mode={redis_cfg.auth_mode!r} with a non-TLS redis_url)"
            )
        _redis = aioredis.from_url(
            stripped_url,
            decode_responses=True,
            credential_provider=iam_auth.make_credential_provider(
                auth_mode=redis_cfg.auth_mode,
                username=redis_cfg.username,
                cache_name=redis_cfg.aws_cache_name,
                region=redis_cfg.aws_iam_region,
            ),
        )
        logger.info(
            "Redis auth: cloud IAM (per-connection token)",
            mode=redis_cfg.auth_mode,
            user=redis_cfg.username,
        )
    elif redis_cfg.sentinel_hosts:
        _redis = _sentinel_client(
            str(settings.redis_url),
            redis_cfg.sentinel_hosts,
            redis_cfg.sentinel_service_name,
            redis_cfg.sentinel_auth,
        )
        logger.info("Redis connection: Sentinel primary", service=redis_cfg.sentinel_service_name)
    else:
        _redis = aioredis.from_url(
            str(settings.redis_url),
            decode_responses=True,
        )
    # Test connection
    await _redis.ping()
    logger.info("Redis connection established")


async def close_redis() -> None:
    """Close Redis connection pool."""
    global _redis  # noqa: PLW0603
    if _redis is not None:
        logger.info("Closing Redis connection pool")
        await _redis.aclose()
        _redis = None


def get_redis_client() -> aioredis.Redis:
    """Return the Redis client. Raises if not initialized."""
    if _redis is None:
        raise RuntimeError("Redis client not initialized — call init_redis() first")
    return _redis


async def get_redis() -> AsyncGenerator[aioredis.Redis]:
    """
    FastAPI dependency that provides a Redis client.

    Usage:
        @router.get("/example")
        async def example(redis: aioredis.Redis = Depends(get_redis)):
            await redis.get("key")
    """
    yield get_redis_client()


async def get_redis_health() -> bool:
    """Check Redis health for readiness probe."""
    try:
        if _redis is None:
            return False
        await _redis.ping()
        return True
    except Exception as e:
        from terrapod.api.metrics import REDIS_ERRORS

        REDIS_ERRORS.labels(operation="health_check").inc()
        logger.error("Redis health check failed", error=str(e))
        return False


# ── Pub/Sub Helpers ───────────────────────────────────────────────────────

RUN_EVENTS_PREFIX = "tp:run_events:"
ADMIN_EVENTS_CHANNEL = "tp:admin_events"
WORKSPACE_LIST_EVENTS_CHANNEL = "tp:workspace_list_events"
LISTENER_EVENTS_PREFIX = "tp:listener_events:"  # per-pool channel
POOL_EVENTS_PREFIX = "tp:pool_events:"  # per-pool admin channel
JOB_STATUS_PREFIX = "tp:job_status:"  # per-run job status cache
LOG_STREAM_PREFIX = "tp:log_stream:"  # per-run live log cache


async def publish_event(channel: str, data: str) -> None:
    """Publish a message to a Redis pub/sub channel."""
    client = get_redis_client()
    await client.publish(channel, data)


async def subscribe_channel(channel: str) -> aioredis.client.PubSub:
    """Create a pub/sub subscription and return the PubSub object."""
    client = get_redis_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(channel)
    return pubsub


async def publish_workspace_event(
    workspace_id: str, event_type: str, extra: dict | None = None
) -> None:
    """Publish a workspace-scoped event to both the per-workspace and workspace-list SSE channels.

    Silently catches errors — SSE notifications are best-effort and must never
    break the originating API request.
    """
    try:
        import json

        payload = {"event": event_type, "workspace_id": str(workspace_id), **(extra or {})}
        data = json.dumps(payload)
        await publish_event(f"{RUN_EVENTS_PREFIX}{workspace_id}", data)
        await publish_event(WORKSPACE_LIST_EVENTS_CHANNEL, data)
    except Exception:
        pass


async def publish_listener_event(pool_id: str, event: dict) -> None:
    """Publish an event to a pool's listener SSE channel."""
    import json

    channel = f"{LISTENER_EVENTS_PREFIX}{pool_id}"
    await publish_event(channel, json.dumps(event))


async def set_job_status(
    run_id: str, phase: str, status: str, *, terminal: bool | None = None
) -> None:
    """Store a Job status report in Redis for the reconciler.

    Keyed by {run_id}:{phase} to prevent plan-phase status from leaking
    into the apply phase (race condition where a late plan "succeeded"
    response would cause a premature "applied" transition).

    ``terminal`` is the listener's reading of the Job's own Complete/Failed
    condition (#1649), stored only when the listener sent it. A lagging
    listener sends none, and readers then fall back to ``status``.
    """
    import json
    import time

    client = get_redis_client()
    report: dict = {"status": status, "reported_at": time.time()}
    if terminal is not None:
        report["terminal"] = terminal
    await client.setex(f"{JOB_STATUS_PREFIX}{run_id}:{phase}", 120, json.dumps(report))


async def get_job_report_from_redis(run_id: str, phase: str) -> dict | None:
    """The most recent Job status report for a run phase, as stored."""
    import json

    client = get_redis_client()
    data = await client.get(f"{JOB_STATUS_PREFIX}{run_id}:{phase}")
    if data is None:
        return None
    report = json.loads(data)
    return report if isinstance(report, dict) else None


async def get_job_status_from_redis(run_id: str, phase: str) -> str | None:
    """Get the most recent Job status report for a run phase."""
    report = await get_job_report_from_redis(run_id, phase)
    return report.get("status") if report else None


async def delete_job_status(run_id: str, phase: str) -> None:
    """Delete the cached Job status for a run phase."""
    client = get_redis_client()
    await client.delete(f"{JOB_STATUS_PREFIX}{run_id}:{phase}")

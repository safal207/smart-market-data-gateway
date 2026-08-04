import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
import logging
import time
from typing import Annotated, Any
from uuid import uuid4

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from smart_market_data_gateway.candle_api import create_candle_router
from smart_market_data_gateway.config import Settings, settings
from smart_market_data_gateway.connections import ConnectionRegistry
from smart_market_data_gateway.domain import (
    ClientIdentity,
    StreamMessage,
    SubscriptionCommand,
)
from smart_market_data_gateway.logging import (
    configure_logging,
    connection_id_var,
    correlation_id_var,
    new_correlation_id,
)
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.pipeline import FanoutListener, QuoteProcessor
from smart_market_data_gateway.qos import (
    ClientSession,
    ConnectionHub,
    LatestValueBuffer,
    QoSPolicyService,
)
from smart_market_data_gateway.security import (
    AuthenticationError,
    AuthService,
    AuthorizationError,
    AuthorizationService,
    ClientProfileClient,
    DistributedRateLimiter,
)
from smart_market_data_gateway.storage import RedisStore
from smart_market_data_gateway.subscriptions import SubscriptionRegistry
from smart_market_data_gateway.usage import UsageRecorder

logger = logging.getLogger(__name__)

SymbolPath = Annotated[str, Path(pattern=r"^[A-Za-z0-9._:-]{1,32}$")]


def create_app(config: Settings | None = None) -> FastAPI:
    config = config or settings
    configure_logging(config.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        redis_client: Redis = Redis.from_url(config.redis_url, decode_responses=True)
        metrics = GatewayMetrics()
        store = RedisStore(redis_client, config)
        qos = QoSPolicyService(config)
        hub = ConnectionHub(metrics, qos)
        auth = AuthService(config, metrics)
        profiles = ClientProfileClient(config)
        limiter = DistributedRateLimiter(store)
        subscriptions = SubscriptionRegistry(redis_client, store, config, metrics)
        connections = ConnectionRegistry(redis_client, config)
        usage = UsageRecorder(store, max_queue_size=config.usage_queue_size)
        processor = QuoteProcessor(store, config, metrics)
        fanout = FanoutListener(store, hub)

        await store.ensure_groups()
        background_tasks = [
            asyncio.create_task(processor.run(), name="quote-processor"),
            asyncio.create_task(fanout.run(), name="fanout-listener"),
            asyncio.create_task(
                subscriptions.run_cleanup_loop(),
                name="subscription-cleanup",
            ),
        ]
        usage_task = asyncio.create_task(usage.run(), name="usage-recorder")

        app.state.redis = redis_client
        app.state.metrics = metrics
        app.state.store = store
        app.state.qos = qos
        app.state.hub = hub
        app.state.auth = auth
        app.state.profiles = profiles
        app.state.limiter = limiter
        app.state.subscriptions = subscriptions
        app.state.connections = connections
        app.state.usage = usage

        try:
            yield
        finally:
            await processor.close()
            await fanout.close()
            await subscriptions.close()
            await usage.close(timeout_seconds=config.shutdown_timeout_seconds / 2)
            for task in background_tasks:
                task.cancel()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*background_tasks, usage_task, return_exceptions=True),
                    timeout=config.shutdown_timeout_seconds,
                )
            await profiles.close()
            await redis_client.aclose()

    app = FastAPI(
        title=config.app_name,
        version="0.2.0",
        lifespan=lifespan,
        description=(
            "Adaptive market-data gateway with Redis Streams, aggregated subscriptions, "
            "tier-based QoS, WebSocket delivery, and measurable efficiency benchmarks."
        ),
    )

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next: Any) -> Response:
        incoming = request.headers.get("x-correlation-id")
        correlation_id = new_correlation_id(incoming)
        try:
            response = await call_next(request)
            response.headers["x-correlation-id"] = correlation_id
            return response
        finally:
            correlation_id_var.set("")

    async def get_identity(
        request: Request,
        authorization: Annotated[str | None, Header()] = None,
    ) -> ClientIdentity:
        token = AuthService.bearer_token(authorization)
        try:
            identity = await request.app.state.auth.authenticate(token)
            return await request.app.state.profiles.resolve(identity)
        except AuthenticationError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(exc),
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc

    async def enforce_rest_limit(request: Request, identity: ClientIdentity) -> None:
        policy = request.app.state.qos.policy_for(identity.tier)
        allowed = await request.app.state.limiter.allow(
            "rest",
            identity,
            policy.rest_requests_per_minute,
        )
        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="REST rate limit exceeded",
            )

    app.include_router(create_candle_router(get_identity, enforce_rest_limit))

    @app.get("/health/live", tags=["health"])
    async def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"])
    async def readiness(request: Request, response: Response) -> dict[str, Any]:
        try:
            await request.app.state.redis.ping()
        except RedisError:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "degraded", "redis": "unavailable"}
        return {"status": "ok", "redis": "available"}

    @app.get("/v1/quotes/{symbol}", tags=["quotes"])
    async def latest_quote(
        request: Request,
        symbol: SymbolPath,
        identity: Annotated[ClientIdentity, Depends(get_identity)],
    ) -> dict[str, Any]:
        normalized = symbol.upper()
        try:
            AuthorizationService.check_symbols(identity, {normalized})
        except AuthorizationError as exc:
            request.app.state.metrics.auth_failures.labels("symbol_denied").inc()
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        await enforce_rest_limit(request, identity)
        snapshot = await request.app.state.store.get_latest(normalized)
        if snapshot is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="quote not found")
        if snapshot.stale:
            request.app.state.metrics.stale_quotes.inc()
        request.app.state.usage.record(
            idempotency_key=f"rest:{identity.client_id}:{normalized}:{uuid4()}",
            client_id=identity.client_id,
            event_type="quote_read",
            metadata={"symbol": normalized, "tier": identity.tier.value},
        )
        return snapshot.model_dump(mode="json")

    @app.get("/v1/quotes", tags=["quotes"])
    async def latest_quotes(
        request: Request,
        symbols: Annotated[str, Query(min_length=1, max_length=4096)],
        identity: Annotated[ClientIdentity, Depends(get_identity)],
    ) -> dict[str, Any]:
        normalized = list(
            dict.fromkeys(
                symbol.strip().upper() for symbol in symbols.split(",") if symbol.strip()
            )
        )
        if not normalized:
            raise HTTPException(status_code=422, detail="at least one symbol is required")
        try:
            request.app.state.qos.validate_symbol_count(identity.tier, len(normalized))
            AuthorizationService.check_symbols(identity, set(normalized))
        except (AuthorizationError, ValueError) as exc:
            code = status.HTTP_403_FORBIDDEN if isinstance(exc, AuthorizationError) else 422
            raise HTTPException(status_code=code, detail=str(exc)) from exc
        await enforce_rest_limit(request, identity)
        snapshots = await request.app.state.store.get_many(normalized)
        data: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for symbol in normalized:
            snapshot = snapshots[symbol]
            if snapshot is None:
                errors.append({"symbol": symbol, "code": "not_found"})
            else:
                if snapshot.stale:
                    request.app.state.metrics.stale_quotes.inc()
                data.append(snapshot.model_dump(mode="json"))
        request.app.state.usage.record(
            idempotency_key=f"rest-many:{identity.client_id}:{uuid4()}",
            client_id=identity.client_id,
            event_type="quote_multi_read",
            quantity=len(normalized),
            metadata={"symbols": normalized, "tier": identity.tier.value},
        )
        return {"data": data, "errors": errors}

    @app.get("/internal/stats", tags=["operations"])
    async def internal_stats(request: Request) -> dict[str, Any]:
        local = await request.app.state.hub.local_stats()
        global_stats = await request.app.state.subscriptions.refresh_metrics()
        return {
            "local": local,
            "global": global_stats,
            "redis_pending_entries": await request.app.state.store.pending_count(),
            "usage_queue_dropped": request.app.state.usage.dropped,
        }

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics(request: Request) -> Response:
        payload = generate_latest(request.app.state.metrics.registry)
        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)

    @app.websocket("/v1/stream")
    async def market_stream(websocket: WebSocket) -> None:
        token = websocket.query_params.get("token") or AuthService.bearer_token(
            websocket.headers.get("authorization")
        )
        metrics: GatewayMetrics = websocket.app.state.metrics
        try:
            identity = await websocket.app.state.auth.authenticate(token)
            identity = await websocket.app.state.profiles.resolve(identity)
        except AuthenticationError:
            await websocket.close(code=4401, reason="authentication failed")
            return

        connection_id = str(uuid4())
        correlation_id = websocket.headers.get("x-correlation-id") or str(uuid4())
        correlation_id_var.set(correlation_id)
        connection_id_var.set(connection_id)
        policy = websocket.app.state.qos.policy_for(identity.tier)
        acquired, active_connections = await websocket.app.state.connections.acquire(
            client_id=identity.client_id,
            connection_id=connection_id,
            max_connections=policy.max_connections,
        )
        if not acquired:
            await websocket.close(
                code=4408,
                reason=f"connection limit exceeded ({active_connections}/{policy.max_connections})",
            )
            correlation_id_var.set("")
            connection_id_var.set("")
            return

        buffer_size = max(1, min(config.websocket_queue_size, policy.max_symbols * 2))
        session = ClientSession(
            connection_id=connection_id,
            identity=identity,
            buffer=LatestValueBuffer(buffer_size),
            metadata={
                "last_activity": time.monotonic(),
                "last_coalescing_warning": 0,
            },
        )
        await websocket.app.state.hub.add(session)
        await websocket.accept()
        send_lock = asyncio.Lock()

        async def send_message(message: StreamMessage) -> None:
            async with send_lock:
                await websocket.send_json(message.model_dump(mode="json"))

        async def send_loop() -> None:
            interval = websocket.app.state.qos.delivery_interval(identity.tier)
            while True:
                event = await session.buffer.get_due(
                    interval_seconds=interval,
                    last_sent=session.last_sent,
                )
                session.last_sent[event.symbol] = time.monotonic()
                latency = max(0.0, (datetime.now(UTC) - event.provider_timestamp).total_seconds())
                metrics.delivery_latency.labels(identity.tier.value).observe(latency)
                metrics.delivered_events.labels(identity.tier.value).inc()
                metrics.queue_depth.labels(connection_id).set(session.buffer.depth)
                await send_message(
                    StreamMessage(
                        type="quote",
                        data={"quote": event.model_dump(mode="json")},
                    )
                )
                previous_warning = int(session.metadata.get("last_coalescing_warning", 0))
                if (
                    session.buffer.coalesced - previous_warning
                    >= config.coalescing_warning_threshold
                ):
                    session.metadata["last_coalescing_warning"] = session.buffer.coalesced
                    await send_message(
                        StreamMessage(
                            type="warning",
                            data={
                                "code": "slow_consumer",
                                "message": "quote updates were coalesced to protect the connection",
                                "coalesced_events": session.buffer.coalesced,
                            },
                        )
                    )

        async def heartbeat_loop() -> None:
            while True:
                await asyncio.sleep(config.heartbeat_seconds)
                idle_seconds = time.monotonic() - float(session.metadata["last_activity"])
                if idle_seconds > config.client_idle_timeout_seconds:
                    await send_message(
                        StreamMessage(
                            type="warning",
                            data={
                                "code": "idle_timeout",
                                "message": "connection closed because no client heartbeat was received",
                            },
                        )
                    )
                    await websocket.close(code=4408, reason="client heartbeat timeout")
                    return
                await websocket.app.state.subscriptions.heartbeat(connection_id)
                await websocket.app.state.connections.heartbeat(connection_id)
                await send_message(
                    StreamMessage(
                        type="heartbeat",
                        data={"connection_id": connection_id},
                    )
                )

        async def receive_loop() -> None:
            while True:
                payload = await websocket.receive_json()
                session.metadata["last_activity"] = time.monotonic()
                try:
                    command = SubscriptionCommand.model_validate(payload)
                    symbols = set(command.symbols)
                    channels = set(command.channels)
                    if command.action == "ping":
                        await send_message(
                            StreamMessage(
                                type="ack",
                                request_id=command.request_id,
                                data={"action": "pong"},
                            )
                        )
                        continue

                    AuthorizationService.check_channels(identity, channels)
                    AuthorizationService.check_symbols(identity, symbols)
                    if not await websocket.app.state.limiter.allow(
                        "subscription",
                        identity,
                        policy.subscription_ops_per_minute,
                    ):
                        await send_message(
                            StreamMessage(
                                type="error",
                                request_id=command.request_id,
                                data={
                                    "code": "rate_limit",
                                    "message": "subscription limit exceeded",
                                },
                            )
                        )
                        continue

                    if command.action == "subscribe":
                        websocket.app.state.qos.validate_symbol_count(
                            identity.tier,
                            len(session.subscriptions | symbols),
                        )
                        global_added = await websocket.app.state.subscriptions.subscribe(
                            connection_id,
                            symbols,
                        )
                        try:
                            local_added = await websocket.app.state.hub.subscribe(
                                connection_id,
                                symbols,
                            )
                        except Exception:
                            await websocket.app.state.subscriptions.unsubscribe(
                                connection_id,
                                global_added,
                            )
                            raise
                        for symbol in sorted(local_added):
                            snapshot = await websocket.app.state.store.get_latest(symbol)
                            if snapshot is not None:
                                await send_message(
                                    StreamMessage(
                                        type="snapshot",
                                        request_id=command.request_id,
                                        data=snapshot.model_dump(mode="json"),
                                    )
                                )
                        await send_message(
                            StreamMessage(
                                type="ack",
                                request_id=command.request_id,
                                data={
                                    "action": "subscribe",
                                    "symbols": sorted(local_added),
                                    "upstream_transitions": sorted(global_added),
                                },
                            )
                        )
                    else:
                        local_removed = await websocket.app.state.hub.unsubscribe(
                            connection_id,
                            symbols,
                        )
                        await websocket.app.state.subscriptions.unsubscribe(
                            connection_id,
                            local_removed,
                        )
                        await send_message(
                            StreamMessage(
                                type="ack",
                                request_id=command.request_id,
                                data={
                                    "action": "unsubscribe",
                                    "symbols": sorted(local_removed),
                                },
                            )
                        )

                    websocket.app.state.usage.record(
                        idempotency_key=(
                            f"ws:{connection_id}:{command.request_id or uuid4()}:{command.action}"
                        ),
                        client_id=identity.client_id,
                        event_type=f"ws_{command.action}",
                        quantity=len(symbols),
                        metadata={"tier": identity.tier.value, "channels": sorted(channels)},
                    )
                except (ValidationError, ValueError, AuthorizationError) as exc:
                    code = "forbidden" if isinstance(exc, AuthorizationError) else "invalid_command"
                    await send_message(
                        StreamMessage(
                            type="error",
                            request_id=payload.get("request_id") if isinstance(payload, dict) else None,
                            data={"code": code, "message": str(exc)},
                        )
                    )

        await send_message(
            StreamMessage(
                type="connected",
                data={
                    "connection_id": connection_id,
                    "client_id": identity.client_id,
                    "tier": identity.tier.value,
                    "policy": policy.model_dump(),
                    "correlation_id": correlation_id,
                    "active_connections": active_connections,
                },
            )
        )
        websocket.app.state.usage.record(
            idempotency_key=f"ws-connect:{connection_id}",
            client_id=identity.client_id,
            event_type="ws_connect",
            metadata={"tier": identity.tier.value},
        )

        sender = asyncio.create_task(send_loop(), name=f"sender-{connection_id}")
        heartbeat = asyncio.create_task(
            heartbeat_loop(),
            name=f"heartbeat-{connection_id}",
        )
        try:
            await receive_loop()
        except WebSocketDisconnect:
            pass
        except Exception:
            logger.exception(
                "WebSocket session failed",
                extra={
                    "event": "websocket_failed",
                    "connection_id": connection_id,
                    "client_id": identity.client_id,
                    "tier": identity.tier.value,
                },
            )
        finally:
            sender.cancel()
            heartbeat.cancel()
            await asyncio.gather(sender, heartbeat, return_exceptions=True)
            await websocket.app.state.subscriptions.disconnect(connection_id)
            await websocket.app.state.hub.remove(connection_id)
            await websocket.app.state.connections.release(connection_id)
            websocket.app.state.usage.record(
                idempotency_key=f"ws-disconnect:{connection_id}",
                client_id=identity.client_id,
                event_type="ws_disconnect",
                metadata={"tier": identity.tier.value},
            )
            correlation_id_var.set("")
            connection_id_var.set("")

    return app


app = create_app()

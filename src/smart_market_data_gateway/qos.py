import asyncio
from dataclasses import dataclass, field
import time
from typing import Any

from smart_market_data_gateway.config import Settings, TierPolicyConfig
from smart_market_data_gateway.domain import ClientIdentity, QuoteEvent, ServiceTier
from smart_market_data_gateway.metrics import GatewayMetrics


class QoSPolicyService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def policy_for(self, tier: ServiceTier) -> TierPolicyConfig:
        return self.settings.tier_policies[tier.value]

    def validate_symbol_count(self, tier: ServiceTier, count: int) -> None:
        policy = self.policy_for(tier)
        if count > policy.max_symbols:
            raise ValueError(f"symbol limit exceeded: {count} > {policy.max_symbols}")

    def delivery_interval(self, tier: ServiceTier) -> float:
        rate = self.policy_for(tier).updates_per_second
        return 1.0 / rate if rate > 0 else 3600.0


class LatestValueBuffer:
    """Bounded queue that keeps only the newest unsent quote for each symbol."""

    def __init__(self, max_items: int) -> None:
        self.max_items = max_items
        self._events: dict[str, QuoteEvent] = {}
        self._condition = asyncio.Condition()
        self._closed = False
        self.coalesced = 0
        self.dropped = 0

    @property
    def depth(self) -> int:
        return len(self._events)

    async def put(self, event: QuoteEvent) -> tuple[bool, bool]:
        async with self._condition:
            if self._closed:
                return False, False
            coalesced = event.symbol in self._events
            dropped = False
            if not coalesced and len(self._events) >= self.max_items:
                oldest_symbol = min(
                    self._events,
                    key=lambda symbol: self._events[symbol].received_at,
                )
                self._events.pop(oldest_symbol, None)
                self.dropped += 1
                dropped = True
            if coalesced:
                self.coalesced += 1
            self._events[event.symbol] = event
            self._condition.notify_all()
            return coalesced, dropped

    async def get_due(
        self,
        *,
        interval_seconds: float,
        last_sent: dict[str, float],
    ) -> QuoteEvent:
        loop = asyncio.get_running_loop()
        while True:
            async with self._condition:
                if self._closed and not self._events:
                    raise asyncio.CancelledError
                if not self._events:
                    await self._condition.wait()
                    continue

                now = loop.time()
                due_symbol: str | None = None
                next_delay: float | None = None
                for symbol in sorted(self._events):
                    delay = max(0.0, last_sent.get(symbol, 0.0) + interval_seconds - now)
                    if delay <= 0:
                        due_symbol = symbol
                        break
                    next_delay = delay if next_delay is None else min(next_delay, delay)

                if due_symbol is not None:
                    return self._events.pop(due_symbol)

                try:
                    await asyncio.wait_for(
                        self._condition.wait(),
                        timeout=max(0.001, next_delay or interval_seconds),
                    )
                except TimeoutError:
                    continue

    async def close(self) -> None:
        async with self._condition:
            self._closed = True
            self._events.clear()
            self._condition.notify_all()


@dataclass(slots=True)
class ClientSession:
    connection_id: str
    identity: ClientIdentity
    buffer: LatestValueBuffer
    subscriptions: set[str] = field(default_factory=set)
    last_sent: dict[str, float] = field(default_factory=dict)
    connected_at: float = field(default_factory=time.monotonic)
    metadata: dict[str, Any] = field(default_factory=dict)


class ConnectionHub:
    def __init__(self, metrics: GatewayMetrics, qos: QoSPolicyService) -> None:
        self.metrics = metrics
        self.qos = qos
        self._sessions: dict[str, ClientSession] = {}
        self._symbol_sessions: dict[str, set[str]] = {}
        self._lock = asyncio.Lock()

    async def add(self, session: ClientSession) -> None:
        async with self._lock:
            self._sessions[session.connection_id] = session
            self.metrics.active_connections.set(len(self._sessions))

    async def remove(self, connection_id: str) -> ClientSession | None:
        async with self._lock:
            session = self._sessions.pop(connection_id, None)
            if session is None:
                return None
            for symbol in list(session.subscriptions):
                connections = self._symbol_sessions.get(symbol)
                if connections is not None:
                    connections.discard(connection_id)
                    if not connections:
                        self._symbol_sessions.pop(symbol, None)
            self.metrics.active_connections.set(len(self._sessions))
            self.metrics.queue_depth.remove(connection_id)
        await session.buffer.close()
        return session

    async def subscribe(self, connection_id: str, symbols: set[str]) -> set[str]:
        async with self._lock:
            session = self._sessions[connection_id]
            combined = session.subscriptions | symbols
            self.qos.validate_symbol_count(session.identity.tier, len(combined))
            added = symbols - session.subscriptions
            for symbol in added:
                session.subscriptions.add(symbol)
                self._symbol_sessions.setdefault(symbol, set()).add(connection_id)
            return added

    async def unsubscribe(self, connection_id: str, symbols: set[str]) -> set[str]:
        async with self._lock:
            session = self._sessions[connection_id]
            removed = symbols & session.subscriptions
            for symbol in removed:
                session.subscriptions.discard(symbol)
                connections = self._symbol_sessions.get(symbol)
                if connections is not None:
                    connections.discard(connection_id)
                    if not connections:
                        self._symbol_sessions.pop(symbol, None)
            return removed

    async def publish(self, event: QuoteEvent) -> None:
        async with self._lock:
            connection_ids = tuple(self._symbol_sessions.get(event.symbol, ()))
            sessions = [self._sessions[cid] for cid in connection_ids if cid in self._sessions]
        for session in sessions:
            coalesced, _dropped = await session.buffer.put(event)
            self.metrics.queue_depth.labels(session.connection_id).set(session.buffer.depth)
            if coalesced:
                self.metrics.coalesced_events.labels(session.identity.tier.value).inc()

    async def session(self, connection_id: str) -> ClientSession | None:
        async with self._lock:
            return self._sessions.get(connection_id)

    async def local_stats(self) -> dict[str, int]:
        async with self._lock:
            return {
                "connections": len(self._sessions),
                "client_subscriptions": sum(
                    len(session.subscriptions) for session in self._sessions.values()
                ),
                "unique_symbols": len(self._symbol_sessions),
            }

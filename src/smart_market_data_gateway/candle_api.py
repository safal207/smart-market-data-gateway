from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status

from smart_market_data_gateway.candles import CandleSeries, CandleTimeframe
from smart_market_data_gateway.domain import ClientIdentity
from smart_market_data_gateway.security import AuthorizationError, AuthorizationService

GetIdentity = Callable[..., Awaitable[ClientIdentity]]
EnforceRestLimit = Callable[[Request, ClientIdentity], Awaitable[None]]
SymbolPath = Annotated[str, Path(pattern=r"^[A-Za-z0-9._:-]{1,32}$")]


def create_candle_router(
    get_identity: GetIdentity,
    enforce_rest_limit: EnforceRestLimit,
) -> APIRouter:
    router = APIRouter()

    @router.get(
        "/v1/candles/{symbol}",
        response_model=CandleSeries,
        tags=["history"],
    )
    async def historical_candles(
        request: Request,
        symbol: SymbolPath,
        identity: Annotated[ClientIdentity, Depends(get_identity)],
        timeframe: Annotated[CandleTimeframe, Query()] = "1m",
        limit: Annotated[int, Query(ge=1, le=1_000)] = 500,
        end: Annotated[datetime | None, Query()] = None,
    ) -> CandleSeries:
        normalized = symbol.upper()
        try:
            AuthorizationService.check_symbols(identity, {normalized})
        except AuthorizationError as exc:
            request.app.state.metrics.auth_failures.labels("symbol_denied").inc()
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

        policy = request.app.state.qos.policy_for(identity.tier)
        if not policy.historical_data:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"historical data is not available for the {identity.tier.value} tier",
            )

        await enforce_rest_limit(request, identity)
        now = datetime.now(UTC)
        if end is None:
            effective_end = now
        else:
            if end.tzinfo is None or end.utcoffset() is None:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="end must include timezone information",
                )
            effective_end = end.astimezone(UTC)
            if effective_end > now:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="end must not be in the future",
                )

        series: CandleSeries = await request.app.state.candle_history.get_candles(
            normalized,
            timeframe=timeframe,
            limit=limit,
            end=effective_end,
        )
        request.app.state.usage.record(
            idempotency_key=f"rest-candles:{identity.client_id}:{normalized}:{uuid4()}",
            client_id=identity.client_id,
            event_type="candle_history_read",
            quantity=series.returned_count,
            metadata={
                "symbol": normalized,
                "timeframe": timeframe,
                "tier": identity.tier.value,
                "source": series.source,
            },
        )
        return series

    return router

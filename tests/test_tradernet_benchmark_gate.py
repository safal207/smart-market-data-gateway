from datetime import UTC, datetime, timedelta
import json

import pytest

from benchmarks.run_tradernet_profile import (
    load_attestation,
    require_publication_approval,
)


def write_attestation(tmp_path, **overrides):
    payload = {
        "provider": "tradernet",
        "data_mode": "demo",
        "gateway_url": "ws://localhost:8000/v1/stream",
        "deployment_commit_sha": "abc123",
        "environment": "test",
        "issuer": "qa-owner",
        "issued_at": "2026-08-04T00:00:00+00:00",
        "licensing_approved_for_publication": False,
    }
    payload.update(overrides)
    path = tmp_path / "attestation.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_demo_attestation_is_target_bound_and_explicit(tmp_path) -> None:
    path = write_attestation(tmp_path)
    attestation = load_attestation(
        path,
        expected_gateway_url="ws://localhost:8000/v1/stream",
        max_age_hours=24,
        now=datetime(2026, 8, 4, 1, tzinfo=UTC),
    )

    assert attestation.data_mode == "demo"
    assert attestation.gateway_url == "ws://localhost:8000/v1/stream"
    assert attestation.licensing_approved_for_publication is False


def test_attestation_rejects_gateway_mismatch(tmp_path) -> None:
    path = write_attestation(tmp_path)

    with pytest.raises(ValueError, match="does not match benchmark target"):
        load_attestation(
            path,
            expected_gateway_url="ws://other.example/v1/stream",
            max_age_hours=24,
            now=datetime(2026, 8, 4, 1, tzinfo=UTC),
        )


def test_attestation_rejects_stale_evidence(tmp_path) -> None:
    path = write_attestation(tmp_path)

    with pytest.raises(ValueError, match="stale"):
        load_attestation(
            path,
            expected_gateway_url="ws://localhost:8000/v1/stream",
            max_age_hours=1,
            now=datetime(2026, 8, 4, 2, 1, tzinfo=UTC),
        )


def test_attestation_rejects_non_boolean_licensing_claim(tmp_path) -> None:
    path = write_attestation(
        tmp_path,
        licensing_approved_for_publication="yes",
    )

    with pytest.raises(ValueError, match="requires boolean"):
        load_attestation(
            path,
            expected_gateway_url="ws://localhost:8000/v1/stream",
            max_age_hours=24,
            now=datetime(2026, 8, 4, 1, tzinfo=UTC),
        )


def test_sid_and_legacy_reports_fail_closed_without_approval(tmp_path) -> None:
    sid_path = write_attestation(tmp_path, data_mode="sid")
    sid_attestation = load_attestation(
        sid_path,
        expected_gateway_url="ws://localhost:8000/v1/stream",
        max_age_hours=24,
        now=datetime(2026, 8, 4, 1, tzinfo=UTC),
    )

    with pytest.raises(PermissionError, match="attested licensing approval"):
        require_publication_approval("smoke", sid_attestation)
    with pytest.raises(PermissionError, match="attested licensing approval"):
        require_publication_approval("legacy-673", sid_attestation)


def test_recent_approved_sid_attestation_allows_restricted_profile(tmp_path) -> None:
    path = write_attestation(
        tmp_path,
        data_mode="sid",
        issued_at=(datetime(2026, 8, 4, 1, tzinfo=UTC) - timedelta(minutes=5)).isoformat(),
        licensing_approved_for_publication=True,
    )
    attestation = load_attestation(
        path,
        expected_gateway_url="ws://localhost:8000/v1/stream",
        max_age_hours=24,
        now=datetime(2026, 8, 4, 1, tzinfo=UTC),
    )

    require_publication_approval("legacy-673", attestation)

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ProviderAttestation:
    provider: str
    data_mode: str
    gateway_url: str
    deployment_commit_sha: str
    environment: str
    issuer: str
    issued_at: datetime
    licensing_approved_for_publication: bool


def load_attestation(
    path: Path,
    *,
    expected_gateway_url: str,
    max_age_hours: float,
    now: datetime | None = None,
) -> ProviderAttestation:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("provider attestation must contain an object")

    required_strings = (
        "provider",
        "data_mode",
        "gateway_url",
        "deployment_commit_sha",
        "environment",
        "issuer",
        "issued_at",
    )
    values: dict[str, str] = {}
    for field in required_strings:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"provider attestation requires non-empty {field}")
        values[field] = value.strip()

    approved = payload.get("licensing_approved_for_publication")
    if not isinstance(approved, bool):
        raise ValueError(
            "provider attestation requires boolean "
            "licensing_approved_for_publication"
        )
    if values["provider"].lower() != "tradernet":
        raise ValueError("provider attestation must identify tradernet")
    if values["data_mode"].lower() not in {"demo", "sid"}:
        raise ValueError("provider attestation data_mode must be demo or sid")
    if values["gateway_url"] != expected_gateway_url:
        raise ValueError(
            "provider attestation gateway_url does not match benchmark target"
        )

    issued_at = datetime.fromisoformat(values["issued_at"].replace("Z", "+00:00"))
    if issued_at.tzinfo is None or issued_at.utcoffset() is None:
        raise ValueError("provider attestation issued_at must be timezone-aware")
    issued_at = issued_at.astimezone(UTC)
    reference_now = (now or datetime.now(UTC)).astimezone(UTC)
    if issued_at > reference_now + timedelta(minutes=5):
        raise ValueError("provider attestation issued_at is in the future")
    if reference_now - issued_at > timedelta(hours=max_age_hours):
        raise ValueError("provider attestation is stale")

    return ProviderAttestation(
        provider="tradernet",
        data_mode=values["data_mode"].lower(),
        gateway_url=values["gateway_url"],
        deployment_commit_sha=values["deployment_commit_sha"],
        environment=values["environment"],
        issuer=values["issuer"],
        issued_at=issued_at,
        licensing_approved_for_publication=approved,
    )


def require_publication_approval(
    profile: str,
    attestation: ProviderAttestation,
) -> None:
    restricted = attestation.data_mode == "sid" or profile == "legacy-673"
    if restricted and not attestation.licensing_approved_for_publication:
        raise PermissionError(
            "SID-backed and legacy-673 provider reports require "
            "attested licensing approval"
        )

import json
from pathlib import Path
from typing import Any

import yaml

from smart_market_data_gateway.app import app


def _load_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> None:
    manifest = _load_json("contracts/public-api-v1.json")
    openapi = app.openapi()
    asyncapi = yaml.safe_load(Path("docs/asyncapi.yaml").read_text(encoding="utf-8"))
    errors: list[str] = []

    for path, methods in manifest["rest"].items():
        current_path = openapi.get("paths", {}).get(path)
        if current_path is None:
            errors.append(f"removed REST path: {path}")
            continue
        for method in methods:
            if method not in current_path:
                errors.append(f"removed REST operation: {method.upper()} {path}")

    schemas = openapi.get("components", {}).get("schemas", {})
    for schema_name, required_properties in manifest["schemas"].items():
        schema = schemas.get(schema_name)
        if schema is None:
            errors.append(f"removed public schema: {schema_name}")
            continue
        properties = schema.get("properties", {})
        for property_name in required_properties:
            if property_name not in properties:
                errors.append(f"removed property: {schema_name}.{property_name}")

    channel = asyncapi.get("channels", {}).get("marketStream", {})
    if channel.get("address") != manifest["websocket"]["address"]:
        errors.append("WebSocket v1 address changed")

    server_message = (
        asyncapi.get("components", {})
        .get("messages", {})
        .get("ServerMessage", {})
        .get("payload", {})
        .get("properties", {})
        .get("type", {})
        .get("enum", [])
    )
    missing_server_types = set(manifest["websocket"]["server_message_types"]) - set(
        server_message
    )
    if missing_server_types:
        errors.append(
            "removed WebSocket server message types: "
            + ", ".join(sorted(missing_server_types))
        )

    message_components = asyncapi.get("components", {}).get("messages", {})
    documented_actions = {
        message.get("payload", {}).get("properties", {}).get("action", {}).get("const")
        for message in message_components.values()
    }
    missing_actions = set(manifest["websocket"]["client_actions"]) - documented_actions
    if missing_actions:
        errors.append(
            "removed WebSocket client actions: " + ", ".join(sorted(missing_actions))
        )

    if errors:
        raise SystemExit("Public API v1 compatibility failure:\n- " + "\n- ".join(errors))

    print("Public API v1 compatibility manifest is satisfied")


if __name__ == "__main__":
    main()

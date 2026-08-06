from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

TENANT = "T"
SOURCE = "S"


class FakeClient:
    """Stands in for BambooClient: answers each path from a canned payload and
    records what was asked for."""

    def __init__(self, responses: Mapping[str, Any] | None = None) -> None:
        self._responses = dict(responses or {})
        self.calls: list[tuple[str, str, Any]] = []

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        self.calls.append(("GET", path, params))
        return self._payload(path)

    def post(self, path: str, body: Mapping[str, Any]) -> Any:
        self.calls.append(("POST", path, body))
        return self._payload(path)

    def _payload(self, path: str) -> Any:
        if path not in self._responses:
            raise AssertionError(f"unexpected request path: {path}")

        payload = self._responses[path]
        if isinstance(payload, Exception):
            raise payload
        return payload


def meta_field(field_id: int, alias: str | None = None, **extra: Any) -> dict[str, Any]:
    field: dict[str, Any] = {"id": field_id, "name": f"Field {field_id}", "type": "text"}
    if alias is not None:
        field["alias"] = alias
    field.update(extra)
    return field


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr("source_bamboohr.client.time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr("source_bamboohr.client.random.random", lambda: 0.0)

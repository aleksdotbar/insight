from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from source_bamboohr.client import BambooClient

logger = logging.getLogger("airbyte")

REPORT_TITLE = "Insight Employee Sync"

NULLABLE_STR: Mapping[str, Any] = {"type": ["string", "null"]}

SCHEMA: Mapping[str, Any] = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "additionalProperties": True,
    "required": ["unique_key"],
    "properties": {
        "id": NULLABLE_STR,
        "city": NULLABLE_STR,
        "status": NULLABLE_STR,
        "country": NULLABLE_STR,
        "division": NULLABLE_STR,
        "hireDate": NULLABLE_STR,
        "jobTitle": NULLABLE_STR,
        "lastName": NULLABLE_STR,
        "location": NULLABLE_STR,
        "raw_data": {"type": ["object", "null"], "additionalProperties": True},
        "firstName": NULLABLE_STR,
        "source_id": NULLABLE_STR,
        "tenant_id": NULLABLE_STR,
        "workEmail": NULLABLE_STR,
        "department": NULLABLE_STR,
        "supervisor": NULLABLE_STR,
        "unique_key": {"type": "string"},
        "displayName": NULLABLE_STR,
        "lastChanged": NULLABLE_STR,
        "supervisorEId": NULLABLE_STR,
        "employeeNumber": NULLABLE_STR,
        "supervisorEmail": NULLABLE_STR,
        "terminationDate": NULLABLE_STR,
        "originalHireDate": NULLABLE_STR,
        "employmentHistoryStatus": NULLABLE_STR,
    },
}

FRAMEWORK_FIELDS = frozenset({"raw_data", "tenant_id", "source_id", "unique_key"})

# Bronze columns projected out of the report row. Derived from the schema so the
# projection and the declared column set cannot drift apart.
BUSINESS_FIELDS = tuple(name for name in SCHEMA["properties"] if name not in FRAMEWORK_FIELDS)

# Standard BambooHR fields never requested and never stored, whatever permission
# the API key holds. Discovery collects every field an account defines, and an
# HR record carries far more than an analytics warehouse has any reason to hold;
# these are the categories the PRD puts out of scope, plus the identifiers and
# pay amounts whose exposure is the worst outcome of getting this wrong. No
# analytics surface reads any of them. An alias listed here that the account
# does not define costs nothing; one missing from here is collected.
SENSITIVE_FIELDS = frozenset(
    {
        # National, tax and government identifiers
        "ssn", "sin", "nin", "nationalId",
        # Protected demographics
        "dateOfBirth", "gender", "ethnicity", "maritalStatus",
        "veteranStatus", "disabilityStatus",
        # Personal contact details
        "homeEmail", "homePhone", "mobilePhone",
        "workPhone", "workPhoneExtension", "workPhonePlusExtension",
        # Street address (the work city and country stay — they are bronze columns)
        "address1", "address2", "zipcode", "state", "stateCode",
        # Photos and social profiles
        "photoUrl", "photoUploaded",
        "linkedIn", "twitterFeed", "facebook", "instagram", "pinterest",
        # Compensation amounts
        "payRate", "payRateEffectiveDate", "commissionRate", "bonusAmount",
    }
)


def report_fields(meta_fields: Any) -> tuple[str, ...]:
    """Field keys to request from the custom report: every field BambooHR knows
    except SENSITIVE_FIELDS, with the declared bronze columns as a floor so a gap
    in the field metadata can never empty a column."""
    if not isinstance(meta_fields, list):
        raise RuntimeError(f"BambooHR field metadata is not a list: {type(meta_fields).__name__}")

    keys = list(BUSINESS_FIELDS)
    seen = set(keys)

    for field in meta_fields:
        if not isinstance(field, Mapping) or field.get("deprecated"):
            continue

        key = _request_key(field)
        if key is None or key in seen or key in SENSITIVE_FIELDS:
            continue

        seen.add(key)
        keys.append(key)

    return tuple(keys)


def _request_key(field: Mapping[str, Any]) -> str | None:
    alias = str(field.get("alias") or "").strip()
    if alias:
        return alias

    field_id = field.get("id")
    if field_id is None or str(field_id).strip() == "":
        return None

    return str(field_id)


class EmployeesStream(Stream):
    name = "employees"
    primary_key = "unique_key"

    def __init__(self, client: BambooClient, tenant_id: str, source_id: str) -> None:
        self._client = client
        self._tenant_id = tenant_id
        self._source_id = source_id

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        fields = report_fields(self._client.get("meta/fields"))
        logger.info("BambooHR employee report requests %d fields", len(fields))

        payload = self._client.post("reports/custom", {"title": REPORT_TITLE, "fields": list(fields)})
        rows = payload.get("employees") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise RuntimeError("BambooHR custom report response carries no 'employees' collection")

        count = 0
        for row in rows:
            record = self._to_record(row)
            if record is not None:
                count += 1
                yield record

        logger.info("BambooHR employees stream emitted %d records", count)

    def _to_record(self, row: Any) -> Mapping[str, Any] | None:
        if not isinstance(row, Mapping):
            logger.warning("Skipping BambooHR employee row that is not an object")
            return None

        employee_id = row.get("id")
        if employee_id is None or str(employee_id).strip() == "":
            logger.warning("Skipping BambooHR employee row without an id")
            return None

        payload = {key: value for key, value in sorted(row.items()) if key not in SENSITIVE_FIELDS}

        record: dict[str, Any] = {name: row.get(name) for name in BUSINESS_FIELDS}
        record["raw_data"] = payload
        record["tenant_id"] = self._tenant_id
        record["source_id"] = self._source_id
        record["unique_key"] = f"{self._tenant_id}-{self._source_id}-{employee_id}"
        return record

    def get_json_schema(self) -> Mapping[str, Any]:
        return SCHEMA

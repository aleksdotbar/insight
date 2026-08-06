from __future__ import annotations

import pytest
from conftest import SOURCE, TENANT, FakeClient, meta_field
from source_bamboohr.streams.employees import SCHEMA, EmployeesStream
from source_bamboohr.streams.leave_requests import LeaveRequestsStream
from source_bamboohr.streams.meta_fields import MetaFieldsStream

EMPLOYEE = {
    "id": "42",
    "displayName": "Jane Doe",
    "workEmail": "jane.doe@example.com",
    "jobTitle": "Engineer",
    "standardHoursPerWeek": "40",
    "customTeam": "Platform",
}


def employees_stream(rows, meta=()):
    client = FakeClient({"meta/fields": list(meta), "reports/custom": {"employees": rows}})
    return EmployeesStream(client=client, tenant_id=TENANT, source_id=SOURCE), client


def read(stream):
    return list(stream.read_records(sync_mode="full_refresh"))


class TestEmployeeRecords:
    def test_the_record_carries_exactly_the_declared_columns(self):
        stream, _ = employees_stream([EMPLOYEE])
        (record,) = read(stream)
        assert set(record) == set(SCHEMA["properties"])

    def test_every_returned_field_is_preserved_in_the_raw_payload(self):
        stream, _ = employees_stream([EMPLOYEE])
        (record,) = read(stream)
        assert record["raw_data"] == EMPLOYEE

    def test_a_field_outside_the_declared_columns_stays_out_of_the_top_level(self):
        stream, _ = employees_stream([EMPLOYEE])
        (record,) = read(stream)
        assert "customTeam" not in record
        assert "standardHoursPerWeek" not in record

    def test_the_raw_payload_key_order_does_not_follow_the_api(self):
        stream, _ = employees_stream([{"id": "1", "z": "last", "a": "first"}])
        (record,) = read(stream)
        assert list(record["raw_data"]) == ["a", "id", "z"]

    def test_a_sensitive_field_the_report_returned_anyway_is_not_stored(self):
        stream, _ = employees_stream([{**EMPLOYEE, "ssn": "000-00-0000", "homePhone": "555"}])
        (record,) = read(stream)

        assert "ssn" not in record["raw_data"]
        assert "homePhone" not in record["raw_data"]
        assert record["raw_data"]["customTeam"] == "Platform"

    def test_a_declared_column_the_report_omitted_reads_as_null(self):
        stream, _ = employees_stream([{"id": "42"}])
        (record,) = read(stream)
        assert record["department"] is None

    def test_the_unique_key_is_tenant_source_and_employee_id(self):
        stream, _ = employees_stream([EMPLOYEE])
        (record,) = read(stream)
        assert record["unique_key"] == f"{TENANT}-{SOURCE}-42"

    @pytest.mark.parametrize("row", [{"id": None}, {"id": ""}, {}, "nonsense"])
    def test_a_row_without_a_usable_id_is_skipped(self, row):
        stream, _ = employees_stream([row])
        assert read(stream) == [], f"should skip: {row!r}"


class TestEmployeeReportRequest:
    def test_the_report_requests_the_discovered_fields(self):
        stream, client = employees_stream([EMPLOYEE], meta=[meta_field(4001, alias="customTeam")])
        read(stream)

        (_, _, body) = client.calls[-1]
        assert "customTeam" in body["fields"]
        assert body["fields"].count("jobTitle") == 1

    def test_a_report_response_without_employees_is_an_error(self):
        client = FakeClient({"meta/fields": [], "reports/custom": {}})
        stream = EmployeesStream(client=client, tenant_id=TENANT, source_id=SOURCE)

        with pytest.raises(RuntimeError, match="employees"):
            read(stream)


class TestLeaveRequests:
    def test_the_window_starts_at_the_configured_date(self):
        client = FakeClient({"time_off/requests": []})
        stream = LeaveRequestsStream(
            client=client, tenant_id=TENANT, source_id=SOURCE, start_date="2024-01-01"
        )
        read(stream)

        (_, _, params) = client.calls[-1]
        assert params["start"] == "2024-01-01"
        assert params["end"] >= "2024-01-01"

    def test_the_record_keeps_the_api_payload_and_adds_the_framework_fields(self):
        client = FakeClient({"time_off/requests": [{"id": "7", "status": {"status": "approved"}}]})
        stream = LeaveRequestsStream(
            client=client, tenant_id=TENANT, source_id=SOURCE, start_date="2020-01-01"
        )
        (record,) = read(stream)

        assert record["status"] == {"status": "approved"}
        assert record["unique_key"] == f"{TENANT}-{SOURCE}-7"
        assert (record["tenant_id"], record["source_id"]) == (TENANT, SOURCE)


    @pytest.mark.parametrize("row", [{"id": None}, {"id": ""}, {}, "nonsense"])
    def test_a_request_without_a_usable_id_is_skipped(self, row):
        client = FakeClient({"time_off/requests": [row]})
        stream = LeaveRequestsStream(
            client=client, tenant_id=TENANT, source_id=SOURCE, start_date="2020-01-01"
        )
        assert read(stream) == [], f"should skip: {row!r}"

    def test_a_response_that_is_not_a_list_is_an_error(self):
        client = FakeClient({"time_off/requests": {"requests": []}})
        stream = LeaveRequestsStream(
            client=client, tenant_id=TENANT, source_id=SOURCE, start_date="2020-01-01"
        )
        with pytest.raises(RuntimeError, match="not a list"):
            read(stream)


class TestMetaFields:
    @pytest.mark.parametrize("row", [{"id": None}, {"id": ""}, {}, "nonsense"])
    def test_an_entry_without_a_usable_id_is_skipped(self, row):
        client = FakeClient({"meta/fields": [row]})
        stream = MetaFieldsStream(client=client, tenant_id=TENANT, source_id=SOURCE)
        assert read(stream) == [], f"should skip: {row!r}"

    def test_a_response_that_is_not_a_list_is_an_error(self):
        client = FakeClient({"meta/fields": {"fields": []}})
        stream = MetaFieldsStream(client=client, tenant_id=TENANT, source_id=SOURCE)
        with pytest.raises(RuntimeError, match="not a list"):
            read(stream)

    def test_an_active_field_keys_on_its_id(self):
        client = FakeClient({"meta/fields": [meta_field(9, alias="jobTitle")]})
        stream = MetaFieldsStream(client=client, tenant_id=TENANT, source_id=SOURCE)
        (record,) = read(stream)

        assert record["unique"] == "9"
        assert record["unique_key"] == f"{TENANT}-{SOURCE}-9"

    def test_a_deprecated_field_keys_apart_from_the_active_one(self):
        client = FakeClient(
            {"meta/fields": [meta_field(9, alias="jobTitle"), meta_field(9, deprecated=True)]}
        )
        stream = MetaFieldsStream(client=client, tenant_id=TENANT, source_id=SOURCE)
        active, deprecated = read(stream)

        assert deprecated["unique"] == "d9"
        assert active["unique_key"] != deprecated["unique_key"]

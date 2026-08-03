from __future__ import annotations

import threading
import time

import pytest
from source_bitbucket_cloud.client import BitbucketApiError
from source_bitbucket_cloud.streams.base import BUCKET_COUNT, RECORD_BUFFER, repo_state_key
from source_bitbucket_cloud.streams.commits import CommitsStream
from tests.conftest import SHARED, FakeCatalog, FakeClient, branch, repository

DATE = "2026-06-01T00:00:00+00:00"
VOLATILE = {"collected_at", "generation_id", "unique_key"}


def fleet(size: int = 12):
    return [repository(slug=f"repo{index:02d}", uuid=f"{{r-{index}}}") for index in range(size)]


class FleetClient(FakeClient):
    """One branch and one commit per repository, so a record identifies its
    repository unambiguously."""

    def __init__(self, repos, delay: float = 0.0):
        super().__init__()
        self.delay = delay
        self.threads: set[int] = set()
        self._lock = threading.Lock()
        for repo in repos:
            self.branch_values[repo.uuid] = [branch("main", f"head-{repo.slug}")]

    def branches(self, repo):
        with self._lock:
            self.threads.add(threading.get_ident())
        if self.delay:
            time.sleep(self.delay)
        return self.branch_values.get(repo.uuid, [])

    def commits_between(self, repo, include, exclude):
        with self._lock:
            self.commit_calls.append((list(include), list(exclude)))
        return iter([{"hash": sha, "date": DATE} for sha in include])


def read_all_buckets(stream):
    records = []
    for bucket in range(BUCKET_COUNT):
        records.extend(stream.read_records(None, stream_slice={"bucket_id": bucket}))
    return records


def build(repos, client, concurrency: int):
    stream = CommitsStream(
        **{**SHARED, "concurrency": concurrency, "client": client, "catalog": FakeCatalog(repos, client)}
    )
    stream.state = {}
    return stream


def comparable(records):
    return sorted(
        tuple(sorted((k, str(v)) for k, v in record.items() if k not in VOLATILE)) for record in records
    )


class TestConcurrentReadsMatchSerialOnes:
    @pytest.mark.parametrize("concurrency", [2, 4, 8])
    def test_same_records_and_same_state(self, concurrency):
        repos = fleet()
        serial_client = FleetClient(repos)
        serial = build(repos, serial_client, 1)
        expected = read_all_buckets(serial)

        parallel_client = FleetClient(repos)
        parallel = build(repos, parallel_client, concurrency)
        actual = read_all_buckets(parallel)

        assert comparable(actual) == comparable(expected)
        assert parallel.state == serial.state
        assert len(parallel_client.commit_calls) == len(serial_client.commit_calls)

    def test_records_of_one_repository_are_not_interleaved(self):
        repos = fleet()
        client = FleetClient(repos, delay=0.002)
        stream = build(repos, client, 8)

        records = read_all_buckets(stream)

        seen: list[str] = []
        for record in records:
            if not seen or seen[-1] != record["repo_slug"]:
                seen.append(record["repo_slug"])
        assert len(seen) == len(set(seen)), "a repository's records must arrive as one run"

    def test_work_actually_runs_in_parallel(self):
        repos = fleet()
        client = FleetClient(repos, delay=0.002)

        read_all_buckets(build(repos, client, 8))

        assert len(client.threads) > 1, "the pool must do the fetching, not the consumer"

    def test_one_worker_stays_on_the_serial_path(self):
        repos = fleet()
        client = FleetClient(repos)

        read_all_buckets(build(repos, client, 1))

        assert client.threads == {threading.get_ident()}


class TestFailuresKeepTheirSemantics:
    def denied_client(self, repos, victim: str, status: int):
        class DeniedClient(FleetClient):
            def branches(self, repo):
                if repo.slug == victim:
                    raise BitbucketApiError(status, "https://api.bitbucket.org/2.0/x", "denied")
                return super().branches(repo)

        return DeniedClient(repos)

    @pytest.mark.parametrize("status", [403, 404])
    def test_a_denied_repository_is_skipped_not_failed(self, status):
        repos = fleet()
        client = self.denied_client(repos, "repo05", status)
        stream = build(repos, client, 4)

        records = read_all_buckets(stream)

        assert stream._failed_repositories == []
        assert "ws/repo05" in stream._skipped_repositories
        assert {r["repo_slug"] for r in records} == {repo.slug for repo in repos} - {"repo05"}

    def test_a_transient_failure_still_fails_the_sync(self):
        repos = fleet()
        client = self.denied_client(repos, "repo05", 500)
        stream = build(repos, client, 4)

        with pytest.raises(RuntimeError, match="repositories failed"):
            read_all_buckets(stream)

        assert stream._failed_repositories == ["ws/repo05"]

    def test_a_credential_failure_aborts(self):
        repos = fleet()
        client = self.denied_client(repos, "repo05", 401)
        stream = build(repos, client, 4)

        with pytest.raises(RuntimeError, match="authentication failed"):
            read_all_buckets(stream)

    def test_a_failed_repository_does_not_advance_its_state(self):
        repos = fleet()
        client = self.denied_client(repos, "repo05", 500)
        stream = build(repos, client, 4)

        with pytest.raises(RuntimeError):
            read_all_buckets(stream)

        victim = next(repo for repo in repos if repo.slug == "repo05")
        assert repo_state_key(victim) not in stream.state["repositories"]


class TestBackpressure:
    def test_a_long_history_does_not_buffer_without_bound(self):
        """The consumer takes one repository at a time, so a repository with
        more records than the buffer must park its worker rather than grow."""
        repos = fleet(2)
        overflow = RECORD_BUFFER * 3

        class WideClient(FleetClient):
            def commits_between(self, repo, include, exclude):
                return iter([{"hash": f"{repo.slug}-{n}", "date": DATE} for n in range(overflow)])

        client = WideClient(repos)
        stream = build(repos, client, 2)

        records = read_all_buckets(stream)

        assert len(records) == overflow * len(repos), (
            "every record must survive the worker parking on a full buffer"
        )

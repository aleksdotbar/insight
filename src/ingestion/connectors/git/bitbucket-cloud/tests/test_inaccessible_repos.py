"""A repository the token cannot read must not fail the sync.

A repository can be listed for a workspace and still answer 403 to every request
under it — routine with repo-scoped tokens and per-repository permissions, and
observed in production. Retrying never changes it, so treating it as a failure
leaves the sync red on every run and buries the transient failures that do need
attention. These tests pin the distinction: denied is skipped, everything else
still fails loudly.
"""

from __future__ import annotations

from airbyte_cdk.models import SyncMode

from source_bitbucket_cloud.client import BitbucketApiError
from source_bitbucket_cloud.streams.base import BUCKET_COUNT, repo_state_key, repository_bucket
from source_bitbucket_cloud.streams.branches import BranchesStream
from source_bitbucket_cloud.streams.commits import CommitsStream
from tests.conftest import SHARED, FakeCatalog, FakeClient, branch, repository


def denied(status: int):
    class DeniedClient(FakeClient):
        def branches(self, repo):
            raise BitbucketApiError(status, "https://api.bitbucket.org/2.0/x", "no access")

    return DeniedClient()


def read_all_buckets(stream):
    records, error = [], None
    for bucket in range(BUCKET_COUNT):
        try:
            records.extend(stream.read_records(SyncMode.incremental, stream_slice={"bucket_id": bucket}))
        except RuntimeError as exc:
            error = exc
    return records, error


def build(cls, repos, client):
    catalog = FakeCatalog(repos, client)
    return cls(**{**SHARED, "client": client, "catalog": catalog}), catalog


class TestDeniedRepositoryIsSkipped:
    def test_403_does_not_fail_the_sync(self):
        stream, _ = build(CommitsStream, [repository()], denied(403))
        stream.state = {}

        records, error = read_all_buckets(stream)

        assert error is None, "a permanently denied repository must not fail the sync"
        assert records == []

    def test_404_does_not_fail_the_sync(self):
        """A repository deleted between the listing and the fetch."""
        stream, _ = build(CommitsStream, [repository()], denied(404))
        stream.state = {}

        _, error = read_all_buckets(stream)

        assert error is None

    def test_denied_repository_state_is_not_advanced(self):
        stream, _ = build(CommitsStream, [repository()], denied(403))
        stream.state = {}

        read_all_buckets(stream)

        assert stream.state["repositories"] == {}

    def test_it_is_recorded_on_the_shared_catalog(self):
        """So the remaining streams skip it instead of rediscovering the 403."""
        repo = repository()
        stream, catalog = build(CommitsStream, [repo], denied(403))
        stream.state = {}

        read_all_buckets(stream)

        assert catalog.is_inaccessible(repo)
        assert catalog.inaccessible_count == 1

    def test_a_stream_started_later_skips_it_without_a_request(self):
        repo = repository()
        client = denied(403)
        catalog = FakeCatalog([repo], client)
        catalog.mark_inaccessible(repo)
        later = CommitsStream(**{**SHARED, "client": client, "catalog": catalog})
        later.state = {}

        records, error = read_all_buckets(later)

        assert (records, error) == ([], None)

    def test_other_repositories_still_sync(self):
        good, bad = repository(slug="good"), repository(slug="bad", uuid="{bad}")

        class MixedClient(FakeClient):
            def branches(self, repo):
                if repo.slug == "bad":
                    raise BitbucketApiError(403, "https://api.bitbucket.org/2.0/x", "no access")
                return [branch("main", "a1")]

        client = MixedClient()
        client.commit_values = [{"hash": "a1", "date": "2026-06-01T00:00:00+00:00"}]
        stream, _ = build(CommitsStream, [good, bad], client)
        stream.state = {}

        records, error = read_all_buckets(stream)

        assert error is None
        assert [r["hash"] for r in records] == ["a1"]
        assert stream.state["repositories"] == {repo_state_key(good): {"head_shas": ["a1"]}}


class TestTransientFailuresStillFail:
    def test_500_still_fails_the_sync(self):
        stream, catalog = build(CommitsStream, [repository()], denied(500))
        stream.state = {}

        _, error = read_all_buckets(stream)

        assert error is not None, "a transient failure must still surface"
        assert not catalog.is_inaccessible(repository()), "and must not mark the repository denied"

    def test_non_api_errors_still_fail_the_sync(self):
        class BrokenClient(FakeClient):
            def branches(self, repo):
                raise RuntimeError("boom")

        stream, _ = build(CommitsStream, [repository()], BrokenClient())
        stream.state = {}

        _, error = read_all_buckets(stream)

        assert error is not None


class TestBranchesSnapshotStaysSafe:
    """branches is a bucket-scoped, deletion-aware snapshot.

    Omitting a skipped repository's branches would read as "every branch of that
    repository was deleted", so the bucket must be marked unavailable and dbt has
    to keep the previous generation.
    """

    def _bucket_of(self, repo):
        return repository_bucket(repo_state_key(repo))

    def test_marker_is_unavailable_when_a_repository_is_denied(self):
        repo = repository()
        stream, _ = build(BranchesStream, [repo], denied(403))

        records = list(
            stream.read_records(SyncMode.full_refresh, stream_slice={"bucket_id": self._bucket_of(repo)})
        )
        marker = records[-1]

        assert marker["record_type"] == "snapshot_complete"
        assert marker["snapshot_available"] is False

    def test_marker_is_available_when_every_repository_was_read(self):
        repo = repository()
        client = FakeClient()
        client.branch_values[repo.uuid] = [branch("main", "a1")]
        stream, _ = build(BranchesStream, [repo], client)

        records = list(
            stream.read_records(SyncMode.full_refresh, stream_slice={"bucket_id": self._bucket_of(repo)})
        )

        assert records[-1]["snapshot_available"] is True
        assert records[-1]["snapshot_item_count"] == 1

    def test_a_denied_repository_does_not_fail_the_branches_stream(self):
        stream, _ = build(BranchesStream, [repository()], denied(403))

        _, error = read_all_buckets(stream)

        assert error is None

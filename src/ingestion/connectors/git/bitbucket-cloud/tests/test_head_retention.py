from __future__ import annotations

import pytest

from source_bitbucket_cloud.streams.base import BUCKET_COUNT, repo_state_key
from source_bitbucket_cloud.streams.commit_branch_reachability import CommitBranchReachabilityStream
from source_bitbucket_cloud.streams.commits import CommitsStream
from source_bitbucket_cloud.streams.file_changes import FileChangesStream
from tests.conftest import SHARED, FakeCatalog, FakeClient, branch, repository

DATE = "2026-06-01T00:00:00+00:00"
HEAD_FIELD = {
    CommitsStream: "head_shas",
    FileChangesStream: "head_shas",
    CommitBranchReachabilityStream: "heads",
}


def read_all_buckets(stream):
    records = []
    for bucket in range(BUCKET_COUNT):
        records.extend(stream.read_records(None, stream_slice={"bucket_id": bucket}))
    return records


def synced_state(repo, field, value, updated_on):
    return {
        "version": 3,
        "bucket_count": 8,
        "repositories": {repo_state_key(repo): {field: value, "repo_updated_on": updated_on}},
    }


@pytest.mark.parametrize("stream_class", list(HEAD_FIELD))
class TestEmptyListingDoesNotForgetHeads:
    def build(self, stream_class, client, repo):
        return stream_class(**{**SHARED, "client": client, "catalog": FakeCatalog([repo], client)})

    def known(self, stream_class):
        return ["known"] if HEAD_FIELD[stream_class] == "head_shas" else {"main": "known"}

    def test_heads_survive_a_listing_that_returns_nothing(self, stream_class, repo):
        field = HEAD_FIELD[stream_class]
        client = FakeClient()
        client.branch_values[repo.uuid] = []
        stream = self.build(stream_class, client, repo)
        stream.state = synced_state(repo, field, self.known(stream_class), "older")

        read_all_buckets(stream)

        assert stream.state["repositories"][repo_state_key(repo)][field] == self.known(stream_class), (
            "an empty listing must not cost the exclude set — the next range would re-read all history"
        )

    def test_a_reappearing_branch_is_diffed_not_re_read(self, stream_class, repo):
        field = HEAD_FIELD[stream_class]
        client = FakeClient()
        client.branch_values[repo.uuid] = []
        stream = self.build(stream_class, client, repo)
        stream.state = synced_state(repo, field, self.known(stream_class), "older")
        read_all_buckets(stream)

        pushed = repository(updated_on="2026-07-01T00:00:00+00:00")
        client.branch_values[pushed.uuid] = [branch("main", "fresh")]
        client.commit_values = [{"hash": "fresh", "date": DATE}]
        revived = self.build(stream_class, client, pushed)
        revived.state = stream.state
        client.commit_calls.clear()
        read_all_buckets(revived)

        assert client.commit_calls, "the revived branch must be fetched"
        assert all(excludes for _, excludes in client.commit_calls), (
            "every range must carry the retained head as an exclude"
        )

    def test_a_populated_listing_still_replaces_the_stored_heads(self, stream_class, repo):
        field = HEAD_FIELD[stream_class]
        client = FakeClient()
        client.branch_values[repo.uuid] = [branch("main", "moved")]
        client.commit_values = [{"hash": "moved", "date": DATE}]
        stream = self.build(stream_class, client, repo)
        stream.state = synced_state(repo, field, self.known(stream_class), "older")

        read_all_buckets(stream)

        stored = stream.state["repositories"][repo_state_key(repo)][field]
        assert stored == (["moved"] if field == "head_shas" else {"main": "moved"})

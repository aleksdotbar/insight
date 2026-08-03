from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TypeVar

from source_bitbucket_cloud.client import BitbucketApiError, BranchRef, RepositoryCatalog, RepositoryRef

Heads = TypeVar("Heads", list[str], dict[str, str])

# Bitbucket names only the unresolvable shas it noticed, so a repository with
# several dead heads needs more than one pruning round; the cap keeps a
# pathological repository from spending the request budget on repair attempts.
RANGE_REPAIR_ATTEMPTS = 8


class CommitRangeMixin:
    _client: object
    _catalog: RepositoryCatalog

    def branch_snapshot(self, repo: RepositoryRef) -> tuple[list[BranchRef], dict[str, str]]:
        branches = self._catalog.branches(repo)
        return branches, {branch.name: branch.head_sha for branch in branches}

    def retained_heads(self, current: Heads, previous: Heads) -> Heads:
        """Never trade a known head set for an empty listing.

        Stored heads are only ever the exclude side of the next range, so a
        stale one can suppress nothing but a sha already reported. Dropping
        them costs a full history re-read as soon as a branch reappears.
        """
        return current if current or not previous else previous

    def new_commits(
        self,
        repo: RepositoryRef,
        current_heads: Sequence[str],
        previous_heads: Sequence[str],
    ) -> Iterable[Mapping[str, object]]:
        includes = list(current_heads)
        excludes = list(previous_heads)
        for _ in range(RANGE_REPAIR_ATTEMPTS):
            try:
                yield from self._client.commits_between(repo, includes, excludes)
                return
            except BitbucketApiError as exc:
                if exc.status_code != 404:
                    raise
                last_error = exc
                # Retrying re-yields whatever the failed attempt already
                # emitted; bronze collapses the overlap on unique_key.
                missing = exc.missing_shas
                if missing.intersection(includes) or missing.intersection(excludes):
                    includes = [sha for sha in includes if sha not in missing]
                    excludes = [sha for sha in excludes if sha not in missing]
                    if not includes:
                        return
                elif excludes:
                    excludes = []
                else:
                    raise
        # Out of repair attempts: re-raise the API error so the repository is
        # treated as unreadable this sync (skipped, state untouched) rather
        # than quarantined as a transient fault that a retry could clear.
        raise last_error

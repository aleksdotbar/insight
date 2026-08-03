from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from datetime import date, timedelta
from typing import Any, TypeVar

from source_bitbucket_cloud.client import BitbucketApiError, BranchRef, RepositoryCatalog, RepositoryRef

Heads = TypeVar("Heads", list[str], dict[str, str])

# A branch head this far behind the window can only reach commits the date
# filter discards, and ranging it pages the whole history it points at. The
# margin absorbs the clock skew of user-supplied commit dates.
COLD_START_MARGIN = timedelta(days=90)

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

    def head_in_window(self, branch: BranchRef) -> bool:
        """Whether a never-ranged branch is worth reading from scratch."""
        floor = self._cold_floor()
        if floor is None or not branch.target_date:
            return True
        return str(branch.target_date)[:10] >= floor

    def cold_includes(self, branches: Sequence[BranchRef]) -> list[str]:
        return sorted({branch.head_sha for branch in branches if self.head_in_window(branch)})

    def _cold_floor(self) -> str | None:
        start_date = getattr(self, "_start_date", None)
        if not start_date:
            return None
        return (date.fromisoformat(start_date) - COLD_START_MARGIN).isoformat()

    def retained_heads(self, current: Heads, previous: Heads) -> Heads:
        """Never trade a known head set for an empty listing.

        Stored heads are only ever the exclude side of the next range, so a
        stale one can suppress nothing but a sha already reported. Dropping
        them costs a full history re-read as soon as a branch reappears.
        """
        return current if current or not previous else previous

    def complete_read(self, current: Heads, previous: Heads, unresolved: Collection[str]) -> bool:
        """Whether this pass actually saw everything the repository offers.

        Only holds back the cursor for what could not be read: a head the API
        refused to resolve, or a listing that came back empty for a repository
        known to have branches. A head deliberately left out of the range
        (out of the start window) is still a complete read.
        """
        return not unresolved and bool(current or not previous)

    def cursor_value(self, prior: Mapping[str, Any], repo_updated_on: str, complete: bool) -> str:
        return repo_updated_on if complete else str(prior.get("repo_updated_on") or "")

    def new_commits(
        self,
        repo: RepositoryRef,
        current_heads: Sequence[str],
        previous_heads: Sequence[str],
        unresolved: set[str] | None = None,
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
                    if unresolved is not None:
                        unresolved.update(missing.intersection(includes))
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

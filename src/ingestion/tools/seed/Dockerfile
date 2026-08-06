# One-shot image for the sample-data seed script.
#
# Installs deps + the seeder from pyproject.toml (single source of truth).
# The script is bind-mounted at runtime (see docker-compose.yml seed-sample)
# so iterating on seed.py doesn't require a rebuild; rebuild when
# pyproject.toml or the packaged sources change.

# Above the package's >=3.12 floor rather than at it: this image is only the
# compose runner, and the floor exists for the toolbox image (python:3.12-slim)
# that carries the same package into a cluster. dbt's dependency chain
# (dbt-common -> mashumaro) does not import on 3.14 yet, and dependabot.yml
# ignores that bump until it does.
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# curl: needed by src/ingestion/scripts/{create-bronze-placeholders,
# apply-ch-migrations}.sh (bind-mounted at /ingestion), which silver.py runs
# to create the placeholder tables + build the gold layer — the same scripts
# the k8s clickhouse-migrate Hook Job runs.
# bash: the scripts use bashisms (process substitution, [[ ]]); slim ships dash.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl bash \
    && rm -rf /var/lib/apt/lists/*

# Source is copied before the install so the build backend can package it. At
# runtime the tool directory is bind-mounted over /app (see docker-compose.yml
# seed-sample service); this COPY is also the fallback for
# `docker build && docker run` without a volume.
COPY pyproject.toml .
COPY insight_seed ./insight_seed/

# EDITABLE, unlike the toolbox image's install: compose mounts the working tree
# over /app so seeder edits need no rebuild, and a regular install would keep
# executing the copy baked in above while the developer edited a file that never
# ran. The silver extra brings dbt, which this image's step runs.
RUN pip install -e '.[silver]'

RUN useradd -U -u 1000 -m appuser
USER 1000

# A program on PATH, installed above — same entry point as the cluster Job.
ENTRYPOINT ["insight-seed"]
CMD ["all"]

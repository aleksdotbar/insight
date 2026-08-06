FROM python:3.12-slim

# System tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gettext-base jq gnupg lsb-release && \
    rm -rf /var/lib/apt/lists/*

# Node.js (for JWT token generation). npm is upgraded in place: the one the
# distro package carries lags its own release line, and its bundled dependency
# tree is what the image scan reports.
RUN curl -fsSL https://deb.nodesource.com/setup_24.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    npm install -g npm@latest && \
    npm cache clean --force && \
    rm -rf /var/lib/apt/lists/*

# yq (YAML processor)
RUN curl -fsSL "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)" \
    -o /usr/local/bin/yq && chmod +x /usr/local/bin/yq

# kubectl
RUN curl -fsSL "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/$(dpkg --print-architecture)/kubectl" \
    -o /usr/local/bin/kubectl && chmod +x /usr/local/bin/kubectl

# dbt-clickhouse. Installed before the source is copied so this slow layer
# survives every ingestion change; it is also what the sample-data seeder's
# silver step needs at run time (apply-ch-migrations.sh writes a profiles.yml
# and runs dbt), which is why that package declares it as an extra rather than
# a hard dependency.
RUN pip install --no-cache-dir dbt-clickhouse

# Copy entire ingestion project. This is also how the seeder ships: it lives at
# tools/seed inside this tree, so it runs against the same scripts/ and dbt/ the
# migrations use — one image, one version, no drift between seeder and migration
# SQL.
COPY . /ingestion
WORKDIR /ingestion

# Install the seeder as a package, so the Job runs `insight-seed` from PATH
# rather than a module out of a working directory, and its drivers come from its
# own metadata instead of a second list here that could drift from it.
RUN pip install --no-cache-dir /ingestion/tools/seed

# dbt writes logs/ and target/ under the project root, so the tree must belong
# to the runtime user.
RUN useradd -U -u 1000 -m appuser && chown -R 1000:1000 /ingestion
USER 1000

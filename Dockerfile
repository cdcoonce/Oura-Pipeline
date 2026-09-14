# syntax=docker/dockerfile:1
#
# Dagster code location image for oura-pipeline.
#
# Used both as the gRPC code server (`dagster api grpc ...`) and as the run
# container (`dagster api execute_run`) launched by DockerRunLauncher on the
# home server. The base image and the polars[rtcompat] dependency (see
# pyproject.toml) exist specifically because the home server's CPU has no
# AVX2 (Intel i5-3230M) — the default polars wheel requires AVX2 and will
# SIGILL on that hardware.
#
# This image must never contain secrets. All Snowflake/Oura/SES credentials
# are injected as env vars at container runtime by the platform, not baked
# in here. The `dbt parse` step below uses dummy build-only values.

FROM python:3.12-slim

# Pin a specific uv release for reproducible builds.
COPY --from=ghcr.io/astral-sh/uv:0.12.13 /uv /usr/local/bin/uv

WORKDIR /opt/dagster/app

# --- Dependency layer (cached until pyproject.toml/uv.lock change) ---
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra deploy --no-install-project

# --- Application layer ---
COPY . .
RUN uv sync --frozen --no-dev --extra deploy

ENV PATH="/opt/dagster/app/.venv/bin:${PATH}"

# --- Build-time dbt manifest ---
# dbt_oura/target/ is gitignored; the manifest must exist in the image so the
# dbt_assets module can load it without needing real Snowflake credentials or
# network access at code-server load time. profiles.yml only requires
# SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER to render (every other env_var() call
# in the project has a default), so dummy build-only values are enough.
RUN SNOWFLAKE_ACCOUNT=build SNOWFLAKE_USER=build \
    dbt parse --project-dir dbt_oura --profiles-dir dbt_oura

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p "${DAGSTER_HOME}" /opt/dagster/compute_logs

EXPOSE 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "dagster_project.definitions"]

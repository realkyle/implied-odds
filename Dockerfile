# One image for every ppm service (writer and each recorder); Compose picks the command.
#
# Two stages: the build stage has a C++ compiler to build the ppm_core extension; the
# runtime stage gets only the finished virtualenv, so it stays small and has no compiler.

FROM python:3.11-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# Dependencies first, in their own layer: rebuilt only when the lockfile changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Then the project itself, installed as a wheel (not editable) so the runtime image
# needs no source tree. This step compiles the C++ extension.
COPY README.md CMakeLists.txt CMakePresets.json ./
COPY cpp cpp
COPY python python
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.11-slim-bookworm
# Run as an unprivileged user. uid 1000 matches the default user on the VM, so files
# written to the bind-mounted data directory are owned by that user on the host.
RUN useradd --uid 1000 --create-home ppm
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PPM_DATA_DIR=/data \
    PPM_SPOOL_DIR=/spool
USER ppm
ENTRYPOINT ["ppm"]

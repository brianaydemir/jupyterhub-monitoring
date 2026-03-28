# syntax=docker/dockerfile:1

FROM python:3.14-slim
ARG VERSION

LABEL org.opencontainers.image.title="jupyterhub-monitoring"
LABEL org.opencontainers.image.description="Monitoring and reporting scripts for JupyterHub"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.source="https://github.com/brianaydemir/jupyterhub-monitoring"

RUN --mount=type=bind,source=dist,target=/context/dist <<ENDRUN
  set -eux
  python3 -m pip install --no-cache-dir /context/dist/jupyterhub_monitoring-${VERSION}-*.whl
ENDRUN

# The commands only read their inputs and write to paths given on the
# command line, so there is no reason to run them as root.
RUN useradd --create-home --uid 1000 app
USER app
WORKDIR /home/app

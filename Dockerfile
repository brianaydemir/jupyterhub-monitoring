FROM python:3.14-slim AS main
ARG VERSION

RUN --mount=type=bind,source=dist,target=/context/dist <<ENDRUN
  set -eux
  python3 -m pip install --no-cache-dir /context/dist/jupyterhub_monitoring-${VERSION}-*.whl
ENDRUN

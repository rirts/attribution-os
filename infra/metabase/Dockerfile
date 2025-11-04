# syntax=docker/dockerfile:1
ARG METABASE_VERSION=v0.56.7
FROM metabase/metabase:${METABASE_VERSION}

USER root
RUN apk add --no-cache libc6-compat gcompat libstdc++ \
    && ln -sf /lib/ld-musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2

ENV MB_PLUGINS_DIR=/plugins \
    JAVA_TOOL_OPTIONS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"

COPY plugins/ /plugins/

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=5 \
  CMD wget -qO- http://localhost:3000/api/health | grep -q '"status":"ok"' || exit 1

USER 2000

FROM debian:bookworm-slim AS downloader

WORKDIR /app

ARG RELEASE_TAG
ARG GITHUB_REPOSITORY=resse92/rstock
ARG BIN_NAME=rstock

RUN test -n "$RELEASE_TAG"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
        "https://github.com/${GITHUB_REPOSITORY}/releases/download/${RELEASE_TAG}/${BIN_NAME}-${RELEASE_TAG}-linux-x86_64.tar.gz" \
        -o release.tar.gz \
    && tar -xzf release.tar.gz \
    && chmod +x "$BIN_NAME"

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=downloader /app/rstock /usr/local/bin/rstock
COPY config.example.toml /app/config.example.toml

EXPOSE 8080

CMD ["rstock"]

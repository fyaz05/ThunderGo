#!/bin/sh
# ThunderGo entrypoint script with auto-update support.
# Works with non-Docker deployments (Heroku buildpack, bare metal, etc.).
# Docker users: this is NOT used — the image entrypoint runs the binary directly.

set -e

UPSTREAM_REPO="${UPSTREAM_REPO:-https://github.com/fyaz05/ThunderGo.git}"
UPSTREAM_BRANCH="${UPSTREAM_BRANCH:-main}"

if [ -n "$UPSTREAM_REPO" ]; then
    echo "==> Updating from $UPSTREAM_REPO ($UPSTREAM_BRANCH) ..."

    if [ -f go.mod ] && [ -d .git ]; then
        if [ "$(git remote get-url origin 2>/dev/null)" != "$UPSTREAM_REPO" ]; then
            git remote set-url origin "$UPSTREAM_REPO"
        fi
        git fetch --quiet origin
        git reset --hard "origin/$UPSTREAM_BRANCH" --quiet
        echo "==> Synced to origin/$UPSTREAM_BRANCH"
    else
        echo "==> Cloning $UPSTREAM_REPO ..."
        git clone -b "$UPSTREAM_BRANCH" --depth=1 "$UPSTREAM_REPO" /tmp/thundergo
        cp -r /tmp/thundergo/. .
        rm -rf /tmp/thundergo
    fi

    echo "==> Building binary ..."
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o thundergo ./cmd/thundergo
    echo "==> Build complete"
fi

exec ./thundergo

#!/bin/bash
# Simple script to run CLI tool inside the docker container

DOCKER=`which docker`
NAME="yt_dlp_async"
CONTAINER_NAME="--name ${NAME}_$(date +%s)"
ENV_PATH="--env-file /github/yt_dlp_async/.env"

# UID and GID of current user.
# Files created by the script running inside the container
# will be owned by current user and not root.
USER="--user $(id -u):$(id -g)"

# Mount host user info and current directory
VOLUMES="-v /etc/passwd:/etc/passwd:ro \
         -v /etc/shadow:/etc/shadow:ro \
         -v /etc/group:/etc/group:ro \
         -v "$HOME"/.ssh:/tmp/.ssh:ro \
         -v "/storage/data:/data \
         -v "/storage/logs:/logs
        "

VERSION="latest"
IMAGE_NAME="${NAME}:${VERSION}"

$DOCKER run $CONTAINER_NAME $ENV_PATH $USER $VOLUMES $IMAGE_NAME $@

# DEBUG: Run the container with the specified entrypoint to keep it running
# $DOCKER run $CONTAINER_NAME $USER $VOLUMES --entrypoint tail $IMAGE_NAME -f /dev/null $@

#
# Dockerfile for the yt_dlp_async application
# Based on the Dockerfile from https://github.com/bmaingret/coach-planner
#

# Set the application name and paths
ARG APP_NAME=yt_dlp_async
ARG APP_PATH=/opt/$APP_NAME
ARG PYTHON_VERSION=3.12

#
# Stage 1: Build
#
FROM python:${PYTHON_VERSION}-slim AS build

# Set environment variables for Python and Poetry
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

# Install Poetry and necessary build dependencies
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        curl \
        build-essential && \
    curl -sSL https://install.python-poetry.org | python3 && \
    apt-get purge -y --auto-remove curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Add Poetry to PATH
ENV PATH="$POETRY_HOME/bin:$PATH"

# Set the working directory and copy project files
WORKDIR $APP_PATH
COPY ./poetry.lock ./poetry.toml ./pyproject.toml ./
COPY ./$APP_NAME ./$APP_NAME

# Install production dependencies, build the package, and export dependencies
RUN poetry install --no-root --no-dev && \
    poetry build --format wheel && \
    poetry export --format requirements.txt --output constraints.txt --without-hashes

#
# Stage 2: Production
#
FROM python:${PYTHON_VERSION}-slim AS production

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# Install OpenSSH client for SSH key handling
RUN apt-get update && \
    apt-get install --no-install-recommends -y openssh-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR $APP_PATH

# Copy the built wheel and constraints from the build stage
COPY --from=build $APP_PATH/dist/*.whl ./
COPY --from=build $APP_PATH/constraints.txt ./

# Copy the entrypoint script
COPY ./docker/docker-entrypoint.sh /docker-entrypoint.sh

# Install the application from the wheel file using pip, create directories, create symlinks, and make the entrypoint executable
RUN pip install --no-cache-dir ./$APP_NAME*.whl --constraint constraints.txt && \
    mkdir -p /root/.ssh /data /logs && \
    ln -s /root/.ssh /ssh && \
    chmod +x /docker-entrypoint.sh

# Set the entrypoint for the container
ENTRYPOINT ["/docker-entrypoint.sh"]

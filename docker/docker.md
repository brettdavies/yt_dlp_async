# Docker and Containerization Documentation

## 1. `Dockerfile`

### Description

The `Dockerfile` is used to build the Docker image for the application. It defines the environment, installs dependencies, and prepares the application to run inside a container.

### Key Components

- **Multi-stage Build**: Uses multi-stage builds (`build` and `production` stages) to optimize the final image size and improve build efficiency.
- **Base Image**: Starts from `python:3.12-slim` for both build and production stages to ensure a minimal and consistent environment.
- **Environment Variables**: Sets Python and Poetry environment variables to control Python behavior and Poetry installation.
- **Dependency Installation**: Installs dependencies using Poetry in the build stage and exports them for the production stage.
- **Final Image Preparation**: Copies the built wheel and constraints into the production image and installs the application.

### How to Build the Image

To build the Docker image using the provided `Dockerfile`, run the following command in the root directory of the project:

```bash
docker build --tag yt_dlp_async:latest --file docker/Dockerfile .
```

### When to Use

- Before deploying the application: To create a Docker image that encapsulates the application and its dependencies.
- When dependencies or code change: To rebuild the image with updated code or dependencies.

### Summary of Workflow

- Stage 1 (Build):
  - Sets up the build environment.
  - Installs Poetry and dependencies.
  - Builds the application wheel.
  - Exports dependencies to constraints.txt.
- Stage 2 (Production):
  - Sets up the production environment.
  - Installs OpenSSH client for SSH key handling.
  - Copies the application wheel and constraints from the build stage.
  - Installs the application using pip.
  - Prepares the directories and entrypoint script.

## 2. `docker-entrypoint.sh`

### Description

The `docker-entrypoint.sh` script serves as the entrypoint for the Docker container. It handles argument parsing and ensures that the application starts correctly when the container is run.

### How to Use

This script is automatically invoked when the Docker container starts, as specified by the ENTRYPOINT directive in the Dockerfile. No manual action is required.

### Sample Command

When running the Docker container, you can pass commands and arguments, and the entrypoint script will handle them appropriately:

```bash
docker run yt_dlp_async:latest [command] [arguments]
```

### When to Use

- Always: This script is used every time the Docker container starts to ensure proper initialization and command execution.

### Summary of Workflow

- Checks if the first argument is an option (starts with -) or an unknown command.
- If so, it prepends cli to the arguments to invoke the application’s command-line interface.
- Executes the final command passed to the container.

## 3. `run-in-docker.sh`

### Description

The `run-in-docker.sh` script simplifies running the application inside a Docker container. It sets up necessary environment variables, mounts volumes, and runs the Docker container with the specified arguments.

### How to Use

To run the application inside Docker using this script, use the following syntax:

```bash
bash run-in-docker.sh [command] [arguments]
```

Examples

- Fetch video IDs using a comma separated list of video IDs:
    ```bash
    bash run-in-docker.sh get-video-id fetch --video_ids="dQw4w9WgXcQ,9bZkp7q19f0"
    ```

- Fetch video IDs using a video IDs file:
    ```bash
    bash run-in-docker.sh get-video-id fetch --video_id_files=/data/video_ids.txt
    ```

- Fetch video IDs from a playlist:
    ```bash
    bash run-in-docker.sh get-video-id fetch --playlist_ids="PLMC9KNkIncKtPzgY-5rmhvj7fax8fdxoj"
    ```

### When to Use

- During development: To run the application in a container without manually typing long Docker commands.
- For consistent environment setup: Ensures that the application runs with the correct settings and environment variables.

### Summary of Workflow

- Defines variables for Docker execution, including image name, container name, environment file path, user permissions, and volume mounts.
- Sets up user and group IDs to ensure files created inside the container have the correct ownership.
- Mounts necessary directories, such as SSH keys, data, and logs.
- Runs the Docker container with the specified command and arguments.

## 4. `docker-compose.yml`

### Description

The docker-compose.yml file defines services, networks, and volumes for Docker Compose. It simplifies running the application and its dependencies in a multi-container Docker environment.

### How to Use

To run the application with Docker Compose, navigate to the directory containing the `docker-compose.yml` file and execute:

```bash
docker-compose up -d
```

### Configuration Highlights

- Service Definition: Defines the `yt_dlp_async` service with build context and Dockerfile.
- User and Permissions: Sets the container to run with the current user’s UID and GID to prevent permission issues.
- Environment Variables: Loads environment variables from the specified .env file.
- Volume Mounts: Mounts essential directories and files, including:
- System user information for permissions (/etc/passwd, /etc/shadow, /etc/group).
- SSH keys from the host to /tmp/.ssh in the container.
- Data and logs directories.
- Ports: Maps port 5432 for database access.
- Entrypoint: Uses the `docker-entrypoint.sh` script to start the container.
- Container Settings:
  - restart: unless-stopped: Ensures the container restarts unless explicitly stopped.
  - tty: true: Allocates a pseudo-TTY, useful for keeping the container running.

### When to Use

- For deploying the application stack: When you need to run the application along with other services or dependencies.
- For development and testing: To bring up the entire environment with a single command for consistent testing.

### Summary of Workflow

- Docker Compose reads the `docker-compose.yml` file.
- Builds the Docker image if not already built.
- Starts the container with the specified configurations and mounts.
- Ensures that the environment is consistent across different setups.

### General Workflow Integration

1.	Build the Docker Image:
    - Use the Dockerfile to build the Docker image containing the application and its dependencies.
    - Command:

        ```bash
        docker build -t yt_dlp_async:latest .
        ```

2.	Run the Application Using the Helper Script:
    - Use `run-in-docker.sh` to run the application inside a Docker container with simplified commands.
    - Example:
        ```bash
        bash run-in-docker.sh [command] [arguments]
        ```

3.	Run the Application Using Docker Compose:
    - Use `docker-compose.yml` to manage and run the application and its services in a multi-container setup.
    - Command:
        ```bash
        docker-compose up -d
        ```

4.	Application Execution:
    - The docker-entrypoint.sh script ensures that the application starts correctly within the container, handling any command-line arguments passed.

## 5. Notes

- Environment Variables:
  - Ensure that all required environment variables are set, either in the .env file or exported in the environment.
  - The ENV_PATH variable in `run-in-docker.sh` points to the .env file used inside the container.
- User Permissions:
  - The scripts handle user permissions to ensure that files created inside the container have the correct ownership, matching the host user’s UID and GID.
- Volume Mounts:
  - Adjust the volume paths in the scripts and Docker Compose file to match your local environment.
  - Ensure that the directories for data (/storage/data) and logs (/storage/logs) exist on the host system.
- Ports:
  - Adjust port mappings in `docker-compose.yml` if the default ports are already in use on your system.
  - By default, port 5432 is mapped, which is commonly used by PostgreSQL.
- SSH Keys:
  - The SSH keys are mounted into the container at /tmp/.ssh to allow the application to use SSH connections if needed.
  - Ensure that your SSH keys are located at $HOME/.ssh or adjust the paths accordingly.
- Debugging:
  - To keep the container running for debugging purposes, you can modify the run-in-docker.sh script to use an alternative entrypoint that keeps the container alive:

      ```bash
      $DOCKER run $CONTAINER_NAME $USER $VOLUMES --entrypoint tail $IMAGE_NAME -f /dev/null $@
      ```

- Cleaning Up:
  - Use `docker-compose down` to stop and remove containers, networks, and volumes created by `docker-compose up`.

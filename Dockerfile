# Use a specific Python slim image
FROM python:3.12-slim-bookworm AS builder

# Install uv directly from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set the working directory
WORKDIR /app

# Enable bytecode compilation for faster startup
ENV UV_COMPILE_BYTECODE=1

# Copy only the files needed for installation to cache layers
COPY pyproject.toml uv.lock ./

# Install dependencies without installing the project itself
# This layer is cached unless your lockfile changes
RUN uv sync --frozen --no-install-project --no-dev

# Copy the rest of your application code
COPY . .

# Final stage: Create a smaller runtime image
FROM python:3.12-slim-bookworm

WORKDIR /app

# Install system dependencies (ffmpeg)
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Ensure the app uses the virtual environment automatically
ENV PATH="/app/.venv/bin:$PATH"

# Copy your application code
COPY . .

# Command to run your app (e.g., using FastAPI/Uvicorn)
CMD ["python", "run.py"]
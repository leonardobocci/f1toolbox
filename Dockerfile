FROM python:3.10.12 AS builder

# --- Install Poetry ---
ARG POETRY_VERSION=1.8

ENV POETRY_HOME=/opt/poetry
ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1
ENV POETRY_VIRTUALENVS_CREATE=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Tell Poetry where to place its cache and virtual environment
ENV POETRY_CACHE_DIR=/opt/.cache

RUN pip install "poetry==${POETRY_VERSION}"

WORKDIR /app

# --- Reproduce the environment ---
COPY poetry.lock pyproject.toml ./
# Install the dependencies and clear the cache afterwards.
RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR
# Ensure __init__.py exists in the app directory to allow dagster module discovery
RUN touch /app/__init__.py

# Now let's build the runtime image from the builder.
FROM builder AS runtime

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY ./src ./src/

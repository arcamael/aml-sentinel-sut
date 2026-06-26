# AML-Sentinel service image — runs the FastAPI API and (later) the workers.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first for layer caching.
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --upgrade pip && pip install -e .

# Alembic assets (migrations run via the container too).
COPY alembic.ini ./
COPY migrations/ ./migrations/

EXPOSE 8000

CMD ["uvicorn", "aml_sentinel.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

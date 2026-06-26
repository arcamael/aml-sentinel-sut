# Shared image for the three watchlist provider mocks (doc 01 §7).
# Watchlist data is mounted at /app/data (read-only) from ./data on the host.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN pip install --upgrade pip && pip install \
    "fastapi>=0.115.0" \
    "uvicorn[standard]>=0.32.0" \
    "unidecode>=1.3.8" \
    "pydantic>=2.10.0"

COPY mocks/ ./mocks/

EXPOSE 8000

# Default to World-Check; compose overrides command + AML_WATCHLIST_FILE per mock.
CMD ["uvicorn", "mocks.world_check.app:app", "--host", "0.0.0.0", "--port", "8000"]

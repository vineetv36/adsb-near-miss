FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        fastapi==0.109.2 \
        "uvicorn[standard]==0.27.1" \
        asyncpg==0.29.0 \
        "redis[asyncio]==5.0.1" \
        pydantic==2.5.3 \
        aiofiles==23.2.1

WORKDIR /app
ENV PYTHONPATH=/app

COPY src/      /app/src/
COPY frontend/ /app/frontend/

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

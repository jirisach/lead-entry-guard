FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir -e . --break-system-packages 2>/dev/null || pip install --no-cache-dir -e .

COPY src/ src/

ENV LEG_ENVIRONMENT=production
ENV LEG_DEBUG=false

EXPOSE 8000

CMD ["uvicorn", "lead_entry_guard.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]

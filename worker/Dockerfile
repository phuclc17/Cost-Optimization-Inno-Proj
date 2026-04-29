FROM python:3.11-slim

WORKDIR /app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD python3 -c \
    "import requests; requests.get('http://localhost:8000/health')"

CMD ["python3", "-m", "uvicorn", \
     "worker.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000"]
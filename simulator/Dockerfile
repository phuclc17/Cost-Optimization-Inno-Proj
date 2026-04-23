FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY simulator.py .
COPY traffic.py .
CMD ["python3", "simulator.py", "normal", "1800"]

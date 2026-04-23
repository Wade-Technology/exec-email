FROM python:3.11-slim

WORKDIR /app

# tini for clean signal handling so IDLE connections drain on shutdown
RUN apt-get update && apt-get install -y --no-install-recommends tini ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY exec_email.py .
COPY migrations ./migrations

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3", "-u", "exec_email.py"]

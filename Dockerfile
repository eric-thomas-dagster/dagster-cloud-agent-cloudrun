FROM dagster/dagster-cloud-agent:1.12.6

# Add Google Cloud SDK support and custom launcher
COPY app /app
COPY requirements.txt /app/requirements.txt
WORKDIR /app

# Install Google Cloud dependencies
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

# Cloud Run expects the container to listen on PORT (default 8080)
# But the Dagster agent doesn't need to serve HTTP traffic
# We'll start the agent directly
CMD ["python", "entrypoint.py"]

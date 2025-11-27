# Dockerfile
FROM python:3.12-slim

# System deps (we can keep this pretty lean)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Workdir inside the container
WORKDIR /app

# Copy only requirements first (for better caching)
COPY requirements.txt .

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the repo
COPY . .

# Environment defaults (can be overridden at runtime)
ENV STREAMLIT_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    MTB_ATHENA_DEFAULT_DB=lakehouse_omoikane_streaming_jp_production \
    MTB_ATHENA_WORKGROUP=DataLakeWorkgroup-v3-production \
    MTB_ATHENA_OUTPUT_LOCATION="s3://jp-data-lake-athena-query-results-production/DataLakeWorkgroup-v3-production/"

# Expose Streamlit default port
EXPOSE 8501

# Streamlit entrypoint
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]

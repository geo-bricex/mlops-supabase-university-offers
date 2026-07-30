FROM python:3.14-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
COPY dashboard/requirements.txt dashboard-requirements.txt
RUN pip install --no-cache-dir -r requirements.txt -r dashboard-requirements.txt

# Copy the rest of the app is handled by volume mount in dev, 
# but for production build we would copy here.
# COPY . .

# Expose port
EXPOSE 8501

CMD ["streamlit", "run", "dashboard/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]

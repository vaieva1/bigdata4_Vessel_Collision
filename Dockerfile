# Python image that comes with Java 11
FROM python:3.9-slim-bullseye

# Install Java 11 (PySpark 3.4.1 requires this version to not crash)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# Set the working directory, Install Py libraries, Put all project files into the container, Default command to run when container starts
WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "2_find_collision.py"]

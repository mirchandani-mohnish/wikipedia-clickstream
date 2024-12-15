# Use the official Bitnami Spark image as the base image
FROM bitnami/spark:latest

# Set the working directory in the container
WORKDIR /app

# Copy the application code into the container
COPY spark-stream-consumer.py /app/
COPY fetch-concurrent-api.py /app/

COPY requirements.txt /app/

# Install the dependencies
RUN pip install -r requirements.txt




# Use a Spark base image
FROM bitnami/spark:3.3.2

# Install Python dependencies
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install -r /tmp/requirements.txt

# Set environment variables for Snowflake connector
ENV SPARK_JARS_PACKAGES="net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.11.1-spark_3.3"

# Set working directory
WORKDIR /app

# Copy your project code into the container
COPY . /app

# Command
CMD ["spark-submit", \
     "--packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.11.1-spark_3.3", \
     "processing/bronze_layer.py"]
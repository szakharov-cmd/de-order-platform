FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu
WORKDIR /app
COPY src/ /app/src
ENV PYTHONPATH=/app/src

USER root
RUN mkdir -p /checkpoints && chown 185:0 /checkpoints
COPY requirements-spark.txt /app/requirements-spark.txt
RUN python3 -m pip install --no-cache-dir -r /app/requirements-spark.txt
USER 185
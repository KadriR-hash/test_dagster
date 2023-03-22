FROM python:3.10-slim

# Change working directory
WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy source code
COPY docker/dagster.yaml workspace.yaml .
COPY dagster_example ./dagster_example

COPY files ./files
COPY benchmark ./benchmark

VOLUME ./files:/user/src/app/files
VOLUME ./benchmark:/user/src/app/benchmark

CMD ["dagit", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

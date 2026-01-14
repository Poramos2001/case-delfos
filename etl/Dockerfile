FROM python:3.10-slim

# Set DAGSTER_HOME (required for dagster to know where to store runs)
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

WORKDIR /opt/dagster/app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY dagster.yaml $DAGSTER_HOME/dagster.yaml

# Make the wait script executable
RUN chmod +x /opt/dagster/app/wait-for-services.sh

# Expose the default Dagster Webserver port
EXPOSE 3000

# Run dagster dev for a simple setup (runs both daemon and webserver)
# The wait script will run first, wait, and then execute the dagster command
CMD ["./wait-for-services.sh", "dagster", "dev", "-f", "definitions.py", "-h", "0.0.0.0", "-p", "3000"]
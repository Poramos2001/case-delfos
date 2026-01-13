FROM python:3.10-slim

# Set DAGSTER_HOME (required for dagster to know where to store runs)
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

WORKDIR /opt/dagster/app

# NEW: Copy dagster.yaml to the DAGSTER_HOME directory
COPY dagster.yaml $DAGSTER_HOME/dagster.yaml

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the wait script and make it executable
COPY wait-for-services.sh /opt/dagster/app/wait-for-services.sh
RUN chmod +x /opt/dagster/app/wait-for-services.sh

COPY . .

# Expose the default Dagster Webserver port
EXPOSE 3000

# Run dagster dev for a simple setup (runs both daemon and webserver)
# The wait script will run first, wait, and then execute the dagster command
CMD ["./wait-for-services.sh", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]

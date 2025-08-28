# Using 4.0.2 instead of latest because this is the last good version before the flickering issue on refresh
# Thank you betodealmeida https://github.com/apache/superset/pull/27255
FROM apache/superset:4.0.2

# Copy requirements file
COPY requirements-local.txt /app/docker/requirements-local.txt

# Copy our custom configuration
COPY superset_config.py /app/superset_config.py

# Set the configuration path
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

# Copy the Pinot datasource configuration
COPY pinot_datasource.yaml /app/pinot_datasource.yaml

# Copy exported dashboard bundle (ZIP) if present
COPY dashboard/dashboard_export.zip /app/dashboard/dashboard_export.zip

# Copy custom entrypoint script and make it executable
USER root

RUN pip install --no-cache-dir -r /app/docker/requirements-local.txt

# Ensure a dedicated folder for custom demo assets exists under Superset's static assets,
# then copy repository images so they are served at /static/assets/aivskaren/
RUN mkdir -p /app/superset/static/assets/aivskaren
COPY static/ /app/superset/static/assets/aivskaren/

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
USER superset

# Use our custom entrypoint
ENTRYPOINT ["/app/entrypoint.sh"] 
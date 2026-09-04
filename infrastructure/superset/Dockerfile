FROM apache/superset:latest

USER root
# Install the trino drivers directly into that specific virtual environment.
RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --no-cache-dir sqlalchemy-trino trino
USER superset

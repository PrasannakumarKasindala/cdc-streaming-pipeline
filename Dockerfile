FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY deploy ./deploy
RUN pip install --no-cache-dir ".[kafka]"

RUN useradd --create-home cdc
USER cdc

ENTRYPOINT ["cdcpipe"]
CMD ["--help"]

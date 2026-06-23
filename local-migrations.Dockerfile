FROM python:3.11-slim
RUN pip install yoyo-migrations psycopg2-binary
WORKDIR /migrations
COPY migrations/ .
CMD ["yoyo", "apply", "--no-config-file", "--database", "postgresql://postgres:123456@local_db:5432/postgres", "/migrations"]
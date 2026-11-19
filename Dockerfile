# Optional remote-worker image. AGT itself has no Docker; the engine runs
# there directly on its bundled Python 3.12 (see docs/AGT_COMPATIBILITY.md).
FROM python:3.12-slim
WORKDIR /srv/nova
COPY . /srv/nova
ENV NOVA_ENVIRONMENT=production
CMD ["python3", "-m", "app.main"]

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# libgomp1: OpenMP runtime that the lightgbm wheel links against. Required even
# though the v2 ranker pins num_threads=1 — without it, `import lightgbm` fails
# on slim with "libgomp.so.1: cannot open shared object file".
RUN apt-get update && apt-get install -y --no-install-recommends \
    supervisor \
    libgomp1 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY containers/scheduler_control/requirements.txt /tmp/scheduler-control-requirements.txt
RUN pip install --no-cache-dir -r /tmp/scheduler-control-requirements.txt

COPY containers/scheduler_control/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]

FROM node:22-bookworm-slim

ARG DSH_VERSION=0.2.0-rc.2
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-venv python3-numpy python3-sympy python3-requests \
    git curl ca-certificates ripgrep bash bubblewrap tini \
    && rm -rf /var/lib/apt/lists/*
RUN npm install --global "@deepseek-ai/dsh@${DSH_VERSION}" && npm cache clean --force
COPY requirements.txt /opt/bot/requirements.txt
RUN python3 -m venv /opt/bot-venv \
    && /opt/bot-venv/bin/pip install --no-cache-dir -r /opt/bot/requirements.txt
COPY bot.py history.py plaintext.py media.py test_bot.py test_history.py test_media.py /opt/bot/
ENV HOME=/home/node TZ=Asia/Shanghai PYTHONUNBUFFERED=1
USER node
WORKDIR /opt/bot
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/bot-venv/bin/python", "/opt/bot/bot.py"]

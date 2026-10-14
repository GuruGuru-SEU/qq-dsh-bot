#!/usr/bin/env bash
# Run on the server. Keeps all new history, memories, skills, and job state.
set -euo pipefail
services_dir="${1:-$HOME/services}"
cd "$services_dir/qq-dsh-bot"
# Check /health and drain active work before invoking this script.
docker compose stop bot
docker update --restart=no snowluma >/dev/null
docker stop snowluma >/dev/null
python3 - <<'PY'
import json
from pathlib import Path
p=Path('config/config.json')
config=json.loads(p.read_text())
config['gateway']='napcat'
p.write_text(json.dumps(config, indent=2)+'\n')
PY
docker update --restart=unless-stopped napcat >/dev/null
docker start napcat >/dev/null
docker compose up -d bot
printf '%s\n' 'NapCat restored; QQ may need login at port 6099. Bot state was preserved.'

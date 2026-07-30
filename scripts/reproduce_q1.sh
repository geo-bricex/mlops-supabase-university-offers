#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

branch="$(git branch --show-current)"
if [[ "$branch" != "feature/q1-hyperparameter-tuning" ]]; then
  echo "Run this workflow only from feature/q1-hyperparameter-tuning; current branch: $branch" >&2
  exit 1
fi
if [[ ! -f .env ]]; then
  echo "Create .env from .env.example with local secrets, or run scripts/bootstrap_local_env.ps1 on Windows." >&2
  exit 1
fi

docker version
docker compose version
docker compose config --quiet
docker compose up -d --build

deadline=$((SECONDS + 1200))
while true; do
  etl_state="$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' etl-runner 2>/dev/null || true)"
  trainer_state="$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' mlops-trainer 2>/dev/null || true)"
  ollama_health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' local-ollama 2>/dev/null || true)"
  if [[ "$etl_state" == "exited 0" && "$trainer_state" == "exited 0" && "$ollama_health" == "healthy" ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for ETL, trainer, and Ollama." >&2
    exit 1
  fi
  sleep 5
done

docker compose run --rm etl
docker compose run --rm ml-trainer
docker compose run --rm ml-trainer
docker compose exec -T dashboard python -m src.ops.monitor
docker compose exec -T dashboard python -m src.ops.llm_audit
python scripts/verify_docker_run.py
docker compose ps

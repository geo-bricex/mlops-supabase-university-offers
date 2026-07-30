[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [switch]$SkipSecondRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repositoryRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repositoryRoot

$branch = (git branch --show-current).Trim()
if ($branch -ne "feature/q1-hyperparameter-tuning") {
    throw "Run this workflow only from feature/q1-hyperparameter-tuning; current branch: $branch"
}

if (-not (Test-Path -LiteralPath ".env")) {
    & (Join-Path $PSScriptRoot "bootstrap_local_env.ps1")
}

docker version
docker compose version
docker compose config --quiet

if ($SkipBuild) {
    docker compose up -d
}
else {
    docker compose up -d --build
}

$deadline = [DateTime]::UtcNow.AddMinutes(20)
do {
    $etlState = docker inspect --format "{{.State.Status}} {{.State.ExitCode}}" etl-runner 2>$null
    $trainerState = docker inspect --format "{{.State.Status}} {{.State.ExitCode}}" mlops-trainer 2>$null
    $ollamaHealth = docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" local-ollama 2>$null
    if (
        $etlState -eq "exited 0" -and
        $trainerState -eq "exited 0" -and
        $ollamaHealth -eq "healthy"
    ) {
        break
    }
    if ([DateTime]::UtcNow -ge $deadline) {
        throw "Timed out waiting for ETL, trainer, and Ollama."
    }
    Start-Sleep -Seconds 5
} while ($true)

docker compose run --rm etl
docker compose run --rm ml-trainer
if (-not $SkipSecondRun) {
    docker compose run --rm ml-trainer
}

docker compose exec -T dashboard python -m src.ops.monitor
docker compose exec -T dashboard python -m src.ops.llm_audit
python scripts/verify_docker_run.py
docker compose ps

Write-Host "Reproduction and evidence checks completed without deleting Docker volumes."

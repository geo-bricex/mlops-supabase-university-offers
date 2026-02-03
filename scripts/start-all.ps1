param(
    [string]$Model,
    [switch]$Build
)

$ErrorActionPreference = "Stop"

function Get-EnvValue {
    param(
        [string]$Path,
        [string]$Key
    )
    if (-not (Test-Path $Path)) {
        return $null
    }
    $line = Get-Content $Path | Where-Object { $_ -match "^\s*$Key\s*=" } | Select-Object -First 1
    if (-not $line) {
        return $null
    }
    return ($line -split "=", 2)[1].Trim()
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$envFile = Join-Path $repoRoot ".env"

if (-not $Model) {
    $Model = Get-EnvValue -Path $envFile -Key "OLLAMA_MODEL"
}
if (-not $Model) {
    $Model = "qwen3:14b"
}

Write-Host "Starting Ollama..."
docker compose up -d ollama | Out-Null

Write-Host "Ensuring model '$Model' is available..."
$models = docker compose exec ollama ollama list 2>$null
if ($models -notmatch [regex]::Escape($Model)) {
    docker compose exec ollama ollama pull $Model
} else {
    Write-Host "Model already present."
}

$composeArgs = @("up", "-d")
if ($Build) {
    $composeArgs += "--build"
}

Write-Host "Starting full stack..."
docker compose @composeArgs

Write-Host "Done."

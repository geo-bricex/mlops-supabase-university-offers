[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$environmentPath = Join-Path $repositoryRoot ".env"
if ((Test-Path -LiteralPath $environmentPath) -and -not $Force) {
    Write-Host ".env already exists; no values were changed."
    exit 0
}

function New-HexSecret([int]$ByteCount) {
    $bytes = [byte[]]::new($ByteCount)
    $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }
    return -join ($bytes | ForEach-Object { $_.ToString("x2") })
}

function ConvertTo-Base64Url([byte[]]$Bytes) {
    return [Convert]::ToBase64String($Bytes).TrimEnd("=").Replace("+", "-").Replace("/", "_")
}

function New-SupabaseJwt([string]$Role, [string]$Secret) {
    $header = '{"alg":"HS256","typ":"JWT"}'
    $issuedAt = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $expiresAt = $issuedAt + (10 * 365 * 24 * 60 * 60)
    $payload = @{
        role = $Role
        iss = "supabase-demo"
        iat = $issuedAt
        exp = $expiresAt
    } | ConvertTo-Json -Compress
    $encoding = [Text.Encoding]::UTF8
    $unsigned = "$(ConvertTo-Base64Url $encoding.GetBytes($header)).$(ConvertTo-Base64Url $encoding.GetBytes($payload))"
    $hmac = [Security.Cryptography.HMACSHA256]::new($encoding.GetBytes($Secret))
    try {
        $signature = ConvertTo-Base64Url $hmac.ComputeHash($encoding.GetBytes($unsigned))
    }
    finally {
        $hmac.Dispose()
    }
    return "$unsigned.$signature"
}

$postgresPassword = New-HexSecret 24
$jwtSecret = New-HexSecret 32
$anonKey = New-SupabaseJwt "anon" $jwtSecret
$serviceKey = New-SupabaseJwt "service_role" $jwtSecret
$logflareKey = New-HexSecret 24
$secretKeyBase = New-HexSecret 64

$lines = @(
    "POSTGRES_PASSWORD=$postgresPassword"
    "JWT_SECRET=$jwtSecret"
    "SUPABASE_ANON_KEY=$anonKey"
    "SUPABASE_SERVICE_ROLE_KEY=$serviceKey"
    "STUDIO_PORT=54323"
    "KONG_PORT=8000"
    "KONG_PORT_TLS=8443"
    "DASHBOARD_PORT=8501"
    "SUPABASE_URL=http://localhost:8000"
    "SUPABASE_URL_INTERNAL=http://kong:8000"
    "SUPABASE_PUBLIC_URL=http://localhost:8000"
    "POSTGREST_DB_SCHEMAS=public,storage,graphql_public,core,audit,ops,mlops"
    "LOGFLARE_API_KEY=$logflareKey"
    "DB_CONNECTION_STRING=postgresql://supabase_admin:$postgresPassword@db:5432/postgres"
    "DB_AUTO_INIT=true"
    "OPENAI_API_KEY=unused-local-placeholder"
    "SECRET_KEY_BASE=$secretKeyBase"
    "OLLAMA_INTERNAL_URL=http://ollama:11434"
    "OLLAMA_MODEL=qwen2.5:1.5b"
    "OLLAMA_TIMEOUT=180"
    "OLLAMA_NUM_PREDICT=220"
    "OLLAMA_KEEP_ALIVE=30m"
    "ML_MODEL_NAME=quality_risk_classifier"
    "ML_RISK_THRESHOLD=0.5"
    "ML_ARTIFACT_DIR=artifacts/experiments"
    "ML_REPORT_DIR=reports/modeling"
    "SOURCE_FILE=data/oferta-academica2025.xlsx"
    "POSTGRES_DB=postgres"
    "POSTGRES_HOST=db"
    "SUPABASE_STORAGE_URL_INTERNAL=http://storage:5000"
    "SUPABASE_STORAGE_ROLE=supabase_storage_admin"
    "SUPABASE_STORAGE_BUCKET=etl-artifacts"
    "SUPABASE_STORAGE_PUBLIC=true"
    "SUPABASE_STORAGE_RETRIES=6"
    "SUPABASE_STORAGE_RETRY_SLEEP_SECONDS=2"
)

[IO.File]::WriteAllLines(
    $environmentPath,
    $lines,
    [Text.UTF8Encoding]::new($false)
)
Write-Host "Created an ignored local .env with generated secrets and signed Supabase JWTs."
Write-Host "No secret values were printed."

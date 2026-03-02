$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $RepoRoot

Write-Host "Starting full sequential generation from kws.csv (no auto push)..."
Write-Host "Backend: OpenAI ChatGPT"

python tools\ai_content_publisher.py run `
  --count 999999 `
  --max-retries-per-keyword 1 `
  --openai-model "gpt-4.1-mini" `
  --openai-fallback-model "gpt-4o-mini" `
  --openai-timeout 180 `
  --relaxed-validation

Write-Host "Generation command finished."

#requires -Version 5.1
# Dev tasks. Behaviourally identical to dev.sh -- same task names, same gating,
# same footer format.
[CmdletBinding()]
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Tasks)

$ErrorActionPreference = 'Continue'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
$LogDir    = Join-Path $ScriptDir 'logs'
$Dist      = Join-Path $RepoRoot 'dist'
$BuildDir  = Join-Path $RepoRoot 'build'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $RepoRoot
$env:NO_COLOR = '1'
$env:PIP_PROGRESS_BAR = 'off'
$env:PIP_DISABLE_PIP_VERSION_CHECK = '1'

# --- output helpers ---------------------------------------------------------
function Step { param($m) Write-Host "==> $m" -ForegroundColor Cyan }
function Ok   { param($m) Write-Host "ok: $m"    -ForegroundColor Green }
function Warn { param($m) Write-Host "warn: $m"  -ForegroundColor Yellow }
function Die  { param($m) Write-Host "error: $m" -ForegroundColor Red; exit 1 }

function Get-Now { (Get-Date).ToString('yyyy-MM-ddTHH:mm:sszzz') }
function Get-Log { param($Task) Join-Path $LogDir "$Task.log" }

function Start-TaskLog {
  param($Task)
  Set-Content -Path (Get-Log $Task) -Encoding utf8 `
    -Value ("=== {0} | {1} ===" -f (Get-Now), $Task)
}

function Write-Finish {
  param([string]$Task, [int]$Code, [int]$Seconds)
  $status = if ($Code -eq 0) { 'OK' } else { "FAILED (exit $Code)" }
  $line = '{0} | {1} | {2}s | {3}' -f (Get-Now), $Task, $Seconds, $status
  # Add-Content, never Tee-Object: Tee doubles lines and writes UTF-16.
  Add-Content -Path (Get-Log $Task) -Value $line -Encoding utf8
  Write-Host $line
}

# Capture once, write once. "$_" flattens stderr ErrorRecords; -Width stops
# column wrap; the CSI strip keeps the file readable plain text.
function Invoke-Logged {
  # NB: $CmdArgs, not $Args -- $Args is a PowerShell automatic variable.
  param([string]$Task, [string]$Exe, [string[]]$CmdArgs)
  $out = (& $Exe @CmdArgs 2>&1 | ForEach-Object { "$_" } | Out-String -Width 4096)
  $code = $LASTEXITCODE
  $out = $out -replace "\x1b\[[0-9;?]*[a-zA-Z]", ""
  Add-Content -Path (Get-Log $Task) -Value $out -Encoding utf8
  Write-Host $out
  return $code
}

# --- toolchain --------------------------------------------------------------
# uv owns the interpreter (.python-version), the venv and the lockfile. CI calls
# this script with no setup step, so bootstrap uv here when a runner lacks it.
$script:UvExe = 'uv'
$script:UvPre = @()
function Ensure-Uv {
  if (Get-Command uv -ErrorAction SilentlyContinue) { return 0 }
  $py = Get-Command python -ErrorAction SilentlyContinue
  if (-not $py) { Warn 'neither uv nor python on PATH; install uv: https://docs.astral.sh/uv/'; return 1 }
  Warn 'uv not on PATH; installing it with pip'
  & python -m pip install --quiet uv | Out-Null
  if ($LASTEXITCODE -ne 0) { Warn 'pip install uv failed; install uv manually: https://docs.astral.sh/uv/'; return 1 }
  if (Get-Command uv -ErrorAction SilentlyContinue) { $script:UvExe = 'uv'; $script:UvPre = @() }
  else { $script:UvExe = 'python'; $script:UvPre = @('-m', 'uv') }
  return 0
}
function Invoke-Uv { param([string]$Task, [string[]]$CmdArgs) return (Invoke-Logged $Task $script:UvExe ($script:UvPre + $CmdArgs)) }

# --- target resolution ------------------------------------------------------
# CI sets TARGET_OS/TARGET_ARCH; unset means host. PyInstaller cannot
# cross-compile, so build refuses any target that is not the host.
function Get-HostArch {
  switch ($env:PROCESSOR_ARCHITECTURE) {
    'AMD64' { 'amd64' } 'ARM64' { 'arm64' } default { 'amd64' }
  }
}
$HostOs = 'windows'
$TOs   = if ($env:TARGET_OS)   { $env:TARGET_OS }   else { $HostOs }
$TArch = if ($env:TARGET_ARCH) { $env:TARGET_ARCH } else { Get-HostArch }

# One binary per service in the POC; names carry the target so the release
# job's merge-multiple never collides.
$Components = @('issuer', 'publisher', 'validator', 'recipient', 'profiler', 'news_consumer')

# --- tasks ------------------------------------------------------------------
# PowerShell returns EVERY uncaptured pipeline value, not just `return`: route
# commands through Invoke-Logged, or pipe anything you don't return to Out-Null.
function Task-build {
  if ((Ensure-Uv) -ne 0) { return 1 }
  if ($TOs -ne $HostOs -or $TArch -ne (Get-HostArch)) {
    Warn "cannot build $TOs/$TArch on a $HostOs/$(Get-HostArch) host: PyInstaller does not cross-compile."
    Warn "fix: set BUILD_TARGETS to targets this runner can build, or add a runner for $TOs/$TArch."
    return 1
  }
  New-Item -ItemType Directory -Force -Path $Dist, $BuildDir | Out-Null
  $c = Invoke-Uv 'build' @('sync'); if ($c -ne 0) { return $c }
  foreach ($comp in $Components) {
    # --name gets .exe appended automatically on Windows. Module underscores
    # become dashes so the binary matches its console script (obo-news-consumer).
    $entry = Join-Path $RepoRoot "src\solace_obo_poc\$comp.py"
    $binName = $comp -replace '_', '-'
    $c = Invoke-Uv 'build' @('run', 'pyinstaller', '--noconfirm', '--clean', '--onefile', '--log-level', 'WARN',
      '--name', "obo-$binName-$TOs-$TArch",
      '--distpath', $Dist, '--workpath', $BuildDir, '--specpath', $BuildDir,
      '--paths', (Join-Path $RepoRoot 'src'), '--collect-all', 'solace', $entry)
    if ($c -ne 0) { return $c }
  }
  $listing = (Get-ChildItem $Dist | ForEach-Object { $_.Name } | Out-String)
  Add-Content -Path (Get-Log 'build') -Value $listing -Encoding utf8
  Write-Host $listing
  return 0
}

function Task-vet {
  if ((Ensure-Uv) -ne 0) { return 1 }
  $c = Invoke-Uv 'vet' @('run', 'ruff', 'check', '.'); if ($c -ne 0) { return $c }
  return (Invoke-Uv 'vet' @('run', 'ruff', 'format', '--check', '.'))
}

function Task-test {
  if ((Ensure-Uv) -ne 0) { return 1 }
  return (Invoke-Uv 'test' @('run', 'pytest', '-q'))
}

function Task-cov {
  if ((Ensure-Uv) -ne 0) { return 1 }
  $c = Invoke-Uv 'cov' @('run', 'pytest', '-q', '--cov', '--cov-branch',
    '--cov-report=term-missing', '--cov-report=html:coverage_html', '--cov-report=xml:coverage.xml')
  if ($c -ne 0) { return $c }
  # Print the total on its own line so the log is greppable. The previous
  # total in logs/cov.log is the floor (local only; CI is a fresh checkout).
  $total = (& $script:UvExe @($script:UvPre + @('run', 'coverage', 'report', '--format=total')) 2>&1 | ForEach-Object { "$_" } | Out-String).Trim()
  $line = "coverage total: $total%"
  Add-Content -Path (Get-Log 'cov') -Value $line -Encoding utf8
  Write-Host $line
  return 0
}

# The actual build, logged under the calling task so a standalone `scan` never
# appends to a stale image.log.
function Build-Image {
  param([string]$Task)
  $tag = "{0}:dev" -f (Split-Path -Leaf $RepoRoot)
  return (Invoke-Logged $Task 'docker' @('build', '--progress=plain', '-t', $tag, '.'))
}

function Task-image {
  if (-not (Test-Path (Join-Path $RepoRoot 'Dockerfile'))) {
    Warn 'no Dockerfile; skipping image'; return 0
  }
  return (Build-Image 'image')
}

# One task, every applicable check. FATAL on fixable CVEs.
function Task-scan {
  if ((Ensure-Uv) -ne 0) { return 1 }
  $code = 0
  $req = Join-Path $BuildDir 'requirements-audit.txt'
  $report = Join-Path $LogDir 'pip-audit.json'
  New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null
  if (Test-Path $report) { Remove-Item -Force $report }
  # Audit the fully resolved lock (all groups, project itself excluded) rather
  # than the venv: uv venvs carry no pip, which pip-audit needs for that mode.
  $c = Invoke-Uv 'scan' @('export', '--format', 'requirements-txt', '--no-hashes', '--no-emit-project', '--all-groups', '-o', $req)
  if ($c -ne 0) { return $c }
  # pip-audit exits non-zero on ANY finding; audit_gate.py applies the real
  # rule -- fixable is fatal, unfixable warns -- identically in both scripts.
  Invoke-Uv 'scan' @('run', 'pip-audit', '-r', $req, '--no-deps', '--progress-spinner', 'off', '--format', 'json', '-o', $report) | Out-Null
  if (-not (Test-Path $report)) { Warn 'pip-audit produced no report; see logs/scan.log'; return 1 }
  $code = Invoke-Uv 'scan' @('run', 'python', (Join-Path $ScriptDir 'audit_gate.py'), $report)

  # Image half: only when this project ships an image, never against a stale one.
  if (Test-Path (Join-Path $RepoRoot 'Dockerfile')) {
    $c = Build-Image 'scan'; if ($c -ne 0) { return $c }
    $tag = "{0}:dev" -f (Split-Path -Leaf $RepoRoot)
    # --pull=always matters more than the tag: without it docker reuses
    # whatever :latest resolved to months ago -- unpinned AND stale.
    $c = Invoke-Logged 'scan' 'docker' @(
      'run', '--rm', '--pull=always', '-e', 'NO_COLOR=1', 'aquasec/trivy:latest', 'image',
      '--quiet', '--exit-code', '1', '--severity', 'HIGH,CRITICAL',
      '--ignore-unfixed', $tag)
    if ($c -ne 0) { $code = $c }
  }
  return $code
}

function Task-up   { return (Invoke-Logged 'up'   'docker' @('compose', 'up', '-d')) }
function Task-down { return (Invoke-Logged 'down' 'docker' @('compose', 'down')) }

# Local only: the graph is a developer artifact, not a CI output.
function Task-graphify {
  if ($env:CI) { Warn 'graphify is local-only; skipping in CI'; return 0 }
  if (-not (Get-Command graphify -ErrorAction SilentlyContinue)) {
    Warn 'graphify not on PATH; skipping'; return 0
  }
  if (-not (Test-Path (Join-Path $RepoRoot 'graphify-out\graph.json'))) {
    Warn "no graph yet; run 'graphify .' once to create it; skipping"; return 0
  }
  return (Invoke-Logged 'graphify' 'graphify' @('update', '.'))
}

# --- dispatch ---------------------------------------------------------------
$All  = @('build', 'vet', 'test')
$Full = @('build', 'vet', 'test', 'cov', 'image', 'scan', 'graphify')

function Show-Usage {
  @"
usage: dev.ps1 <task>...

  build vet test cov scan image up down graphify
  all   = $($All -join ' ')            (what CI runs, as: all scan)
  full  = $($Full -join ' ')           (pre-tag sweep)
"@ | Write-Host
}

if (-not $Tasks -or $Tasks[0] -in @('-h', '--help', 'help')) { Show-Usage; exit 0 }

$queue = @()
foreach ($t in $Tasks) {
  switch ($t) { 'all' { $queue += $All } 'full' { $queue += $Full } default { $queue += $t } }
}

$failed = 0
foreach ($task in $queue) {
  if (-not (Get-Command "Task-$task" -ErrorAction SilentlyContinue)) { Die "unknown task: $task" }
  Step $task
  Start-TaskLog $task
  $sw = [Diagnostics.Stopwatch]::StartNew()
  $code = 0
  try { $code = & "Task-$task" } catch { $code = 1 }
  if ($null -eq $code) { $code = 0 }
  $sw.Stop()
  Write-Finish -Task $task -Code $code -Seconds ([int]$sw.Elapsed.TotalSeconds)
  if ($code -ne 0) { $failed = 1; Warn "$task failed; stopping"; break }
  Ok $task
}
exit $failed

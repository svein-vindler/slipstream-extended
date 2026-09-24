$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

Set-Location -LiteralPath $PSScriptRoot

function Write-Ok([string]$Message) {
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-WarningMessage([string]$Message) {
    Write-Host "[!] $Message" -ForegroundColor Yellow
}

function Require-Command([string]$Name, [string]$InstallHint) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Missing '$Name'. $InstallHint"
    }
}

Write-Host "Slipstream local bootstrap for Windows" -ForegroundColor Cyan
Write-Host "This prepares the repository without creating broad cloud credentials"
Write-Host "or weakening the Cloudflare Access policy."
Write-Host ""

Require-Command "py" "Install Python 3.12 or newer from python.org."
Require-Command "git" "Install Git for Windows from git-scm.com."
Require-Command "node" "Install a current Node.js release from nodejs.org."
Require-Command "npm.cmd" "npm is installed with Node.js."
Require-Command "npx.cmd" "npx is installed with Node.js."
Require-Command "gh" "Install GitHub CLI from cli.github.com."

& py -3 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 'Python 3.12 or newer is required.')"
if ($LASTEXITCODE -ne 0) {
    throw "Python 3.12 or newer is required."
}
Write-Ok "Prerequisites found"

& gh auth status *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Signing in to GitHub..."
    & gh auth login
    if ($LASTEXITCODE -ne 0) {
        throw "GitHub login did not complete successfully."
    }
}
Write-Ok "GitHub CLI authenticated"

if (-not (Test-Path -LiteralPath ".venv")) {
    & py -3 -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        throw "Could not create the Python virtual environment."
    }
}

$Python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    throw "The existing .venv is not a Windows virtual environment. Remove or rename it, then rerun this script."
}

& $Python -m pip install --quiet --upgrade pip
if ($LASTEXITCODE -ne 0) {
    throw "Could not update pip."
}
& $Python -m pip install --quiet -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    throw "Could not install Python dependencies."
}
Write-Ok "Python dependencies installed"

Push-Location worker
try {
    & npm.cmd ci
    if ($LASTEXITCODE -ne 0) {
        throw "Could not install Worker dependencies from package-lock.json."
    }
    Write-Ok "Worker dependencies installed from the lockfile"

    & npx.cmd wrangler whoami *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Signing in to Cloudflare..."
        & npx.cmd wrangler login
        if ($LASTEXITCODE -ne 0) {
            throw "Cloudflare login did not complete successfully."
        }
    }
    Write-Ok "Cloudflare Wrangler authenticated"
}
finally {
    Pop-Location
}

Write-Host ""
Write-Host "Local preparation is complete." -ForegroundColor Green
Write-Host ""
Write-Host "Continue with docs/INSTALL.md, starting at section 2. The remaining"
Write-Host "steps create a private R2 bucket, restricted GitHub secrets, the Garmin"
Write-Host "session, the Worker, and the exact-email Cloudflare Access Managed OAuth"
Write-Host "policy."
Write-Host ""
Write-WarningMessage "Only create the secrets listed in docs/INSTALL.md. Current Slipstream has no secret-path or unauthenticated connector mode."

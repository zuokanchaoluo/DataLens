param([switch]$NoBrowser)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$root = $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
$frontend = Join-Path $root 'data-agent-fronted'
$logs = Join-Path $root '.local\startup'
$appUrl = 'http://127.0.0.1:5173/'
$backendUrl = 'http://127.0.0.1:8000'
$containers = @('mysql', 'qdrant', 'elasticsearch', 'embedding')

function Test-DockerEngine {
    try {
        & docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
        return $LASTEXITCODE -eq 0
    } catch {
        return $false
    }
}

function Test-Http([string]$Url) {
    & curl.exe --noproxy '*' --silent --fail --max-time 2 --output NUL $Url
    return $LASTEXITCODE -eq 0
}

function Test-Frontend {
    if (-not (Test-Http $appUrl)) { return $false }
    # The query route accepts POST, so a 405 proves this GET reached FastAPI.
    $status = & curl.exe --noproxy '*' --silent --max-time 2 --output NUL --write-out '%{http_code}' "${appUrl}api/query"
    return ($LASTEXITCODE -eq 0) -and ($status -eq '405')
}

function Wait-Until {
    param(
        [string]$Label,
        [scriptblock]$Ready,
        [int]$TimeoutSeconds,
        [string]$TimeoutTip,
        [System.Diagnostics.Process]$Process
    )

    Write-Host "等待 $Label ..."
    $timer = [Diagnostics.Stopwatch]::StartNew()
    do {
        if ($Process -and $Process.HasExited) {
            throw "$Label 已退出（退出码 $($Process.ExitCode)）。$TimeoutTip"
        }
        if (& $Ready) { return }
        Start-Sleep -Seconds 1
    } while ($timer.Elapsed.TotalSeconds -lt $TimeoutSeconds)

    throw "$Label 在 $TimeoutSeconds 秒内未就绪。$TimeoutTip"
}

function Assert-PortFree([int]$Port) {
    $occupied = [Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners() |
        Where-Object { $_.Port -eq $Port }
    if ($occupied) {
        throw "端口 $Port 已被占用。若项目已在运行，请直接打开 $appUrl；也可以先关闭占用该端口的程序后重试。"
    }
}

$api = $null
$web = $null
$exitCode = 0
try {
    Assert-PortFree 8000
    Assert-PortFree 5173

    Get-Command docker, curl.exe -CommandType Application -ErrorAction Stop | Out-Null
    $node = (Get-Command node.exe -CommandType Application | Select-Object -First 1).Source
    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
        throw '缺少后端虚拟环境。请在项目根目录运行 uv sync --locked。'
    }
    if (-not (Test-Path -LiteralPath (Join-Path $frontend 'node_modules\vite\bin\vite.js') -PathType Leaf)) {
        throw '缺少前端依赖。请在 data-agent-fronted 目录运行 npm ci。'
    }
    if ([string]::IsNullOrWhiteSpace($env:OPENAI_API_KEY)) {
        $env:OPENAI_API_KEY = [Environment]::GetEnvironmentVariable('OPENAI_API_KEY', 'User')
    }
    if ([string]::IsNullOrWhiteSpace($env:OPENAI_API_KEY)) {
        throw '请先将阿里云百炼 API Key 保存到 Windows 用户环境变量 OPENAI_API_KEY。'
    }

    if (-not (Test-DockerEngine)) {
        Write-Host '正在启动 Docker Desktop...'
        & docker desktop start --detach
        if ($LASTEXITCODE -ne 0) {
            throw 'Docker Desktop 自动启动失败。请打开 Docker Desktop，等引擎启动后重试。'
        }
        Wait-Until 'Docker 引擎' { Test-DockerEngine } 120 '请检查 Docker Desktop 是否已启动。'
    }

    Write-Host '启动 MySQL、Qdrant、Elasticsearch 和 Embedding...'
    & docker start @containers
    if ($LASTEXITCODE -ne 0) {
        throw '容器启动失败。如果容器尚未创建，请在项目根目录运行 docker compose -f docker/docker-compose.yaml up -d mysql qdrant elasticsearch embedding。'
    }

    Wait-Until 'MySQL' {
        & docker exec mysql mysqladmin ping --silent 2>$null | Out-Null
        $LASTEXITCODE -eq 0
    } 90 '请运行 docker logs mysql 检查启动日志。'
    Wait-Until 'Qdrant 知识库' {
        (Test-Http 'http://127.0.0.1:6333/collections/column_info_collection') -and
        (Test-Http 'http://127.0.0.1:6333/collections/metric_info_collection')
    } 60 '请核对 Qdrant 和已有的字段、指标向量集合。'
    Wait-Until 'Elasticsearch 取值索引' {
        Test-Http 'http://127.0.0.1:9200/value_index/_count'
    } 90 '请核对 Elasticsearch 和 value_index。'
    Wait-Until 'Embedding 模型预热' {
        Test-Http 'http://127.0.0.1:8081/health'
    } 240 '请运行 docker logs embedding 检查模型加载日志。'

    New-Item -ItemType Directory -Path $logs -Force | Out-Null
    $env:PYTHONUTF8 = '1'
    Assert-PortFree 8000
    $api = Start-Process -FilePath $python -WorkingDirectory $root -WindowStyle Hidden -PassThru `
        -ArgumentList '-u', '-m', 'uvicorn', 'main:app', '--host', '127.0.0.1', '--port', '8000' `
        -RedirectStandardOutput (Join-Path $logs 'api.log') `
        -RedirectStandardError (Join-Path $logs 'api-error.log')
    Wait-Until 'FastAPI' { Test-Http "$backendUrl/openapi.json" } 60 "请查看 $logs\api-error.log。" $api

    Assert-PortFree 5173
    $web = Start-Process -FilePath $node -WorkingDirectory $frontend -WindowStyle Hidden -PassThru `
        -ArgumentList 'node_modules/vite/bin/vite.js', '--host', '127.0.0.1', '--port', '5173', '--strictPort' `
        -RedirectStandardOutput (Join-Path $logs 'web.log') `
        -RedirectStandardError (Join-Path $logs 'web-error.log')
    Wait-Until 'Vite 及 API 代理' { Test-Frontend } 30 "请查看 $logs\web-error.log。" $web

    Write-Host "数析 DataLens已启动：$appUrl" -ForegroundColor Green
    Write-Host "运行日志：$logs"
    Write-Host '使用时请保留此窗口。'
    if (-not $NoBrowser) {
        try { Start-Process $appUrl } catch { Write-Warning "请在浏览器手动打开 $appUrl" }
    }

    $null = Read-Host '用完后按 Enter 停止前后端'
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    $exitCode = 1
} finally {
    foreach ($process in @($web, $api)) {
        if ($process -and -not $process.HasExited) {
            # The virtual environment launcher can spawn the actual Python process.
            & taskkill.exe /pid $process.Id /t /f | Out-Null
        }
    }
}
exit $exitCode

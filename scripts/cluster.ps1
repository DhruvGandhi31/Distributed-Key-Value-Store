<#
.SYNOPSIS
    Minimal dev-only manager for a local kvstored Raft cluster.

.DESCRIPTION
    Starts/stops/inspects kvstored.exe processes (node1-node3, plus an
    optional node4) on localhost, each with its own Raft port, client port,
    and data directory under ./data. A dev/manual verification convenience
    tool, not production infra.

    node4 is always included in every node's --peers list (static
    membership — this project has no joint-consensus support yet), but
    -Action start only launches node1-node3 by default. Pass -IncludeNode4
    to also launch node4 immediately, or start it later on its own to
    simulate a follower joining after the others have been running for a
    while (Phase 5's lagging-follower / InstallSnapshot checkpoint).

    Requires bin/kvstored.exe to already be built:
        go build -o bin/kvstored.exe ./cmd/server

.USAGE
    .\scripts\cluster.ps1 -Action start [-IncludeNode4]
    .\scripts\cluster.ps1 -Action start-node4
    .\scripts\cluster.ps1 -Action status
    .\scripts\cluster.ps1 -Action stop

.EXAMPLE
    Phase 2 checkpoint: verify leader election and re-election on failure.

        .\scripts\cluster.ps1 -Action start
        .\scripts\cluster.ps1 -Action status
        # note which node is leader, e.g. node2 at term 1

        Stop-Process -Id (Get-Content .\data\node2.pid) -Force
        # wait a few seconds for a new election

        .\scripts\cluster.ps1 -Action status
        # a different node should now be leader at a higher term

        .\scripts\cluster.ps1 -Action stop

.EXAMPLE
    Phase 5 checkpoint: a lagging follower catches up via InstallSnapshot,
    not a full log replay.

        .\scripts\cluster.ps1 -Action start
        # write enough keys to cross --snapshot-interval and force at
        # least one compaction on the leader (see docs/phase-5-*.md for a
        # loop that does this via kvctl)

        .\scripts\cluster.ps1 -Action start-node4
        # watch data\logs\node4.log — it should log receiving a snapshot,
        # not thousands of individual AppendEntries

        .\scripts\cluster.ps1 -Action status
        .\scripts\cluster.ps1 -Action stop
#>

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("start", "start-node4", "stop", "status")]
    [string]$Action,

    [switch]$IncludeNode4,

    [int]$SnapshotInterval = 1000
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$binPath = Join-Path $repoRoot "bin\kvstored.exe"
$dataDir = Join-Path $repoRoot "data"
$logDir = Join-Path $dataDir "logs"

$allNodes = @(
    @{ Id = "node1"; RaftAddr = "127.0.0.1:7001"; ClientAddr = "127.0.0.1:8001" },
    @{ Id = "node2"; RaftAddr = "127.0.0.1:7002"; ClientAddr = "127.0.0.1:8002" },
    @{ Id = "node3"; RaftAddr = "127.0.0.1:7003"; ClientAddr = "127.0.0.1:8003" },
    @{ Id = "node4"; RaftAddr = "127.0.0.1:7004"; ClientAddr = "127.0.0.1:8004" }
)

# node4 is always in the shared --peers string (every node's static
# membership config includes it from the start), even when its process
# isn't launched yet — that's what lets it join later and be replicated to
# immediately, without restarting node1-node3.
$peers = ($allNodes | ForEach-Object { "$($_.Id)=$($_.RaftAddr)" }) -join ","

function Get-PidFile($id) { Join-Path $dataDir "$id.pid" }
function Get-LogFile($id) { Join-Path $logDir "$id.log" }

function Start-Nodes($nodesToStart) {
    if (-not (Test-Path $binPath)) {
        Write-Error "bin\kvstored.exe not found. Build it first: go build -o bin/kvstored.exe ./cmd/server"
        return
    }

    New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null

    foreach ($node in $nodesToStart) {
        $id = $node.Id
        $pidFile = Get-PidFile $id
        $logFile = Get-LogFile $id

        if (Test-Path $pidFile) {
            $existingPid = Get-Content $pidFile
            if (Get-Process -Id $existingPid -ErrorAction SilentlyContinue) {
                Write-Host "$id already running (PID $existingPid), skipping"
                continue
            }
            Remove-Item $pidFile -Force
        }

        $args = @(
            "--id", $id,
            "--raft-addr", $node.RaftAddr,
            "--client-addr", $node.ClientAddr,
            "--peers", $peers,
            "--data-dir", $dataDir,
            "--snapshot-interval", $SnapshotInterval
        )

        # kvstored logs everything (both the stdlib logger and the raft
        # logger) to stderr, so $logFile captures stderr and stdout goes to
        # a throwaway file.
        $proc = Start-Process -FilePath $binPath -ArgumentList $args `
            -RedirectStandardOutput "$logFile.out" -RedirectStandardError $logFile `
            -PassThru -WindowStyle Hidden

        Set-Content -Path $pidFile -Value $proc.Id
        Write-Host "started $id (PID $($proc.Id)) raft=$($node.RaftAddr) client=$($node.ClientAddr)"
    }
}

function Stop-Cluster {
    foreach ($node in $allNodes) {
        $id = $node.Id
        $pidFile = Get-PidFile $id

        if (-not (Test-Path $pidFile)) {
            Write-Host "$id has no PID file, skipping"
            continue
        }

        $nodePid = Get-Content $pidFile
        Stop-Process -Id $nodePid -Force -ErrorAction SilentlyContinue
        Remove-Item $pidFile -Force
        Write-Host "stopped $id (PID $nodePid)"
    }
}

function Show-Status {
    $rows = foreach ($node in $allNodes) {
        $id = $node.Id
        $pidFile = Get-PidFile $id
        $logFile = Get-LogFile $id

        $running = $false
        $nodePid = $null
        if (Test-Path $pidFile) {
            $nodePid = Get-Content $pidFile
            $running = [bool](Get-Process -Id $nodePid -ErrorAction SilentlyContinue)
        }

        $leaderInfo = "-"
        if (Test-Path $logFile) {
            $match = Select-String -Path $logFile -Pattern "became Leader term=(\d+)" | Select-Object -Last 1
            if ($match) {
                $term = $match.Matches[0].Groups[1].Value
                $leaderInfo = "leader at term $term"
            }
        }

        [PSCustomObject]@{
            Node    = $id
            PID     = if ($nodePid) { $nodePid } else { "-" }
            Running = $running
            Status  = $leaderInfo
        }
    }

    $rows | Format-Table -AutoSize
}

switch ($Action) {
    "start" {
        $toStart = if ($IncludeNode4) { $allNodes } else { $allNodes[0..2] }
        Start-Nodes $toStart
    }
    "start-node4" { Start-Nodes @($allNodes[3]) }
    "stop" { Stop-Cluster }
    "status" { Show-Status }
}

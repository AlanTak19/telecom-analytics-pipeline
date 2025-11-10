<#
.SYNOPSIS
    Complete shutdown script for Telecom Analytics Pipeline
.DESCRIPTION
    Stops all Python processes, Spark jobs, Docker containers, and optionally cleans data
.EXAMPLE
    .\scripts\shutdown_all.ps1
#>

# Set error action preference
$ErrorActionPreference = "SilentlyContinue"

# Clear screen
Clear-Host

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════════════╗" -ForegroundColor Red
Write-Host "║  🛑 TELECOM ANALYTICS PIPELINE - COMPLETE SHUTDOWN                ║" -ForegroundColor Red
Write-Host "╚════════════════════════════════════════════════════════════════════╝" -ForegroundColor Red
Write-Host ""

# ============================================
# Step 1: Stop Python Processes
# ============================================
Write-Host "┌─────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ Step 1/7: Stopping Python processes...                         │" -ForegroundColor Yellow
Write-Host "└─────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow

$pythonProcesses = Get-Process python* -ErrorAction SilentlyContinue
if ($pythonProcesses) {
    $pythonProcesses | ForEach-Object {
        Write-Host "   Stopping: $($_.ProcessName) (PID: $($_.Id))" -ForegroundColor Gray
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
    }
    Write-Host "   ✅ Python processes stopped ($($pythonProcesses.Count) processes)" -ForegroundColor Green
} else {
    Write-Host "   ℹ️  No Python processes running" -ForegroundColor Cyan
}

Start-Sleep -Seconds 2

# ============================================
# Step 2: Stop Spark Streaming Jobs
# ============================================
Write-Host ""
Write-Host "┌─────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ Step 2/7: Stopping Spark Streaming jobs...                     │" -ForegroundColor Yellow
Write-Host "└─────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow

# Check if spark-master container exists
$sparkMaster = docker ps -aq --filter "name=spark-master"
if ($sparkMaster) {
    Write-Host "   Killing Spark submit processes..." -ForegroundColor Gray
    docker exec spark-master pkill -9 -f spark-submit 2>$null
    docker exec spark-master pkill -9 -f SparkSubmit 2>$null
    docker exec spark-master pkill -9 -f java 2>$null
    Write-Host "   ✅ Spark jobs stopped" -ForegroundColor Green
} else {
    Write-Host "   ℹ️  Spark container not running" -ForegroundColor Cyan
}

Start-Sleep -Seconds 3

# ============================================
# Step 3: Stop Docker Compose Services
# ============================================
Write-Host ""
Write-Host "┌─────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ Step 3/7: Stopping Docker Compose services...                  │" -ForegroundColor Yellow
Write-Host "└─────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow

Write-Host "   Running docker-compose down..." -ForegroundColor Gray
docker-compose down 2>&1 | Out-Null
Write-Host "   ✅ Docker Compose services stopped" -ForegroundColor Green

Start-Sleep -Seconds 3

# ============================================
# Step 4: Force Stop Individual Containers
# ============================================
Write-Host ""
Write-Host "┌─────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ Step 4/7: Force stopping remaining containers...               │" -ForegroundColor Yellow
Write-Host "└─────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow

$containers = @(
    "spark-master",
    "spark-worker",
    "postgres",
    "kafka",
    "zookeeper",
    "airflow-webserver",
    "airflow-scheduler",
    "airflow-init"
)

$stoppedCount = 0
foreach ($container in $containers) {
    $exists = docker ps -aq --filter "name=$container"
    if ($exists) {
        Write-Host "   Stopping: $container" -ForegroundColor Gray
        docker stop $container 2>&1 | Out-Null
        docker rm -f $container 2>&1 | Out-Null
        $stoppedCount++
    }
}

if ($stoppedCount -gt 0) {
    Write-Host "   ✅ Stopped $stoppedCount containers" -ForegroundColor Green
} else {
    Write-Host "   ℹ️  No additional containers to stop" -ForegroundColor Cyan
}

Start-Sleep -Seconds 2

# ============================================
# Step 5: Clean Docker Networks
# ============================================
Write-Host ""
Write-Host "┌─────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ Step 5/7: Cleaning Docker networks...                          │" -ForegroundColor Yellow
Write-Host "└─────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow

$network = docker network ls --filter "name=telecom" --format "{{.Name}}"
if ($network) {
    Write-Host "   Removing network: $network" -ForegroundColor Gray
    docker network rm $network 2>&1 | Out-Null
    Write-Host "   ✅ Network removed" -ForegroundColor Green
} else {
    Write-Host "   ℹ️  No networks to clean" -ForegroundColor Cyan
}

# ============================================
# Step 6: Optional - Clean Data Volumes
# ============================================
Write-Host ""
Write-Host "┌─────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ Step 6/7: Data volume cleanup (OPTIONAL)                       │" -ForegroundColor Yellow
Write-Host "└─────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""
Write-Host "   ⚠️  WARNING: This will DELETE ALL DATA!" -ForegroundColor Red
Write-Host "   This includes:" -ForegroundColor Yellow
Write-Host "      - PostgreSQL data" -ForegroundColor Yellow
Write-Host "      - Kafka logs" -ForegroundColor Yellow
Write-Host "      - Spark checkpoints" -ForegroundColor Yellow
Write-Host "      - Airflow logs" -ForegroundColor Yellow
Write-Host ""
$cleanVolumes = Read-Host "   Delete data volumes? (yes/NO)"

if ($cleanVolumes -eq "yes") {
    Write-Host ""
    Write-Host "   🗑️  Removing Docker volumes..." -ForegroundColor Red
    docker-compose down -v 2>&1 | Out-Null
    
    Write-Host "   🗑️  Cleaning data directories..." -ForegroundColor Red
    if (Test-Path "data") {
        Remove-Item -Path "data\*" -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "   ✅ Data directories cleaned" -ForegroundColor Green
    }
    
    Write-Host "   ✅ Volumes cleaned" -ForegroundColor Green
} else {
    Write-Host "   ⏭️  Skipped volume cleanup (data preserved)" -ForegroundColor Cyan
}

# ============================================
# Step 7: Clean Spark Checkpoints ONLY
# ============================================
Write-Host ""
Write-Host "┌─────────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "│ Step 7/7: Cleaning Spark checkpoints...                        │" -ForegroundColor Yellow
Write-Host "└─────────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow

if (Test-Path "data\spark") {
    Remove-Item -Path "data\spark\*" -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "   ✅ Spark checkpoints cleaned" -ForegroundColor Green
} else {
    Write-Host "   ℹ️  No checkpoints to clean" -ForegroundColor Cyan
}

# ============================================
# Verification
# ============================================
Write-Host ""
Write-Host "┌─────────────────────────────────────────────────────────────────┐" -ForegroundColor Cyan
Write-Host "│ Verification: Checking remaining processes...                  │" -ForegroundColor Cyan
Write-Host "└─────────────────────────────────────────────────────────────────┘" -ForegroundColor Cyan

# Check Docker containers
$runningContainers = docker ps --format "{{.Names}}"
if ($runningContainers) {
    Write-Host ""
    Write-Host "   ⚠️  Still running containers:" -ForegroundColor Yellow
    $runningContainers | ForEach-Object { Write-Host "      - $_" -ForegroundColor Yellow }
} else {
    Write-Host "   ✅ No Docker containers running" -ForegroundColor Green
}

# Check Python processes
$pythonStillRunning = Get-Process python* -ErrorAction SilentlyContinue
if ($pythonStillRunning) {
    Write-Host "   ⚠️  Python processes still running:" -ForegroundColor Yellow
    $pythonStillRunning | ForEach-Object { 
        Write-Host "      - $($_.ProcessName) (PID: $($_.Id))" -ForegroundColor Yellow 
    }
} else {
    Write-Host "   ✅ No Python processes running" -ForegroundColor Green
}

# ============================================
# Summary
# ============================================
Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║                    ✅ SHUTDOWN COMPLETE                            ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "📋 Summary:" -ForegroundColor Cyan
Write-Host "   • Python processes: Stopped" -ForegroundColor White
Write-Host "   • Spark jobs: Stopped" -ForegroundColor White
Write-Host "   • Docker containers: Stopped & removed" -ForegroundColor White
Write-Host "   • Spark checkpoints: Cleaned" -ForegroundColor White
if ($cleanVolumes -eq "yes") {
    Write-Host "   • Data volumes: Cleaned" -ForegroundColor White
} else {
    Write-Host "   • Data volumes: Preserved" -ForegroundColor White
}
Write-Host ""
Write-Host "🚀 To restart the system:" -ForegroundColor Cyan
Write-Host "   .\scripts\fresh_start.ps1" -ForegroundColor White
Write-Host ""
Write-Host "   OR manually:" -ForegroundColor Cyan
Write-Host "   docker-compose up -d" -ForegroundColor White
Write-Host ""
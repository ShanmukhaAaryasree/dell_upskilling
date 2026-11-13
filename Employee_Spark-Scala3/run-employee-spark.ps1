param(
    [switch]$LocalSbt
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$jarName = "employee-spark-assembly-0.1.0.jar"

Push-Location $projectRoot
try {
    $sparkSubmit = Get-Command "spark-submit" -ErrorAction SilentlyContinue
    $hadoopReady = $false
    if ($env:HADOOP_HOME) {
        $winutils = Join-Path $env:HADOOP_HOME "bin\winutils.exe"
        $hadoopReady = Test-Path $winutils -PathType Leaf
    }

    if ($LocalSbt -or -not $sparkSubmit -or -not $hadoopReady) {
        $sbt = Get-Command "sbt" -ErrorAction SilentlyContinue
        if (-not $sbt) {
            throw "A local run requires sbt; a spark-submit run requires spark-submit, HADOOP_HOME, and winutils.exe."
        }

        Write-Host "Running Employee Analysis with the local Spark runtime from sbt..."
        $previousErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        & $sbt.Source "Compile / runMain com.training.EmployeeAnalysis" 2>&1
        $runExitCode = $LASTEXITCODE
        $ErrorActionPreference = $previousErrorActionPreference
        if ($runExitCode -ne 0) {
            throw "Local Spark run failed with exit code $runExitCode."
        }
        return
    }

    $jar = Get-ChildItem -Path (Join-Path $projectRoot "target") -Filter $jarName -File -Recurse |
        Select-Object -First 1
    if (-not $jar) {
        throw "Assembly JAR '$jarName' was not found under target. Run 'sbt clean test assembly' from the project root first."
    }

    $env:PATH = "$(Join-Path $env:HADOOP_HOME 'bin');$env:PATH"

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $sparkSubmit.Source --class com.training.EmployeeAnalysis --master "local[*]" $jar.FullName 2>&1
    $runExitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference
    if ($runExitCode -ne 0) {
        throw "spark-submit failed with exit code $runExitCode."
    }
} finally {
    Pop-Location
}

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$version = "3.5.0"
$expectedSha256 = "8B1BBFD09568AA3A52449671486A131E06162433444F84F38C3DED221484A741"
$targetRoot = Join-Path $projectRoot "target"
$archivePath = Join-Path $targetRoot "hadoop-$version.zip"
$extractPath = Join-Path $targetRoot "winutils-extract"
$hadoopHome = Join-Path $targetRoot "hadoop-$version"
$winutilsPath = Join-Path $hadoopHome "bin\winutils.exe"
$downloadUrl = "https://github.com/zehelh/winutils/releases/download/hadoop-$version/hadoop-$version.zip"

New-Item -ItemType Directory -Path $targetRoot -Force | Out-Null

if (-not (Test-Path -LiteralPath $winutilsPath -PathType Leaf)) {
    Invoke-WebRequest -Uri $downloadUrl -OutFile $archivePath
    $actualSha256 = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash
    if ($actualSha256 -ne $expectedSha256) {
        throw "Hadoop $version winutils archive checksum mismatch."
    }

    Expand-Archive -LiteralPath $archivePath -DestinationPath $extractPath -Force
    $sourceWinutils = Join-Path $extractPath "hadoop-$version\bin\winutils.exe"
    if (-not (Test-Path -LiteralPath $sourceWinutils -PathType Leaf)) {
        throw "winutils.exe was not found in the verified Hadoop $version archive."
    }

    $destinationBin = Split-Path -Parent $winutilsPath
    New-Item -ItemType Directory -Path $destinationBin -Force | Out-Null
    Copy-Item -LiteralPath $sourceWinutils -Destination $winutilsPath -Force
}

Write-Host "Installed Hadoop $version winutils at $winutilsPath"

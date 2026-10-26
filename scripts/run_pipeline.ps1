# End-to-end pipeline for PowerShell (pwsh on Windows, macOS or Linux).
# Usage: ./scripts/run_pipeline.ps1            (HDFS, default)
#        ./scripts/run_pipeline.ps1 -Fs local
param(
    [ValidateSet("hdfs", "local")] [string] $Fs = "hdfs",
    [string] $LoadDate = (Get-Date -Format "yyyy-MM-dd")
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$env:PIPELINE_FS = $Fs
$env:LOAD_DATE = $LoadDate
$env:HADOOP_CONF_DIR = Join-Path $Root "conf/hadoop"
if (-not $env:JAVA_HOME) { $env:JAVA_HOME = "/opt/homebrew/opt/openjdk@17" }
$Python = if ($IsWindows) { Join-Path $Root ".venv/Scripts/python.exe" } else { Join-Path $Root ".venv/bin/python" }
$env:PYSPARK_PYTHON = $Python
if (-not $env:SPARK_HOME) { $env:SPARK_HOME = & $Python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))" }
$SparkSubmit = Join-Path $env:SPARK_HOME "bin/spark-submit"
$Src = Join-Path $Root "src"

function Step($Name) { Write-Host "`n== $Name ==" -ForegroundColor Cyan }
function Invoke-Checked([scriptblock] $Block) {
    & $Block
    if ($LASTEXITCODE -ne 0) { throw "Step failed with exit code $LASTEXITCODE" }
}
function Submit($Script) {
    Invoke-Checked { & $SparkSubmit --conf spark.ui.enabled=false --driver-java-options "-Dlog4j.configurationFile=$Root/conf/spark/log4j2.properties" --py-files "$Src/common.py,$Src/mapping.py" (Join-Path $Src $Script) 2>$null }
}

Set-Location $Root
Step "1. Download CMS files (if missing)"
if (-not (Get-ChildItem "data/raw/*.csv" -ErrorAction SilentlyContinue)) { Invoke-Checked { & $Python src/download.py } }

if ($Fs -eq "hdfs") {
    Step "2. Land raw files in HDFS (load_date=$LoadDate)"
    foreach ($f in Get-ChildItem "data/raw/*.csv") {
        $dest = "/data/cms/raw/$($f.BaseName)/load_date=$LoadDate"
        Invoke-Checked { hdfs dfs -mkdir -p $dest 2>$null }
        Invoke-Checked { hdfs dfs -put -f $f.FullName "$dest/$($f.Name)" 2>$null }
        $hdfsBytes = [long](hdfs dfs -stat "%b" "$dest/$($f.Name)" 2>$null)
        if ($hdfsBytes -ne $f.Length) { throw "Size mismatch for $($f.Name): local $($f.Length), HDFS $hdfsBytes" }
        "{0,-45} {1,12} bytes -> {2}" -f $f.Name, $hdfsBytes, $dest
    }
}

Step "3. Bronze: raw files -> Hive tables (cms_raw)"
Submit "ingest.py"
Step "4. Standard: map sources to the standard format (cms_std)"
Submit "transform.py"
Step "5. Data quality checks (critical failures stop the run)"
Submit "validate.py"
Step "6. Marts: HiveQL via spark-sql (cms_mart)"
$Warehouse = if ($Fs -eq "hdfs") { "hdfs://localhost:9000/user/hive/warehouse" } else { Join-Path $Root "spark-warehouse" }
$DefaultFs = if ($Fs -eq "hdfs") { "hdfs://localhost:9000" } else { "file:///" }
$Metastore = Join-Path $Root ".metastore/metastore_db"
Invoke-Checked {
    & (Join-Path $env:SPARK_HOME "bin/spark-sql") --silent `
        --conf spark.ui.enabled=false `
        --conf "spark.sql.warehouse.dir=$Warehouse" `
        --conf "spark.hadoop.fs.defaultFS=$DefaultFs" `
        --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$Metastore;create=true" `
        -f (Join-Path $Root "sql/marts.hql") *> $null
}
Step "7. Export report tables"
Submit "report.py"

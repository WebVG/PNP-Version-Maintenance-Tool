<#
.SYNOPSIS
  Safe SharePoint Online version trim tool using PnP.PowerShell only.

.DESCRIPTION
  - Uses ONLY PnP.PowerShell (no SPO Management Shell needed).
  - Shows current PnP site version policy and offers to update it.
  - Enumerates document libraries (all, specific, or from CSV).
  - Trims file versions older than N days.
  - ALWAYS runs as DryRun on the first execution (per script/state file).
  - Processes work in ~25% batches OR max N minutes per batch.
  - Respects retention, eDiscovery, records (SharePoint blocks invalid deletes).
  - Logs warnings/errors to CSV and/or a simple text log.

.REQUIREMENTS
  - PnP.PowerShell
  - You must already be connected, e.g.:
      Connect-PnPOnline -Url "https://tenant.sharepoint.com/sites/site" -Interactive

.NOTES
  After the first dry run, you can run again with -Delete.
#>

# -------------------------------------------------------------------------------------
# Text event log helper (used by the main function)
# -------------------------------------------------------------------------------------
function Write-TrimEvent {
    param(
        [string]$Level,     # Info / Warn / Error
        [string]$Message
    )

    $ts = (Get-Date).ToString("o")
    $line = "[$ts] [$Level] $Message"

    if ($script:TextLogPath) {
        $dir = Split-Path $script:TextLogPath -Parent
        if ($dir -and -not (Test-Path $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }
        Add-Content -Path $script:TextLogPath -Value $line
    }

    switch ($Level) {
        'Error' { Write-Host $line -ForegroundColor Red }
        'Warn'  { Write-Host $line -ForegroundColor Yellow }
        default { } # Info stays in file only
    }
}

# -------------------------------------------------------------------------------------
# Show + optionally update PnP site version policy
# -------------------------------------------------------------------------------------
function Show-PnPSiteVersionPolicy {
    [CmdletBinding()]
    param()

    try {
        $policy = Get-PnPSiteVersionPolicy
    } catch {
        Write-Warning "Failed to get site version policy: $($_.Exception.Message)"
        return
    }

    if (-not $policy) {
        Write-Host "No site version policy returned." -ForegroundColor Yellow
        return
    }

    Write-Host "========== Current Site Version Policy (Raw) ==========" -ForegroundColor Cyan
    $policy | Format-List *

    # Friendly summary
    $autoStatus = if ($policy.EnableAutoExpirationVersionTrim) { "ENABLED" } else { "DISABLED" }
    $color      = if ($policy.EnableAutoExpirationVersionTrim) { 'Green' } else { 'Yellow' }

    Write-Host ""
    Write-Host "----------------- Policy Status Summary -----------------" -ForegroundColor Cyan
    Write-Host ("Auto-expiration : {0}"     -f $autoStatus)             -ForegroundColor $color
    Write-Host (" Major versions  : {0}"     -f $policy.MajorVersions)
    Write-Host (" Expire after    : {0} days" -f $policy.ExpireVersionsAfterDays)
    Write-Host "---------------------------------------------------------" -ForegroundColor Cyan

    Write-Host ""
    Write-Host "Review this carefully before trimming versions." -ForegroundColor Yellow

    $answer = Read-Host "Change/update policy now? (y/N)"
    if ($answer -eq 'y') {
        $enable = Read-Host "Enable auto expiration version trim? (true/false) [current: $($policy.EnableAutoExpirationVersionTrim)]"
        if ([string]::IsNullOrWhiteSpace($enable)) { $enable = $policy.EnableAutoExpirationVersionTrim }

        $major = Read-Host "Max major versions? [current: $($policy.MajorVersions)]"
        if (-not [int]::TryParse($major, [ref]0)) { $major = $policy.MajorVersions }

        $days  = Read-Host "Expire versions after how many days? [current: $($policy.ExpireVersionsAfterDays)]"
        if (-not [int]::TryParse($days, [ref]0)) { $days = $policy.ExpireVersionsAfterDays }

        try {
            Set-PnPSiteVersionPolicy `
                -EnableAutoExpirationVersionTrim ([bool]$enable) `
                -MajorVersions ([int]$major) `
                -ExpireVersionsAfterDays ([int]$days) `
                -ApplyToExistingDocumentLibraries `
                -ApplyToNewDocumentLibraries

            Write-Host "Policy updated." -ForegroundColor Green

            # For cooldown after policy change
            $global:PnPVersionTrim_LastPolicyUpdateUtc = (Get-Date).ToUniversalTime()
        } catch {
            Write-Warning "Failed to update policy: $($_.Exception.Message)"
        }
    }
}

# -------------------------------------------------------------------------------------
# Cheap size calculator (sum File_x0020_Size for files in a list)
# -------------------------------------------------------------------------------------
function Get-PnPListSizeBytes {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ListTitle
    )

    $items = Get-PnPListItem -List $ListTitle -PageSize 2000 -Fields "FileLeafRef","File_x0020_Size","FSObjType"

    $total = 0L
    foreach ($item in $items) {
        # FSObjType: 0 = File, 1 = Folder
        if ($item["FSObjType"] -eq 0 -and $item["File_x0020_Size"]) {
            $total += [int64]$item["File_x0020_Size"]
        }
    }

    return $total
}

# -------------------------------------------------------------------------------------
# Size log for before/after measurements
# -------------------------------------------------------------------------------------
function Write-PnPSizeLog {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$LogPath,
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][string]$SiteUrl,
        [Parameter(Mandatory)][string]$LibraryTitle,
        [Parameter(Mandatory)][ValidateSet('Before','After')][string]$Phase,
        [Parameter(Mandatory)][long]$SizeBytes
    )

    $dir = Split-Path $LogPath -Parent
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }

    if (-not (Test-Path $LogPath)) {
        "Timestamp,RunId,SiteUrl,LibraryTitle,Phase,SizeBytes,SizeMB" |
            Out-File -FilePath $LogPath -Encoding UTF8
    }

    $mb   = [math]::Round($SizeBytes / 1MB, 2)
    $line = "{0},{1},{2},{3},{4},{5},{6}" -f (Get-Date).ToString("o"),
        $RunId, $SiteUrl, $LibraryTitle, $Phase, $SizeBytes, $mb

    Add-Content -Path $LogPath -Value $line
}

# -------------------------------------------------------------------------------------
# Main function: Invoke-PnPVersionTrimTool
# -------------------------------------------------------------------------------------
function Invoke-PnPVersionTrimTool {
    [CmdletBinding()]
    param(
        [int]$OlderThanDays   = 45,
        [string]$LibraryTitle,
        [string]$LibraryCsvPath,
        [switch]$Delete,
        [string]$LogPath,
        [string]$TextLogPath,
        [int]$BatchPercent    = 25,
        [int]$MaxBatchMinutes = 5,
        [switch]$AutoContinue,
        [switch]$BypassBatching,

        # Per-file version batching controls
        [int]$VersionBatchSize   = 50,
        [int]$VersionBatchPauseMs = 250,
        [int]$MaxRetries         = 5
    )

    # Make TextLogPath visible to Write-TrimEvent
    $script:TextLogPath = $TextLogPath

    #
    # --- SAFETY: FIRST RUN MUST ALWAYS BE DRY RUN ---
    #
    $stateRoot = Join-Path $env:LOCALAPPDATA "PnPVersionTrim"
    if (-not (Test-Path $stateRoot)) {
        New-Item -ItemType Directory -Force -Path $stateRoot | Out-Null
    }
    $stateFile        = Join-Path $stateRoot "state.json"
    $firstRun         = -not (Test-Path $stateFile)
    $effectiveDryRun  = $true

    if ($firstRun) {
        Write-Host "First run detected. THIS WILL BE A DRY RUN ONLY." -ForegroundColor Yellow
    } elseif ($Delete) {
        $effectiveDryRun = $false
    }

    if ($effectiveDryRun) {
        Write-Host "DRY RUN: No versions will be deleted." -ForegroundColor Cyan
    } else {
        Write-Host "DELETE MODE: Actual deletions will occur." -ForegroundColor Red
        $confirm = Read-Host "Type DELETE to proceed"
        if ($confirm -ne 'DELETE') {
            Write-Host "Cancelled." -ForegroundColor Yellow
            return
        }
    }

    #
    # --- OPTIONAL CSV LOGGING ---
    #
    if ($LogPath) {
        if (-not (Test-Path (Split-Path $LogPath -Parent))) {
            New-Item -ItemType Directory -Force -Path (Split-Path $LogPath -Parent) | Out-Null
        }
        if (-not (Test-Path $LogPath)) {
            "Timestamp,Action,LibraryTitle,FileRef,VersionId,VersionLabel,VersionCreated,Result,Message" |
                Out-File -FilePath $LogPath -Encoding UTF8
        }
        function Write-TrimLog {
            param($Action,$Library,$FileRef,$VersionId,$Label,$Created,$Result,$Message)
            $line = "{0},{1},{2},{3},{4},{5},{6},{7},{8}" -f `
                (Get-Date).ToString("o"),$Action,$Library,$FileRef,$VersionId,$Label,$Created.ToString("o"),$Result,$Message
            Add-Content -Path $LogPath -Value $line
        }
    } else {
        function Write-TrimLog {}
    }

    #
    # --- SHOW POLICY + COOLDOWN ---
    #
    Show-PnPSiteVersionPolicy

    if ($global:PnPVersionTrim_LastPolicyUpdateUtc) {
        $minutesSinceChange = (New-TimeSpan -Start $global:PnPVersionTrim_LastPolicyUpdateUtc -End (Get-Date).ToUniversalTime()).TotalMinutes
        if ($minutesSinceChange -lt 30) {
            Write-Host "Policy was just changed ($([math]::Round($minutesSinceChange,1)) minutes ago)." -ForegroundColor Yellow
            Write-Host "Skipping trim to avoid running while policy changes may be pending." -ForegroundColor Yellow
            return
        }
    }

    #
    # --- DISCOVER TARGET LIBRARIES ---
    #
    $ctx    = Get-PnPContext
    $cutoff = (Get-Date).AddDays(-1 * $OlderThanDays)

    $ctx.RequestTimeout = [Math]::Max(($MaxBatchMinutes * 60000), 600000)  # at least 10 min

    $libFilter = @()
    if ($LibraryCsvPath) {
        if (-not (Test-Path $LibraryCsvPath)) { Write-Warning "CSV not found."; return }
        $csv = Import-Csv $LibraryCsvPath
        foreach ($row in $csv) {
            if ($row.LibraryTitle) { $libFilter += $row.LibraryTitle }
            elseif ($row.Title)   { $libFilter += $row.Title }
        }
        $libFilter = $libFilter | Select-Object -Unique
    }

    if ($LibraryTitle) {
        $lists = Get-PnPList -Identity $LibraryTitle
    } else {
        $lists = Get-PnPList | Where-Object { $_.BaseTemplate -eq 101 -and -not $_.Hidden }
        if ($libFilter.Count -gt 0) {
            $lists = $lists | Where-Object { $libFilter -contains $_.Title }
        }
    }

    if (-not $lists -or $lists.Count -eq 0) {
        Write-Warning "No document libraries found."
        return
    }

    Write-Host ""
    Write-Host ("Target libraries: {0}" -f (($lists | Select-Object -ExpandProperty Title) -join ', ')) -ForegroundColor Green

    #
    # --- SIZE BEFORE ---
    #
    $runId          = [Guid]::NewGuid().ToString()
    $siteUrl        = (Get-PnPContext).Url
    $startSizeBytes = 0L

    foreach ($list in $lists) {
        $size = Get-PnPListSizeBytes -ListTitle $list.Title
        $startSizeBytes += $size
        if ($LogPath) {
            Write-PnPSizeLog -LogPath $LogPath -RunId $runId `
                -SiteUrl $siteUrl -LibraryTitle $list.Title -Phase 'Before' -SizeBytes $size
        }
    }

    Write-Host ("Starting total size: {0:N2} MB" -f ($startSizeBytes / 1MB)) -ForegroundColor Cyan

    #
    # --- BUILD WORK ITEM LIST (ALL FILES IN TARGET LIBRARIES) ---
    #
    $workItems = New-Object System.Collections.Generic.List[object]

    foreach ($list in $lists) {
        Write-Host "Discovering files in: $($list.Title)" -ForegroundColor Yellow
        $listTitle = $list.Title

        Get-PnPListItem -List $list -PageSize 2000 -Fields "FileRef","FileLeafRef" -ScriptBlock {
            param($items)
            foreach ($i in $items) {
                if ($i.FileSystemObjectType -ne "File") { continue }
                $workItems.Add([pscustomobject]@{
                    LibraryTitle = $listTitle
                    ItemId       = $i.Id
                    FileRef      = $i["FileRef"]
                    FileLeaf     = $i["FileLeafRef"]
                })
            }
        }
    }

    $total = $workItems.Count
    if ($total -eq 0) {
        Write-Host "No files discovered." -ForegroundColor Yellow
        return
    }

    Write-Host ("Total files discovered: {0}" -f $total) -ForegroundColor Green

    #
    # --- HELPERS: retry for ExecuteQuery ---
    #
    function Invoke-WithRetry([scriptblock]$Action, [int]$Attempts = $MaxRetries) {
        $try = 1
        while ($true) {
            try { & $Action; break }
            catch {
                if ($try -ge $Attempts) { throw }
                $delay = [math]::Pow(2, $try)  # 2,4,8,16...
                Write-Warning "ExecuteQuery failed (attempt $try): $($_.Exception.Message). Retrying in $delay sec..."
                Start-Sleep -Seconds $delay
                $try++
            }
        }
    }

    #
    # --- BATCH PROCESSING LOGIC (across files) ---
    #
    $processedCount       = 0
    $filesWithOldVersions = 0
    $failedDeletes        = 0
    $skippedByError       = 0

    if ($BypassBatching) {
        Write-Host "Batching disabled. Processing all items..." -ForegroundColor Yellow
        $batchSize       = $total
        $MaxBatchMinutes = [int]::MaxValue
    } else {
        if ($BatchPercent -le 0 -or $BatchPercent -gt 100) { $BatchPercent = 25 }
        $batchSize = [math]::Ceiling($total * ($BatchPercent / 100))
        Write-Host "Batch = ~${BatchPercent}% of files or ${MaxBatchMinutes} minutes, whichever comes first." -ForegroundColor Cyan
    }

    $index       = 0
    $batchNumber = 0

    while ($index -lt $total) {
        $batchNumber++
        $batchStart = $index
        $batchEnd   = [math]::Min($batchStart + $batchSize - 1, $total - 1)
        $timer      = [System.Diagnostics.Stopwatch]::StartNew()

        Write-Host ""
        Write-Host "===== Batch $batchNumber ($($batchStart+1) to $($batchEnd+1)) =====" -ForegroundColor Magenta

        while ($index -le $batchEnd -and $index -lt $total) {

            if (-not $BypassBatching -and $timer.Elapsed.TotalMinutes -ge $MaxBatchMinutes) {
                Write-Host "Batch time exceeded; moving to next batch." -ForegroundColor Yellow
                break
            }

            $item = $workItems[$index]
            $index++
            $processedCount++

            # Progress bar
            $percent = if ($total -gt 0) {
                [math]::Floor(($processedCount / [double]$total) * 100)
            } else {
                0
            }

            Write-Progress -Activity "Trimming file versions" `
                           -Status "Processed $processedCount of $total files; $filesWithOldVersions had old versions" `
                           -PercentComplete $percent

            #
            # Load file + versions by list + item id (avoids URL encoding weirdness)
            #
            $list     = $ctx.Web.Lists.GetByTitle($item.LibraryTitle)
            $listItem = $list.GetItemById($item.ItemId)
            $ctx.Load($listItem)
            $ctx.Load($listItem.File)
            $ctx.Load($listItem.File.Versions)

            try {
                $ctx.ExecuteQuery()
            } catch {
                $msg = $_.Exception.Message
                Write-TrimEvent -Level 'Error' -Message ("Failed to load {0}/ID={1}: {2}" -f $item.LibraryTitle, $item.ItemId, $msg)
                $skippedByError++
                continue
            }

            $file     = $listItem.File
            $versions = $file.Versions

            #
            # Filter versions older than cutoff (historical only)
            #
            $old = @()
            foreach ($v in $versions) {
                if (-not $v.IsCurrentVersion -and $v.Created -lt $cutoff) { $old += $v }
            }
            if ($old.Count -eq 0) { continue }

            $filesWithOldVersions++

            #
            # DRY RUN: log what would be deleted
            #
            if ($effectiveDryRun) {
                foreach ($v in $old) {
                    Write-TrimLog "DryRun" $item.LibraryTitle $item.FileRef $v.ID $v.VersionLabel $v.Created "Planned" "DryRun - would delete"
                }
                continue
            }

            #
            # DELETE MODE: chunk deletions per file to avoid huge CSOM calls
            #
            $chunkSize = if ($VersionBatchSize -gt 0) { $VersionBatchSize } else { 50 }

            for ($i = 0; $i -lt $old.Count; $i += $chunkSize) {
                $chunk = $old[$i..([math]::Min($i + $chunkSize - 1, $old.Count - 1))]

                foreach ($v in $chunk) {
                    $v.DeleteObject()
                }

                try {
                    Invoke-WithRetry { $ctx.ExecuteQuery() }
                    if ($VersionBatchPauseMs -gt 0) {
                        Start-Sleep -Milliseconds $VersionBatchPauseMs
                    }

                    foreach ($v in $chunk) {
                        Write-TrimLog "Delete" $item.LibraryTitle $item.FileRef $v.ID $v.VersionLabel $v.Created "Deleted" "Deleted"
                    }
                } catch {
                    $failedDeletes++
                    $msg = $_.Exception.Message

                    if ($msg -match 'retention|hold|record') {
                        $skippedByError++
                        Write-TrimEvent -Level 'Warn' -Message ("Skipped {0} due to retention/hold: {1}" -f $item.FileRef, $msg)
                    } else {
                        Write-TrimEvent -Level 'Error' -Message ("Failed to delete versions for {0}: {1}" -f $item.FileRef, $msg)
                    }

                    foreach ($v in $chunk) {
                        Write-TrimLog "Delete" $item.LibraryTitle $item.FileRef $v.ID $v.VersionLabel $v.Created "Failed" $msg
                    }
                }
            }
        }

        if ($index -ge $total) { break }

        if (-not $AutoContinue) {
            $x = Read-Host "Batch $batchNumber done. Press Enter to continue, or type 'q' to quit"
            if ($x -eq 'q') { break }
        }
    }

    # Finish progress bar
    Write-Progress -Activity "Trimming file versions" -Completed

    #
    # --- SIZE AFTER + SUMMARY ---
    #
    $endSizeBytes = 0L
    foreach ($list in $lists) {
        $size = Get-PnPListSizeBytes -ListTitle $list.Title
        $endSizeBytes += $size
        if ($LogPath) {
            Write-PnPSizeLog -LogPath $LogPath -RunId $runId `
                -SiteUrl $siteUrl -LibraryTitle $list.Title -Phase 'After' -SizeBytes $size
        }
    }

    $reclaimed = $startSizeBytes - $endSizeBytes

    Write-Host ""
    Write-Host "===== Version trim summary =====" -ForegroundColor Cyan
    Write-Host ("  Total files scanned        : {0}" -f $processedCount)
    Write-Host ("  Files with old versions    : {0}" -f $filesWithOldVersions)
    Write-Host ("  Failed deletions           : {0}" -f $failedDeletes)
    Write-Host ("  Skipped due to errors/holds: {0}" -f $skippedByError)
    if ($LogPath)     { Write-Host ("  CSV log                    : {0}" -f $LogPath) }
    if ($TextLogPath) { Write-Host ("  Text log                   : {0}" -f $TextLogPath) }

    Write-Host ""
    Write-Host ("Starting total size : {0:N2} MB" -f ($startSizeBytes / 1MB)) -ForegroundColor Cyan
    Write-Host ("Ending total size   : {0:N2} MB" -f ($endSizeBytes / 1MB))   -ForegroundColor Cyan
    Write-Host ("Estimated reclaimed : {0:N2} MB" -f ($reclaimed / 1MB))      -ForegroundColor Green

    #
    # WRITE STATE FILE (for “first run is dry run” behavior)
    #
    $stateOut = @{ LastDryRunUtc = (Get-Date).ToUniversalTime().ToString("o") } | ConvertTo-Json
    Set-Content -Path $stateFile -Value $stateOut -Encoding UTF8

    Write-Host ""
    if ($effectiveDryRun) {
        Write-Host "DONE (Dry Run). Next run can use -Delete." -ForegroundColor Cyan
    } else {
        Write-Host "DONE (Deleted permitted versions)." -ForegroundColor Green
    }
}

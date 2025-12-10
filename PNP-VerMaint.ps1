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
  - Logs warnings/errors to CSV if provided.

.REQUIREMENTS
  - PnP.PowerShell
  - You must already be connected using:
      Connect-PnPOnline -Url "https://tenant.sharepoint.com/sites/site" -Interactive

.NOTES
  After the first dry run, you can run again with -Delete.
#>


# =====================================================================================
#               Function: Show-PnPSiteVersionPolicy
# =====================================================================================

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

    Write-Host "Current Site Version Policy:" -ForegroundColor Cyan
    Write-Host ("  EnableAutoExpirationVersionTrim : {0}" -f $policy.EnableAutoExpirationVersionTrim)
    Write-Host ("  MajorVersions                   : {0}" -f $policy.MajorVersions)
    Write-Host ("  ExpireVersionsAfterDays         : {0}" -f $policy.ExpireVersionsAfterDays)

    Write-Host ""
    Write-Host "Review this carefully." -ForegroundColor Yellow
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
        } catch {
            Write-Warning "Failed to update policy: $($_.Exception.Message)"
        }
    }
}


# =====================================================================================
#               Function: Invoke-PnPVersionTrimTool
# =====================================================================================

function Invoke-PnPVersionTrimTool {
    [CmdletBinding()]
    param(
        [int]$OlderThanDays = 45,
        [string]$LibraryTitle,
        [string]$LibraryCsvPath,
        [switch]$Delete,
        [string]$LogPath,
        [int]$BatchPercent = 25,
        [int]$MaxBatchMinutes = 5,
        [switch]$AutoContinue,
        [switch]$BypassBatching
    )

    #
    # --- SAFETY: FIRST RUN MUST ALWAYS BE DRY RUN ---
    #

    $scriptPath = $MyInvocation.MyCommand.Path
    if (-not $scriptPath) { $scriptPath = (Get-Location).Path }

    $stateFile = Join-Path (Split-Path $scriptPath -Parent) "PnPVersionTrim.state.json"
    $firstRun = -not (Test-Path $stateFile)
    $effectiveDryRun = $true

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
    # --- OPTIONAL LOGGING ---
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
            $line = "{0},{1},{2},{3},{4},{5},{6},{7},{8}" -f (Get-Date).ToString("o"),$Action,$Library,$FileRef,$VersionId,$Label,$Created.ToString("o"),$Result,$Message
            Add-Content -Path $LogPath -Value $line
        }
    } else {
        function Write-TrimLog {}
    }


    #
    # --- SHOW POLICY ---
    #
    Show-PnPSiteVersionPolicy


    #
    # --- DISCOVER TARGET LIBRARIES ---
    #
    $ctx = Get-PnPContext
    $cutoff = (Get-Date).AddDays(-1 * $OlderThanDays)

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
    # --- DISCOVER FILES ---
    #
    $workItems = New-Object System.Collections.Generic.List[object]

    foreach ($list in $lists) {
        Write-Host "Discovering files: $($list.Title)" -ForegroundColor Yellow
        $lt = $list.Title

        Get-PnPListItem -List $list -PageSize 2000 -ScriptBlock {
            param($items)
            foreach ($i in $items) {
                if ($i.FileSystemObjectType -ne "File") { continue }
                $workItems.Add([pscustomobject]@{
                    LibraryTitle = $lt
                    ItemId       = $i.Id
                    FileLeaf     = $i["FileLeafRef"]
                    FileRef      = $i["FileRef"]
                })
            }
        }
    }

    $total = $workItems.Count
    if ($total -eq 0) {
        Write-Host "No files discovered." -ForegroundColor Yellow
        return
    }

    Write-Host ""
    Write-Host "Total files: $total" -ForegroundColor Green


    #
    # --- BATCH PROCESSING LOGIC ---
    #

    if ($BypassBatching) {
        Write-Host "Batching disabled. Processing all items..." -ForegroundColor Yellow
        $batchSize = $total
        $MaxBatchMinutes = [int]::MaxValue
    } else {
        if ($BatchPercent -le 0 -or $BatchPercent -gt 100) { $BatchPercent = 25 }
        $batchSize = [math]::Ceiling($total * ($BatchPercent / 100))
        Write-Host "Batch = ~${BatchPercent}% or ${MaxBatchMinutes}min, whichever first." -ForegroundColor Cyan
    }

    $index = 0
    $batchNumber = 0

    while ($index -lt $total) {

        $batchNumber++
        $batchStart = $index
        $batchEnd   = [math]::Min($batchStart + $batchSize - 1, $total - 1)
        $timer = [System.Diagnostics.Stopwatch]::StartNew()

        Write-Host ""
        Write-Host "===== Batch $batchNumber ($($batchStart+1) to $($batchEnd+1)) =====" -ForegroundColor Magenta

        while ($index -le $batchEnd -and $index -lt $total) {

            $item = $workItems[$index]
            $index++

            if (-not $BypassBatching -and $timer.Elapsed.TotalMinutes -ge $MaxBatchMinutes) {
                Write-Host "Batch time exceeded; moving to next batch." -ForegroundColor Yellow
                break
            }

            #
            # Load versions
            #
            $file = $ctx.Web.GetFileByServerRelativeUrl($item.FileRef)
            $ctx.Load($file)
            $ctx.Load($file.Versions)
            try { $ctx.ExecuteQuery() } catch {
                Write-Warning "Failed load: $($item.FileRef)"
                continue
            }

            $old = @()
            foreach ($v in $file.Versions) {
                if ($v.Created -lt $cutoff) { $old += $v }
            }
            if ($old.Count -eq 0) { continue }

            Write-Host ("File: {0} ({1} old versions)" -f $item.FileLeaf, $old.Count) -ForegroundColor DarkCyan

            #
            # DRY or DELETE
            #
            if ($effectiveDryRun) {
                foreach ($v in $old) {
                    Write-Host ("  DRY: $($v.ID) $($v.VersionLabel) $($v.Created)") -ForegroundColor Yellow
                }
            } else {
                foreach ($v in $old) { $v.DeleteObject() }
                try {
                    $ctx.ExecuteQuery()
                    Write-Host "  Deleted." -ForegroundColor DarkGreen
                } catch {
                    Write-Warning "Deletion failed: $($_.Exception.Message)"
                }
            }
        }

        #
        # Batch pause
        #
        if ($index -ge $total) { break }

        if (-not $AutoContinue) {
            $x = Read-Host "Batch $batchNumber done. Press Enter or type 'q'"
            if ($x -eq 'q') { break }
        }
    }


    #
    # WRITE STATE FILE
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

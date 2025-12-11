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
  - You must already be connected

.NOTES
  After the first dry run, you can run again with -Delete.
#>

# Text log path (optional, separate from CSV)
param(
    ...
    [string]$TextLogPath = "./SPO-TrimVersions.log"
)

function Write-TrimEvent {
    param(
        [string]$Level,     # Info / Warn / Error
        [string]$Message
    )

    $ts = (Get-Date).ToString("o")
    $line = "[$ts] [$Level] $Message"

    if ($TextLogPath) {
        $dir = Split-Path $TextLogPath -Parent
        if ($dir -and -not (Test-Path $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }
        Add-Content -Path $TextLogPath -Value $line
    }

    switch ($Level) {
        'Error' { Write-Host $line -ForegroundColor Red }
        'Warn'  { Write-Host $line -ForegroundColor Yellow }
        default { } # Info stays in file only
    }
}

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

    Write-Host "========== Current Site Version Policy (Raw) ==========" -ForegroundColor Cyan
    # Show everything the policy object exposes
    $policy | Format-List * 

    # Derive a status line
    $autoStatus = if ($policy.EnableAutoExpirationVersionTrim) { "ENABLED" } else { "DISABLED" }

    Write-Host ""
    Write-Host "----------------- Policy Status Summary -----------------" -ForegroundColor Cyan

      Write-Host ("Auto-expiration : {0}" -f $autoStatus) -ForegroundColor ($policy.EnableAutoExpirationVersionTrim ? 'Green' : 'Yellow')
    Write-Host (" Major versions  : {0}" -f $policy.MajorVersions)
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

			# Record the policy update for cooldown logic
			$global:PnPVersionTrim_LastPolicyUpdateUtc = (Get-Date).ToUniversalTime()
		} catch {
			Write-Warning "Failed to update policy: $($_.Exception.Message)"
		}

    }
}



# =====================================================================================
#               Section: Guard Rail Features and Pre-Main Function Add-ons
# =====================================================================================

	function Get-PnPListSizeBytes {
		[CmdletBinding()]
		param(
			[Parameter(Mandatory)]
			[string]$ListTitle
		)

		$ctx  = Get-PnPContext
		$list = Get-PnPList -Identity $ListTitle

		$root = $list.RootFolder
		$ctx.Load($root)
		$ctx.Load($root.Files)
		$ctx.ExecuteQuery()

		$total = 0L
		foreach ($f in $root.Files) {
			$total += $f.Length
		}

		return $total
	}

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

		$mb = [math]::Round($SizeBytes / 1MB, 2)
		$line = "{0},{1},{2},{3},{4},{5},{6}" -f (Get-Date).ToString("o"),
			$RunId, $SiteUrl, $LibraryTitle, $Phase, $SizeBytes, $mb

		Add-Content -Path $LogPath -Value $line
	}

	function Write-PnPSizeLog {
		param(
			[string]$LogPath,
			[string]$RunId,
			[string]$SiteUrl,
			[string]$LibraryTitle,
			[string]$Phase,         # "Before" / "After"
			[long]$SizeBytes
		)

		if (-not (Test-Path (Split-Path $LogPath -Parent))) {
			New-Item -ItemType Directory -Force -Path (Split-Path $LogPath -Parent) | Out-Null
		}
		if (-not (Test-Path $LogPath)) {
			"Timestamp,RunId,SiteUrl,LibraryTitle,Phase,SizeBytes,SizeMB" |
				Out-File -FilePath $LogPath -Encoding UTF8
		}

		$mb = [math]::Round($SizeBytes / 1MB, 2)
		$line = "{0},{1},{2},{3},{4},{5},{6}" -f (Get-Date).ToString("o"), $RunId, $SiteUrl, $LibraryTitle, $Phase, $SizeBytes, $mb
		Add-Content -Path $LogPath -Value $line
	}


# =====================================================================================
#               Function: Invoke-PnPVersionTrimTool (WITH PER-FILE BATCHING)
# =====================================================================================
	$runId   = [Guid]::NewGuid().ToString()
	$siteUrl = (Get-PnPContext).Url

	# BEFORE
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
        [switch]$BypassBatching,

        # ---- NEW: per-file version batching controls ----
        [int]$VersionBatchSize = 50,
        [int]$VersionBatchPauseMs = 500,
        [int]$MaxRetries = 5
    )

    #
    # --- SAFETY: FIRST RUN MUST ALWAYS BE DRY RUN ---
    #
	$stateRoot = Join-Path $env:LOCALAPPDATA "PnPVersionTrim"
	if (-not (Test-Path $stateRoot)) {
		New-Item -ItemType Directory -Force -Path $stateRoot | Out-Null
	}
	$stateFile = Join-Path $stateRoot "state.json"

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
    # --- SHOW POLICY (assumes you have this helper available) ---
    #
    # --- SHOW POLICY ---
	Show-PnPSiteVersionPolicy

	# Cooldown: if policy was just updated, refuse to run
	if ($global:PnPVersionTrim_LastPolicyUpdateUtc) {
		$minutesSinceChange = (New-TimeSpan -Start $global:PnPVersionTrim_LastPolicyUpdateUtc -End (Get-Date).ToUniversalTime()).TotalMinutes
		if ($minutesSinceChange -lt 30) {
			Write-Host "Policy was just changed ($([math]::Round($minutesSinceChange,1)) minutes ago)." -ForegroundColor Yellow
			Write-Host "Skipping trim to avoid running while policy changes may be pending." -ForegroundColor Yellow
			return
		}
	}

	function Get-PnPListSizeBytes {
		param([string]$ListTitle)

		$list = Get-PnPList -Identity $ListTitle
		$ctx  = Get-PnPContext
		$ctx.RequestTimeout = 600000   # 600,000 ms = 10 minutes
		$folder = $list.RootFolder
		$ctx.Load($folder)
		$ctx.Load($folder.Files)
		$ctx.ExecuteQuery()

		$total = 0L
		foreach ($f in $folder.Files) {
			$total += $f.Length
		}
		return $total
	}
	function Invoke-PnPWithRetry {
    param(
        [scriptblock]$Action,
        [int]$MaxAttempts = 5
    )

    $attempt = 1
    while ($true) {
        try {
            & $Action
            break
        } catch {
            if ($attempt -ge $MaxAttempts) { throw }
            $delay = [math]::Pow(2, $attempt)  # 2,4,8,16...
            Write-Warning "Attempt $attempt failed: $($_.Exception.Message). Retrying in $delay seconds..."
            Start-Sleep -Seconds $delay
            $attempt++
			}
		}
	}


    #
    # --- DISCOVER TARGET LIBRARIES ---
    #
    $ctx = Get-PnPContext
    $cutoff = (Get-Date).AddDays(-1 * $OlderThanDays)

    # Increase request timeout to reduce timeouts during large deletes (min 10 minutes)
    $ctx.RequestTimeout = [Math]::Max(($MaxBatchMinutes * 60000), 600000)

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
	[int]$MaxFilesPerBatch = 2000
	[int]$MaxFilesThreshold = 200000  # configurable
	$global:FilesSeen = 0
	$batchNumber = 0

	foreach ($list in $lists) {
		Write-Host "Processing: $($list.Title)" -ForegroundColor Yellow

		Get-PnPListItem -List $list -PageSize 2000 -ScriptBlock {
			param($items)

			if (-not $script:BatchStopwatch) {
				$script:BatchStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
				$script:FilesInBatch   = 0
				$script:BatchNumber    = (++$script:BatchNumber)
				Write-Host "===== Batch $($script:BatchNumber) =====" -ForegroundColor Magenta
			}

			foreach ($i in $items) {
				if ($i.FileSystemObjectType -ne "File") { continue }

				$global:FilesSeen++
				$script:FilesInBatch++

				# do your file version trim here (load versions, dry/delete etc.)

				if ($script:FilesInBatch -ge $MaxFilesPerBatch -or
					$script:BatchStopwatch.Elapsed.TotalMinutes -ge $MaxBatchMinutes) {

					$script:BatchStopwatch.Stop()
					Write-Host "Batch $($script:BatchNumber) limit reached." -ForegroundColor Yellow
					# if you have 
					if ($global:FilesSeen -gt $MaxFilesThreshold) {
						throw "FilesSeen ($global:FilesSeen) exceeded MaxFilesThreshold ($MaxFilesThreshold). Aborting for safety."
					}

					if (-not $AutoContinue) {
						$resp = Read-Host "Press Enter to continue, or 'q' to quit"
						if ($resp -eq 'q') { throw "User aborted batches." }
					}

					$script:BatchStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
					$script:FilesInBatch   = 0
					$script:BatchNumber++
					Write-Host "===== Batch $($script:BatchNumber) =====" -ForegroundColor Magenta
				}
			}
		}
	}

    #
    # --- HELPERS: per-file batching & retries ---
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

    function Get-Chunked([System.Collections.IEnumerable]$collection, [int]$size) {
        if ($size -le 0) { $size = 50 }
        $chunk = New-Object System.Collections.Generic.List[object]
        foreach ($item in $collection) {
            $chunk.Add($item)
            if ($chunk.Count -ge $size) {
                ,$chunk.ToArray()
                $chunk.Clear()
            }
        }
        if ($chunk.Count -gt 0) { ,$chunk.ToArray() }
    }

    #
    # --- BATCH PROCESSING LOGIC (across files) ---
    #

    # Global counters across all batches
    $processedCount       = 0
    $filesWithOldVersions = 0
    $failedDeletes        = 0
    $skippedByError       = 0

    Write-Host "Starting version trim..." -ForegroundColor Cyan
    Write-TrimEvent -Level 'Info' -Message "Starting trim: total files = $total, cutoff = $cutoff"

    if ($BypassBatching) {
        Write-Host "Batching disabled. Processing all items..." -ForegroundColor Yellow
        $batchSize       = $total
        $MaxBatchMinutes = [int]::MaxValue
    } else {
        if ($BatchPercent -le 0 -or $BatchPercent -gt 100) { $BatchPercent = 25 }
        $batchSize = [math]::Ceiling($total * ($BatchPercent / 100))
        Write-Host "Batch = ~${BatchPercent}% or ${MaxBatchMinutes} min, whichever comes first." -ForegroundColor Cyan
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

            # Update progress bar (single dynamic output)
            $percent = [math]::Floor(($processedCount / $total) * 100)
            Write-Progress -Activity "Trimming file versions" `
                           -Status "Processed $processedCount of $total files; $filesWithOldVersions had old versions" `
                           -PercentComplete $percent

            #
            # Load file + versions via list + item ID (avoids URL issues)
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
            # Filter versions older than cutoff
            # (current is not in .Versions; IsCurrentVersion is just extra safety)
            #
            $old = @()
            foreach ($v in $versions) {
                if (-not $v.IsCurrentVersion -and $v.Created -lt $cutoff) { $old += $v }
            }
            if ($old.Count -eq 0) { continue }

            $filesWithOldVersions++

            #
            # DRY RUN: only log what *would* be deleted, no ExecuteQuery spam to console
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
            $chunkSize = 50

            for ($i = 0; $i -lt $old.Count; $i += $chunkSize) {
                $chunk = $old[$i..([math]::Min($i + $chunkSize - 1, $old.Count - 1))]

                foreach ($v in $chunk) {
                    $v.DeleteObject()
                }

                try {
                    # Use your existing retry wrapper here if you have one
                    Invoke-PnPWithRetry { $ctx.ExecuteQuery() }

                    foreach ($v in $chunk) {
                        Write-TrimLog "Delete" $item.LibraryTitle $item.FileRef $v.ID $v.VersionLabel $v.Created "Deleted" "Deleted"
                    }
                } catch {
                    $failedDeletes++
                    $msg = $_.Exception.Message

                    # Distinguish retention/records vs other failures (optional)
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

        #
        # Batch pause / prompt between across-file batches
        #
        if ($index -ge $total) { break }

        if (-not $AutoContinue) {
            $x = Read-Host "Batch $batchNumber done. Press Enter to continue, or type 'q' to quit"
            if ($x -eq 'q') { break }
        }
    }

    # Finish progress bar
    Write-Progress -Activity "Trimming file versions" -Completed

    # End-of-run summary
    Write-Host ""
    Write-Host "===== Version trim summary =====" -ForegroundColor Cyan
    Write-Host ("  Total files scanned        : {0}" -f $processedCount)
    Write-Host ("  Files with old versions    : {0}" -f $filesWithOldVersions)
    Write-Host ("  Failed deletions           : {0}" -f $failedDeletes)
    Write-Host ("  Skipped due to errors/holds: {0}" -f $skippedByError)
    if ($LogPath)      { Write-Host ("  CSV log                    : {0}" -f $LogPath) }
    if ($TextLogPath)  { Write-Host ("  Text log                   : {0}" -f $TextLogPath) }

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
		Write-Host ("Ending total size   : {0:N2} MB" -f ($endSizeBytes / 1MB)) -ForegroundColor Cyan
		Write-Host ("Estimated reclaimed : {0:N2} MB" -f ($reclaimed / 1MB)) -ForegroundColor Green

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
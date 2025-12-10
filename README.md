# PNP-Version-Maintenance-Tool
Safely delete SharePoint File Versions only with this tool using PNP


## Example Usage - not worth autoloading
- cd C:\AdminTools\PnP-VersionTrimTool
- . .\PnP-VersionTrimTool.ps1
#### First run (DryRun forced)
- Invoke-PnPVersionTrimTool -OlderThanDays 60
- Invoke-PnPVersionTrimTool -OlderThanDays 60 -Delete
- Delete

##### Parameters
- No Batch
- Invoke-PnPVersionTrimTool -OlderThanDays 45 -Delete -BypassBatching
- With Logs
- Invoke-PnPVersionTrimTool -OlderThanDays 10 -LogPath .\trim-log.csv
- Multi Libraries
- Invoke-PnPVersionTrimTool -OlderThanDays 10 -LibraryCsvPath .\libs.csv


.csv 
- LibraryTitle
- Documents
- Finance Team
- Marketing Docs


### Always does dry run on initial run 
### Displays the policy and status just in case
### one or multi libraries via .csv
### 25% or 5min batch and Auto


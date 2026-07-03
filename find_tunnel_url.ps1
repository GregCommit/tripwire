param(
    [Parameter(Mandatory=$true)][string]$LogPath,
    [int]$TimeoutSeconds = 30
)

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$url = $null

while ((Get-Date) -lt $deadline -and -not $url) {
    Start-Sleep -Seconds 1
    if (Test-Path $LogPath) {
        $match = Select-String -Path $LogPath -Pattern 'https://[a-zA-Z0-9.-]+\.trycloudflare\.com' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($match) { $url = $match.Matches[0].Value }
    }
}

Write-Output ""
if ($url) {
    Write-Output "  ================================================================"
    Write-Output "   Public URL (open this on your phone):"
    Write-Output "   $url"
    Write-Output "  ================================================================"
} else {
    Write-Output "  Could not detect the tunnel URL within $TimeoutSeconds seconds."
    Write-Output "  Check the 'Tripwire Tunnel' window for the URL or an error."
}
Write-Output ""

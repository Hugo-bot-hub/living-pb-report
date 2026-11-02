param([Parameter(Mandatory=$true)][string]$Message)
# 대시보드 품질 경고를 Windows 토스트로 띄운다. (이 파일은 BOM 없이 저장되므로 제목은 코드포인트로 조립)
[Windows.UI.Notifications.ToastNotificationManager,Windows.UI.Notifications,ContentType=WindowsRuntime] | Out-Null
[Windows.Data.Xml.Dom.XmlDocument,Windows.Data.Xml.Dom.XmlDocument,ContentType=WindowsRuntime] | Out-Null
$title = -join ([char[]](0xC219,0xD5E4,0xB4DC,0x20,0xB300,0xC2DC,0xBCF4,0xB4DC,0x20,0xC810,0xAC80,0x20,0xD544,0xC694))
$safe = [Security.SecurityElement]::Escape($Message)
$xml = New-Object Windows.Data.Xml.Dom.XmlDocument
$xml.LoadXml("<toast><visual><binding template='ToastGeneric'><text>$title</text><text>$safe</text></binding></visual></toast>")
$appId = '{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\WindowsPowerShell\v1.0\powershell.exe'
[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($appId).Show([Windows.UI.Notifications.ToastNotification]::new($xml))

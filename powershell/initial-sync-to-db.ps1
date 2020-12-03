$Script:RequiredVars = 'env:CC_USER_NAME', 'env:CC_API_KEY', 'env:CC_PROJECT_NAME', 'env:CC_APP_ID', 'env:CC_DB_URL', 'env:CC_APP_STRUCTURE_FOLDER_PATH', 'env:CC_REPO_PATH', 'env:CC_SINCE_DAYS'

$Script:MissingVars = 0

foreach ($var in $Script:RequiredVars) {
	if (-not (Test-Path $var)) {
		echo "Expected $var to be set but it isn't"
		$Script:MissingVars++
	}
}

if ($Script:MissingVars > 0) {
	echo "$Script:MissingVars env vars were not set that need to be. Exiting"
	exit 1
}

$script:SinceDateString = (Get-Date).addDays(-$env:CC_SINCE_DAYS).ToString("yyyy-MM-dd")


cd $env:CC_REPO_PATH
.\venv\Scripts\activate.ps1

sync-commcare-app-to-db.exe `
	--username $env:CC_USER_NAME `
	--api-key $env:CC_API_KEY `
	--project $env:CC_PROJECT_NAME `
	--app-id $env:CC_APP_ID `
	--db-url $env:CC_DB_URL `
	--app-structure-json-save-folder-path $env:CC_APP_STRUCTURE_FOLDER_PATH `
	--since $script:SinceDateString
exit

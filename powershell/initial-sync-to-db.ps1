$RequiredVars = 'env:CC_USER', 'env:CC_API_KEY', 'env:CC_PROJECT', 'env:CC_APP_ID', 'env:CC_BACKUP_DB_URL', 'env:CC_APP_STRUCTURE_SAVE_FOLDER'

$MissingVars = 0

foreach ($var in $RequiredVars) {
	if (-not Test-Path $var) {
		echo "Expected $var to be set but it isn't"
		$Script:MissingVars++
	}
}

if ($MissingVars > 0) {
	echo "$MissingVars env vars were not set that need to be. Exiting"
	exit 1
}

# cd to the right drive

sync-commcare-app-to-db `
	--username $env:CC_USER `
	--api_key $env:CC_API_KEY `
	--project $env:CC_PROJECT `
	--app-id $env:CC_APP_ID `
	--db-url $env:CC_BACKUP_DB_URL
	--existing-app-structure-json $env:CC_APP_STRUCTURE_SAVE_FOLDER

exit

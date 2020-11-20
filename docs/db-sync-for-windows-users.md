# CommCare db sync setup for Windows

This is a guide to setting up a Windows machine for syncing a CommCare instance to a SQL db, using an end-to-end, fully automated solution. It provides a step-by-step guide to installing the `commcare-utilties` package, setting environment variables, and syncing to a Microsoft SQL Server database. It also describes how to set up scheduled tasks to automatically sync your database at regular intervals.

Note that this guide does not cover all possible configurations on Windows or the script, and users of this script are also encouraged to review the the README file of the commcare-utilities repo. Specifically, this guide covers working with Powershell and SQL Server. If you use the Windows Subsystem for Linux, some of the commands will be different, and if you use a different database server, that setup will look different.

- [CommCare db sync setup for Windows](#commcare-db-sync-setup-for-windows)
  - [Setup, configuration and first run](#setup-configuration-and-first-run)
  - [Setting up a scheduled task](#setting-up-a-scheduled-task)
  - [Keeping your package up to date and reporting problems](#keeping-your-package-up-to-date-and-reporting-problems)

## Setup, configuration and first run

1. **Install and configure Python**: If you don't already have Python 3 installed on your system, you'l need to take care of that first. Additionally, you may consider using [pyenv-win](https://github.com/pyenv-win/pyenv-win) if you have a need for having multiple Python versions in the environment in which you'll be running this code.
2. **Get the `commcare-utilities` Python package**: Clone [the `commcare-utilities` repo](https://github.com/caktus/commcare-utilities) to the computer that will run the script. Note that if you're installing this script in an environment that does not have open internet access or otherwise cannot access PyPI to download depedencies, you'll need to pre-download the dependencies for this repo. We won't cover that in detail in this guide, but [here is one good solution](https://stackoverflow.com/a/53625778/1264950). Also, note that if you can't run Git, you can download `commcare-utilities` as a Zip file from the repo page.
3. **Create and activate a virtual environment**: From a command line environment on the machine that will be running the script, navigate into the `commcare-utilities` folder. Verify that you're using Python 3, then create a virtual environment by running the following command (substituting in the appropriate value for the path to virtual environment portion -- you'll probably want put this in the root of the commcare-utilities folder):

    ```bash
    python -m venv c:\path\to\myenv
    ```

    This command will create a new virtual environment at the path you specified. Now you need to activate it. To do that, run the following command:

    ```bash
    # if using Command Shell
    C:\> <path-to-venv>\Scripts\activate.bat

    # if using Powershell
    C:\> <venv>\Scripts\Activate.ps1
    ```

4. **Install the dependencies into your virtual environment**: From the same command prompt you used in the previous step, run following command:

    ```bash
    pip install -e .
    ```

5. **Install database drivers**: Next, you'll need to separately install the database driver for the SQL database you're using. Assuming you're using SQL Server, from the same command prompt as in the previous step, run:

    ```bash
    pip install pydobc
    ```

    At this point,  you can close the command line terminal you've been working in so far, as we'll be setting up environment variables, and will need to start a new terminal in order to pick up the new environment variables.

6. **Create your database and db user**: Follow relevant instructions for your database server to create a new database which will hold your CommCare data and to create a database user that will remotely access the database when the script runs. Note that if you're running SQL Server, you'll need to enable remote access to the db.
7. **Set up environment variables**: Now you'll need to set up environment variables that you'll reference when running the script from your command line environment. Open `Control Panel` -> `System Properties` and then click the `Environment Variables` button. Now, you'll set up the following environment variables as user variables:

   - `CC_API_KEY`: You will need to set up an API key in your CommCare dashboard, [as explained here](https://confluence.dimagi.com/display/commcarepublic/Authentication#Authentication-ApiKeyauthentication). Note that CommCare API keys can be whitelisted to a specific IP address. If the IP address of the computer this script will be running on is static, we recommend whitelisting it. Copy your API key from CommCare and save it as the value for `CC_API_KEY`.
   - `CC_APP_ID`: This value can be retreived by logging into your CommCareHQ dashboard, selecting your application from the `Applications` dropdown, then extracting it from the URL of the resulting page. The URL will look like this: `https://www.commcarehq.org/a/<project-name>/apps/view/<this-is-your-app-id>/`.
   - `CC_DB_URL`: This value is a connection string encompassing the database host, name, port, username, password, and driver. For SQL Server, your database url will look something like this: `mssql+pyodbc//<db-user-name>:<db-password>@<db-host-name>:<db-port>/<db-name>?driver=ODBC+Driver+17+for+SQL+Server`
   - `CC_PROJECT_NAME`: You can find this value in your CommCare dashboard by clicking on Dashboard. This will take you to a page with a URL that looks like this: `https://www.commcarehq.org/a/<project-name>/dasbhoard/project`. Set `CC_PROJECT_NAME` to the value you find for `<project-name>`.
   - `CC_USER_NAME`: This will be the email address that you use to log in to the CommCare account that you used to create the API key. **NOTE** this account will also need to have the `System Admin (API)` user role in order for the script to work.
   - `CC_APP_STRUCTURE_FOLDER_PATH`: The database sync script saves a JSON file representing the structure of your CommCare instance (in terms of case types and their field names). Set this value to a folder where you'd like to store this JSON file. For instance, if you've stored `commcare-utilities` to your Documents folder, you might set this value to: `C:\Users\<username>\Documents\commcare-utilities\assets`, after creating a folder called `assets` in the `commcare-utilities` folder.
   - `CC_APP_STRUCTURE_FILE_PATH`: This is the full path to the JSON file that will store the application structure data. This file doesn't exist yet, but the script will use a predictable file name for it, so we can set it now. Set this value to `%CC_APP_STRUCTURE_FOLDER_PATH%\app_structure_latest.json`.
   - `CC_REPO_PATH`: The path to the folder of the `commcare-utilities` repo.

    That's it for setting environment variables. Click "OK" to save these changes.

8. **Open a new command line terminal**: We'll need a new terminal for the new environment variables to take effect. In your new terminal, you can confirm they've been set by running `echo $env:CC_USER_NAME` (Powershell) or `echo %CC_USER_NAME%` (Command Prompt). Now navigate back into the `commcare-utilities` folder and activate your virtual environment.
9. **Run the script**: Now we'll run the script for the first time. Run the following command:

    ```bash
    # Powershell...
    sync-commcare-app-to-db.exe --username $env:CC_USER_NAME --api-key $env:CC_API_KEY --project $env:CC_PROJECT_NAME --app-id $env:CC_APP_ID --db-url $env:CC_DB_URL --app-structure-json-save-folder-path $env:CC_APP_STRUCTURE_FOLDER_PATH

    # OR... Command Prompt
    sync-commcare-app-to-db.exe --username %CC_USER_NAME% --api-key %CC_API_KEY% --project %CC_PROJECT_NAME% --app-id %CC_APP_ID% --db-url %CC_DB_URL% --app-structure-json-save-folder-path %CC_APP_STRUCTURE_FOLDER_PATH
    ```

    This script will likely take several minutes to run. It will output logs as it adds tables and columns to the database.
10. **Confirm your data in your db**: You can confirm that data synced by connecting to your DB using the client of your choice and verifying that new tables have been created and that rows are found.
11. **Re-run the script with the app structure file**: Finally, try re-running the script, but point it to the `app_structure_latest.json` file to avoid the API call to CommCare. This will cause the script to run considerably faster. From the same terminal you were previously working in, run:

    ```bash
    sync-commcare-app-to-db.exe --username $env:CC_USER_NAME --api-key $env:CC_API_KEY --project $env:CC_PROJECT_NAME --app-id $env:CC_APP_ID --db-url $env:CC_DB_URL --existing-app-structure-json $env:CC_APP_STRUCTURE_FILE_PATH

    # OR... Command Prompt
    sync-commcare-app-to-db.exe --username %CC_USER_NAME% --api-key %CC_API_KEY% --project %CC_PROJECT_NAME% --app-id %CC_APP_ID% --db-url %CC_DB_URL% --existing-app-structure-json %CC_APP_STRUCTURE_FILE_PATH
    ```

## Setting up a scheduled task

We've included two Powershell scripts in this repository that can be used to easily set up scheduled tasks to sync down your CommCare data to your db.

The first script (`/powershell/initial-sync-to-db.ps1`) relies on making a call to the CommCare API to retrieve case types and their properties.

The second script (`/powershell/sync-to-db-with-structure-json.ps1`) takes advantage of an `app_structure_latest.json` file saved by the first script in a predictable location to avoid this call.

The strategy we recommend is to set up two scheduled tasks: one that runs the first script less frequently (say once a week, or once a day), and another that runs the second script more frequently at the interval your use case requires.

When the first script runs, it will refresh the app_structure_latest.json file, so you'll pick up any new field types. The disadvantage of running the first script is that it takes much longer and it is an intensive call for the CommCare API to process. We want to avoid having the API make many long-running requests. This should not be a problem, as it is unlikely that your app will have new properties more than once a week.

In order for this overall strategy to work, you should initially run `sync-commcare-app-to-db.exe` pulling down all data in your instance, which is what we did in the setup instructions above, the first time we ran the script:

```bash
sync-commcare-app-to-db.exe --username $env:CC_USER_NAME --api-key $env:CC_API_KEY --project $env:CC_PROJECT_NAME --app-id $env:CC_APP_ID --db-url $env:CC_DB_URL --app-structure-json-save-folder-path $env:CC_APP_STRUCTURE_FOLDER_PATH
```

After all of your historical data has initially been synced to your db, moving forward, you can request only the most recent X-days of data, which is the strategy we take in `initial-sync-to-db.ps1` and `sync-to-db-with-structure-json.ps1`.

Both Powershell scripts expect an environment variable to be set called `CC_SINCE_DAYS`, which you'll need to set in order for them to run. This variable will determine how many days back from the current day to sync data. So if you set the value as `7`, when either Powershell script runs, it will only request the most recent seven days worth of data.

## Keeping your package up to date and reporting problems

You should consider periodically pulling down changes to this repository in order to update your code with any bug fixes that come out. If you cloned this repository from GitHub, you can do that by running `git pull` in the from the root folder of this repository.

If you encounter any bugs or unexpected behavior, [please file an issue on this repo](https://github.com/caktus/commcare-utilities/issues)!

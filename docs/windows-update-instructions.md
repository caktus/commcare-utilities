# How to update your installation

These instructions assume you're working in a Windows environment.

- [How to update your installation](#how-to-update-your-installation)
  - [If installed via Git](#if-installed-via-git)
  - [If installed via downloaded zip file from GitHub](#if-installed-via-downloaded-zip-file-from-github)

## If installed via Git

If you installed `commcare-utilities` via Git, here's what you need to do:

1. Open a Powershell terminal and navigate to the `commcare-utilities` folder.
1. Run the command `git pull` to pull in the most recent version of the code.
1. Activate your virtual environment (if using one). For this step, we assume that you installed a virtual environment called 'venv' to the root of the `commcare-utilities` folder. Run the command:

    ```powershell
    .\venv\Scripts\Activate.ps1
    ```

1. Now you'll install the updated version of the this package and its dependencies. From the same powershell terminal, run the command:

    ```powershell
    pip install -e .
    ```

1. Confirm that the updated version of the dependency `commcare-export` was installed by running the following command:

    ```powershell
    commcare-export --version
    ```

    You should see a version greater than or equal to 1.4.0.

1. Finally, test that the script works as expected. From the same Powershell, run the following command:

    ```powershell
    sync-commcare-app-to-db.exe --username $env:CC_USER_NAME --api-key $env:CC_API_KEY --project $env:CC_PROJECT_NAME --app-id $env:CC_APP_ID --db-url $env:CC_DB_URL --app-structure-json-save-folder-path $env:CC_APP_STRUCTURE_FOLDER_PATH
    ```

## If installed via downloaded zip file from GitHub

If you installed `commcare-utilities`, here's what you'll need to do:

1. First, back up your existing `commcare-utilities` project — note that this will probably be called `commcare-utilities-main`. You can do this by right-clicking on the folder and selecting "rename". Rename it to something like "commcare-utilities-main-backup". You most likely won't need the backup, but in case you've put additional files in the folder, it's a good idea to back it up.
1. Next, go to https://github.com/caktus/commcare-utilities/archive/main.zip in your browser, which will automatically update the repository as a zip file. Alternatively, you can go to https://github.com/caktus/commcare-utilities , click "Code", then "Download ZIP".
1. Unzip the folder.
1. Copy the unzipped `commcare-utilities-main` to the same place your earlier version of the repo was placed. This will allow you to retain the existing values you set for environment variables for the db sync script.
1. Open a Powershell terminal and navigate to `commcare-utilities-main`.
1. Next, you'll need to repeat some of the original setup steps from the ["Setup, configuration, and first run" section of the Commcare db sync setup for Windows doc](https://github.com/caktus/commcare-utilities/blob/main/docs/db-sync-for-windows-users.md#setup-configuration-and-first-run) instructions you originally followed. Specifically, do steps 3 ("Create and activate a virtual environment"), 4 ("Install the dependencies into your virtual environment"), 5 ("Install database drivers"), and 7 ("Create an assets folder" -- for this one, make sure to create a folder in same location and with same name as the original one so your existing environment variables will work).

    ```powershell
    python -m venv ./venv
    ```

1. Next, activate your virtual environment by runnning:

    ```powershell
    ./venv/Scripts/Activate.ps1
    ```

1. Now you'll install the updated version of the this package and its dependencies. From the same powershell terminal, run the command:

    ```powershell
    pip install -e .
    ```

1. You'll also need to install pyodbc manually. From the same Powershell terminal, run:

    ```powershell
    pip install pyodbc
    ```

1. Confirm that the updated version of the dependency `commcare-export` was installed by running the following command:

    ```powershell
    commcare-export --version
    ```

    You should see a version that is greater than or equal to 1.4.0.

1. Finally, test that the script works as expected. From the same Powershell, run the following command:

    ```powershell
    sync-commcare-app-to-db.exe --username $env:CC_USER_NAME --api-key $env:CC_API_KEY --project $env:CC_PROJECT_NAME --app-id $env:CC_APP_ID --db-url $env:CC_DB_URL --app-structure-json-save-folder-path $env:CC_APP_STRUCTURE_FOLDER_PATH
    ```

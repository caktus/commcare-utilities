If computer has internet access and can use pip/PyPy

1. Install Python 3.7 if not already on system.
2. If you use virtualenv or pyenv etc., create your virtual environment and activate it.
3. Install the dependencies

```
pip install .
```

```
pip install install pyodbc
```


5.  (including db driver)
6. Gather required parameters. Save them as env vars or as encrypted.
7. Ensure firewire rules will allow the script to talk to https://www.commcarehq.org
8. Run script for first time, saving JSON file to a given path.
9.  Confirm that it worked by checking your db (and the logs)
10. Get the full path to the `app_structure_latest.json` file.
11. Point to that in subsequent runs
12. Cron strategy.

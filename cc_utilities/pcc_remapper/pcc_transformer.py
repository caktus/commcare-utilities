import sys
from pathlib import Path

from bs4 import BeautifulSoup
from database.models import PccDBRecord

if __name__ == "__main__":
    from cc_utilities.pcc_remapper.tests.message_factory import (
        FAKED_MESSAGES_FILE,
        generate_message_file,
    )

    num_messages = sys.argv[1]
    read = sys.argv[2]
    if not read:
        print("Creating messages")
        message_file = generate_message_file(int(num_messages))
        soup = ""
        with FAKED_MESSAGES_FILE.open("r") as fh:
            soup = BeautifulSoup(fh.read(), "html.parser")
        # FAKED_MESSAGES_FILE.unlink()
        recorder = PccDBRecord(soup=soup)
        recorder.write_each()
    else:
        print("Logging messages")
        patients_csv = Path("tests/patients.csv")
        labs_csv = Path("tests/labs.csv")
        recorder = PccDBRecord()
        patients, labs = recorder.messages_to_csv()
        with patients_csv.open("w", encoding="utf-8") as fh:
            fh.write(patients.getvalue())
        with labs_csv.open("w", encoding="utf-8") as fh:
            fh.write(labs.getvalue())
    recorder.close()

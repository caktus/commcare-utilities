"""xml_poster
A utility function that is meant to send fake data from the Rhapsody Server to the CommCare Proxy
"""
import random
import signal
import ssl
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

from cc_utilities.logger import logger
from cc_utilities.pcc_remapper.tests.message_factory import (
    FAKED_MESSAGES_FILE,
    generate_message_file,
)

PATIENTS_CSV = Path("patients.csv")
LABS_CSV = Path("labs.csv")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def signal_handler(signal, frame):
    logger.info("Shutting Down")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

logger.name = __file__


def send_file(file_xml):
    headers = {"Content-Type": "application/xml"}
    with file_xml.open("rb") as fh:
        req = Request(
            "https://192.168.160.21/pcc-record-request",
            fh.read(),
            headers=headers,
            method="POST",
            unverifiable=True,
        )
        with urlopen(req, context=ctx) as response:
            logger.info(f"Response: {response}")


def fake_sender(time_out=120, num_messages=random.randint(1, 200), times_to_send=None):
    time_out, num_messages, times_to_send = (
        int(time_out),
        int(num_messages),
        int(times_to_send),
    )
    if times_to_send:
        for x in range(times_to_send):
            generate_message_file(num_messages=num_messages)
            logger.info(f"SENDING: {num_messages}")
            send_file(FAKED_MESSAGES_FILE)
            FAKED_MESSAGES_FILE.unlink()
            logger.info("Messages Sent")
            time.sleep(time_out)
    else:
        # Run until a signal is sent
        while True:
            generate_message_file(num_messages=num_messages)
            logger.info(f"SENDING: {num_messages}")
            send_file(FAKED_MESSAGES_FILE)
            FAKED_MESSAGES_FILE.unlink()
            logger.info("Messages sent")
            time.sleep(time_out)


if __name__ == "__main__":
    kwargs = {}
    if len(sys.argv) == 2:
        kwargs["time_out"] = sys.argv[1]
    if len(sys.argv) == 3:
        kwargs["time_out"] = sys.argv[1]
        kwargs["num_messages"] = sys.argv[2]
    if len(sys.argv) == 4:
        kwargs["time_out"] = sys.argv[1]
        kwargs["num_messages"] = sys.argv[2]
        kwargs["times_to_send"] = sys.argv[3]

    fake_sender(**kwargs)

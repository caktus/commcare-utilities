import sys
from pathlib import Path
from urllib.request import Request, urlopen


def send_file(file_xml):
    headers = {"Content-Type": "application/xml"}
    with file_xml.open("rb") as fh:
        req = Request(
            "https://192.168.160.21/log-request",
            fh.read(),
            headers=headers,
            method="POST",
            unverifiable=True,
        )
        with urlopen(req) as response:
            print(response)


if __name__ == "__main__":
    xml_file = Path(sys.argv[1])
    send_file(xml_file)

# This is a simple HTTP server to accept connections from a Rhapsody REST Client
import argparse
from wsgiref.simple_server import make_server

from cc_utilities.logger import logger
from cc_utilities.pcc_remapper.database.classes import PccDBRecord


def generate_response(start_response, status, response_body):
    headers = [
        ("Content-type", "text/plain"),
        ("Content-Length", str(len(response_body))),
    ]
    start_response(status, headers)
    return [response_body.encode("utf-8")]


def app(environ, start_response):
    # Adapted from: https://stackoverflow.com/a/775698/166053
    path = environ["PATH_INFO"]
    method = environ["REQUEST_METHOD"]
    pcc_record = None
    if method == "POST":
        if path.startswith("/pcc-record-request"):
            try:
                request_body_size = int(environ["CONTENT_LENGTH"])
                request_body = environ["wsgi.input"].read(request_body_size)
                pcc_record = PccDBRecord(raw=request_body.decode("utf-8"))
            except (TypeError, ValueError):
                logger.exception("Failed to read or decode request")
                return generate_response(
                    start_response, "500 Internal Server Error", "internal server error"
                )
            return generate_response(start_response, "200 OK", "OK")
    else:
        return generate_response(start_response, "404 Not Found", "not found")

    if pcc_record:
        pcc_record.write_each()


def main_with_args(bind_host, bind_port):
    """The main routine
    Args:
    """
    try:
        httpd = make_server(bind_host, bind_port, app)
        logger.info(f"Serving on port {bind_host}:{bind_port}...")
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Goodbye.")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "bind_host",
        help="The host IP to listen on",
    )
    parser.add_argument(
        "bind_port",
        help="The host port to listen on",
    )
    args = parser.parse_args()
    main_with_args(
        args.bind_host,
        int(args.bind_port),
    )


if __name__ == "__main__":
    main()
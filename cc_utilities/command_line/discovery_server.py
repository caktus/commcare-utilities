# This is a simple HTTP server to accept connections from a Rhapsody REST Client
import argparse
import logging
import sys
from wsgiref.simple_server import make_server

# FIXME: Use main logger configuration if/when merged with the rest of the repo
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

CONSOLE_LOGGER_LEVEL = "INFO"
FILE_LOGGER_LEVEL = "INFO"

console_logger = logging.StreamHandler(sys.stdout)
console_logger.setLevel(logging.DEBUG)
console_logger.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(getattr(logging, CONSOLE_LOGGER_LEVEL))
logger.addHandler(console_logger)

logger = logging.getLogger(__file__)


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
    if method == "POST":
        if path.startswith("/log-request"):
            try:
                request_body_size = int(environ["CONTENT_LENGTH"])
                request_body = environ["wsgi.input"].read(request_body_size)
                logger.info(request_body.decode("utf-8"))
            except (TypeError, ValueError):
                logger.exception("Failed to read or decode request")
                return generate_response(
                    start_response, "500 Internal Server Error", "internal server error"
                )
            return generate_response(start_response, "200 OK", "OK")
    else:
        return generate_response(start_response, "404 Not Found", "not found")


def main_with_args(bind_host, bind_port):
    """The main routine

    Args:

    """

    try:
        httpd = make_server(bind_host, bind_port, app)
        print(f"Serving on port {bind_host}:{bind_port}...")
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("Goodbye.")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "bind_host", help="The host IP to listen on",
    )
    parser.add_argument(
        "bind_port", help="The host port to listen one (email address)",
    )
    args = parser.parse_args()
    main_with_args(
        args.bind_host, int(args.bind_port),
    )


if __name__ == "__main__":
    main()

from prometheus_client import start_http_server, Summary
import time
import random


REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")


@REQUEST_TIME.time()
def process_request(t):
    time.sleep(t)


if __name__ == "__main__":
    start_http_server(8000)
    while True:
        process_request(random.random())

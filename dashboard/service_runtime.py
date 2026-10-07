"""Shutdown coordination for the dashboard's non-streaming Flask responses."""
from __future__ import annotations

import threading


class RequestDrain:
    """Keep shared collectors/history alive until admitted Flask handlers finish.

    Dashboard routes build their HTML/JSON entirely inside the application call;
    response iterables contain only materialized bytes. Network delivery is left
    to the HTTP server and does not hold the shared database open.
    """

    def __init__(self, application):
        self._application = application
        self._condition = threading.Condition()
        self._active = 0
        self._closing = False

    def __call__(self, environ, start_response):
        with self._condition:
            closing = self._closing
            if not closing:
                self._active += 1
        if closing:
            start_response('503 Service Unavailable', [('Content-Type', 'text/plain'), ('Retry-After', '1')])
            return [b'Dashboard is stopping.']
        try:
            return self._application(environ, start_response)
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def close(self):
        with self._condition:
            self._closing = True
            self._condition.wait_for(lambda: self._active == 0)

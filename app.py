import json

from flask import (
    Flask, render_template
)
from prometheus_client import make_wsgi_app
from werkzeug.wsgi import DispatcherMiddleware
from wsgiref.simple_server import make_server
import time
from prometheus_client.core import REGISTRY
from collector import PersistentVolumeUsageCollector
import os ,sys
sys.path.append(os.path.dirname(__file__))

def create_app():

    app = Flask(__name__, instance_relative_config=True)
    app_dispatch = DispatcherMiddleware(app, {
        '/metrics': make_wsgi_app()
    })
    return app_dispatch


if __name__ == "__main__":

    collector = PersistentVolumeUsageCollector()
    REGISTRY.register(collector)
    app = create_app()
    httpd = make_server('', 20211, app)
    httpd.serve_forever()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        pass
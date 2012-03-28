from ujson import decode as json_decode, encode as json_encode
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import defer
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure

from sitebase.backend.postgres import dbBackend

from ysl.twisted.log import debug, info

from yaml import load as yaml_load
import time
import re
import codecs


class SettingService(Resource):

    isLeaf = True
    serviceName = "setting"

    routes = [
        ("^/field/?$", "render_field"),
        ("^/manifest/?$", "render_manifest"),
        ("^/cache/?$", "render_cache"),
    ]

    def __init__(self, c, *args, **kwargs):
        Resource.__init__(self, *args, **kwargs)
        self.config = c
        self.routes = map(lambda x: (re.compile(x[0]), x[1]), self.routes)

    def prepare(self, request):
        request.content.seek(0, 0)
        content = request.content.read()
        if content:
            return defer.succeed(json_decode(content))
        else:
            return defer.succeed(None)

    def finish(self, value, request):
        request.setHeader('Content-Type', 'application/json; charset=UTF-8')
        if isinstance(value, Failure):
            err = value.value
            request.setResponseCode(500)
            error = dict(error="generic", message=str(err))
            request.write(json_encode(error) + "\n")
        else:
            request.setResponseCode(200)
            request.write(json_encode(value) + "\n")

        info("respone time: %.3fms" % ((time.time() - self.startTime) * 1000))
        request.finish()

    def cancel(self, err, call):
        debug("Request cancelling.")
        call.cancel()

    def select(self, input, node_id):
        return dbBackend.select_cache(node_id)

    def render(self, *args, **kwargs):
        self.startTime = time.time()
        return Resource.render(self, *args, **kwargs)

    def render_GET(self, request):
        path = request.path[8:]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        matched = False
        for route in self.routes:
            if route[0].match(path):
                d.addCallback(lambda x, y:
                                  deferToThread(getattr(self, route[1]), x, y),
                              request)
                matched = True
                break

        if not matched:
            d.addCallback(self.render_not_found, request)

        d.addBoth(self.finish, request)
        return NOT_DONE_YET

    def render_not_found(self, input, request):
        return dict()

    def render_field(self, input, request):
        yaml = self.config.get("extra", "field")
        with codecs.open(yaml, "r", encoding="utf-8") as f:
            tree = yaml_load(f.read())
            return tree
        return dict()

    def render_manifest(self, input, request):
        yaml = self.config.get("extra", "manifest")
        with codecs.open(yaml, "r", encoding="utf-8") as f:
            tree = yaml_load(f.read())
            return tree
        return dict()

    def render_cache(self, input, request):
        yaml = self.config.get("extra", "cache")
        with codecs.open(yaml, "r", encoding="utf-8") as f:
            tree = yaml_load(f.read())
            return tree
        return dict()

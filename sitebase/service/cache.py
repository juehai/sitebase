from ujson import decode as json_decode, encode as json_encode
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import defer
from twisted.python import log
from twisted.python.failure import Failure

from sitebase.backend.postgres import dbBackend
from sitebase.service.error import ArgumentError

from ysl.twisted.log import debug, info

import time
import logging


class CacheService(Resource):

    isLeaf = False
    serviceName = "cache"

    def __init__(self, c, *args, **kwargs):
        Resource.__init__(self, *args, **kwargs)
        self.config = c

    def getChild(self, name, request):
        if name == 'build':
            return BuildService(self.config)
        else:
            return self

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
        node_id = request.path.split("/")[-1]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        try:
            node_id = int(node_id)
        except ValueError:
            raise ArgumentError("id must be integer")
        d.addCallback(self.select, node_id)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET


class BuildService(Resource):

    isLeaf = True

    def __init__(self, c, *args, **kwargs):
        Resource.__init__(self, *args, **kwargs)
        self.config = c

    def prepare(self, request):
        request.content.seek(0, 0)
        content = request.content.read()
        if content:
            return defer.succeed(json_decode(content))
        else:
            return defer.succeed(None)

    def build(self, input, node_id):
        try:
            node_id = int(node_id)
        except ValueError:
            raise ArgumentError("id must be integer")
        return dbBackend.build_cache(node_id)

    def finish(self, value, request):
        log.msg("finish value = %s" % str(value), level=logging.DEBUG)
        request.setHeader('Content-Type', 'application/json; charset=UTF-8')
        if isinstance(value, Failure):
            err = value.value
            request.setResponseCode(500)
            error = dict(error="generic", message=str(err))
            request.write(json_encode(error) + "\n")
        else:
            request.setResponseCode(200)
            request.write(json_encode(value) + "\n")

        log.msg("respone time: %.3fms" % (
                (time.time() - self.startTime) * 1000))
        request.finish()

    def cancel(self, err, call):
        log.msg("Request cancelling.", level=logging.DEBUG)
        call.cancel()

    def render(self, *args, **kwargs):
        self.startTime = time.time()
        return Resource.render(self, *args, **kwargs)

    def render_GET(self, request):
        id = request.path.split("/")[-1]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.build, id)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

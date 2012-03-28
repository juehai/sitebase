from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import defer
from twisted.python import log
from twisted.python.failure import Failure
from sitebase.backend.postgres import dbBackend
from ujson import decode as json_decode, encode as json_encode
from sitebase import backend
from sitebase.service.error import MalformedInput, ArgumentError
import time
import logging
from ysl.twisted.log import debug


class NodeService(Resource):

    isLeaf = True
    serviceName = "node"

    def __init__(self, c, *args, **kwargs):
        self.config = c
        self.debug = c.get("server:main", "debug") == "1"
        Resource.__init__(self, *args, **kwargs)

    def select(self, input, node_id, cascade):
        try:
            node_id = int(node_id)
        except ValueError:
            raise ArgumentError("id must be integer")

        return dbBackend.select(node_id, cascade)

    def create(self, input, check_only=False):
        if isinstance(input, dict):
            if "manifest" not in input or "value" not in input:
                raise MalformedInput("manifest or value not specified")
            try:
                node_id = input.get("id", None)
                if node_id:
                    node_id = int(node_id)
            except ValueError:
                raise ArgumentError("id must be integer")

            d = dbBackend.create(node_id, input["manifest"], input["value"])
        else:
            d = dbBackend.upsert(input, True, check_only=check_only)
        return d

    def update(self, input, node_id, check_only=False):
        if node_id:
            try:
                node_id = int(node_id)
            except ValueError:
                raise ArgumentError("id must be integer")
            if "manifest" not in input or "value" not in input:
                raise MalformedInput("manifest or value not specified")
            d = dbBackend.update(node_id, input["manifest"], input["value"])
        else:
            d = dbBackend.upsert(input, check_only=check_only)
        return d

    def delete(self, input, node_id, cascade):
        try:
            node_id = int(node_id)
        except ValueError:
            raise ArgumentError("id must be integer")

        return dbBackend.delete(node_id, cascade)

    def prepare(self, request):
        request.content.seek(0, 0)
        content = request.content.read()
        debug("content size = %d" % len(content))
        if content:
            return defer.succeed(json_decode(content))
        else:
            return defer.succeed(None)

    def finish(self, value, request):
        request.setHeader('Content-Type', 'application/json; charset=UTF-8')
        if isinstance(value, Failure):
            err = value.value
            if self.debug:
                print "-" * 30, "TRACEKBACK", "-" * 30
                value.printTraceback()
                print "^" * 30, "TRACEKBACK", "^" * 30
            request.setResponseCode(500)
            if isinstance(err, backend.ValidationError):
                request.setResponseCode(400)
            elif isinstance(err, backend.NodeNotFound):
                request.setResponseCode(404)
            elif isinstance(err, backend.NodeInUseError):
                request.setResponseCode(400)
            elif isinstance(err, backend.EmptyInputData):
                request.setResponseCode(400)
            elif isinstance(err, backend.BatchOperationError):
                request.setResponseCode(400)
            elif (isinstance(err, Exception) and
                  not isinstance(err, backend.GenericError)):
                err = dict(error="UnknownError", message=err.message)
            debug("original error: %s" % err)
            request.write(json_encode(dict(err)) + "\n")
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

    def _id(self, request):
        try:
            return request.path.split("/")[2:][0]
        except:
            return None

    def render_PUT(self, request):
        check_only = request.args.get("check_only", [False])[0]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.create, check_only=check_only)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

    def render_POST(self, request):
        check_only = request.args.get("check_only", [False])[0]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.update, self._id(request), check_only=check_only)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

    def render_GET(self, request):
        cascade = request.args.get("cascade", [None])[0]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.select, self._id(request), cascade)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

    def render_DELETE(self, request):
        cascade = request.args.get("cascade", [None])[0]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.delete, self._id(request), cascade)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

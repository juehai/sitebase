from ujson import decode as json_decode, encode as json_encode
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.internet import threads

from sitebase import backend, slex
from sitebase.backend.postgres import dbBackend

from ysl.twisted.log import debug, info

import time


class CheckSyntaxService(Resource):

    isLeaf = True
    serviceName = "check_syntax"

    def __init__(self, c, *args, **kwargs):
        Resource.__init__(self, *args, **kwargs)
        self.config = c
        self.debug = c.get("server:main", "debug") == "1"

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
            elif isinstance(err, backend.SearchGrammarError):
                err = dict(error="syntax", message=str(err),
                           traceback=value.getTraceback())
            elif (isinstance(err, Exception) and
                  not isinstance(err, backend.GenericError)):
                err = dict(error="UnknownError", message=err.message)
            request.write(json_encode(dict(err)) + "\n")
        else:
            request.setResponseCode(200)
            request.write(json_encode(value) + "\n")

        info("respone time: %.3fms" % ((time.time() - self.startTime) * 1000))
        request.finish()

    def cancel(self, err, call):
        debug("Request cancelling.")
        call.cancel()

    def render(self, *args, **kwargs):
        self.startTime = time.time()
        return Resource.render(self, *args, **kwargs)

    def check_syntax(self, q):
        suffix = slex.parse(q)
        dbBackend._build_where_clause(suffix)
        return dict(success=True)

    def render_GET(self, request):
        q = request.args.get("q", [None])[0]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(lambda x: threads.deferToThread(self.check_syntax, q))
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

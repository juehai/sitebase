from ujson import decode as json_decode, encode as json_encode
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import defer
from twisted.python import log
from twisted.python.failure import Failure

from sitebase.backend.postgres import dbBackend
from sitebase.service.error import ArgumentError
from sitebase import backend
from sitebase.slex import ParseError

import time
import logging


class SearchService(Resource):

    isLeaf = True
    serviceName = "search"

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

    def search(self, input, q, start, num, order_by, order, return_total):
        try:
            start = int(start)
            num = int(num)
        except ValueError:
            raise ArgumentError("start or num must be integer")
        if not q:
            q = input["q"].encode("UTF-8")
        return dbBackend.search(q, start, num, order_by, order, return_total)

    def finish(self, value, request):
        request.setHeader('Content-Type', 'application/json; charset=UTF-8')
        if isinstance(value, Failure):
            err = value.value
            request.setResponseCode(500)
            if isinstance(err, ArgumentError):
                error = dict(error="argument", message=str(err))
            elif isinstance(err, backend.SearchGrammarError):
                error = dict(error="syntax", message=str(err),
                             traceback=value.getTraceback())
            elif isinstance(err, ParseError):
                error = dict(error="syntax", message=str(err))
            else:
                error = dict(error="generic", message=str(err),
                             traceback=value.getTraceback())
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
        q = request.args.get("q", [None])[0]
        start = request.args.get("start", ["0"])[0]
        num = request.args.get("num", ["20"])[0]
        order_by = request.args.get("order_by", ["id"])[0]
        order = request.args.get("order", ["asc"])[0]
        return_total = request.args.get("return_total", ["0"])[0]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.search, q, start, num,
                      order_by, order, return_total)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

    def render_POST(self, request):
        start = request.args.get("start", ["0"])[0]
        num = request.args.get("num", ["20"])[0]
        order_by = request.args.get("order_by", ["id"])[0]
        order = request.args.get("order", ["asc"])[0]
        return_total = request.args.get("return_total", ["0"])[0]
        d = self.prepare(request)
        request.notifyFinish().addErrback(self.cancel, d)
        d.addCallback(self.search, None, start, num,
                      order_by, order, return_total)
        d.addBoth(self.finish, request)
        return NOT_DONE_YET

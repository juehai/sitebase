from txpostgres import txpostgres
from twisted.internet import defer
from twisted.internet.defer import DeferredList
from collections import namedtuple, defaultdict

from sitebase import backend, slex
from ysl.twisted.log import debug, warn

import re
import time

# FIXES:
#       * strip all input vales [TODO]
#       * search by post        [DONE]


def _debug(*argv, **kwargs):
    print ">" * 40, argv

NodeValue = namedtuple("NodeValue", ["field", "value", "referer"])


class PostgresBackend(object):

    pool = None

    RE_EXPAND_VAR = re.compile("%{([a-zA-Z0-9._-]+)}")

    SQL_CHECK_UNIQUE = "SELECT count(1) FROM nodes WHERE manifest = \
%(manifest)s AND lower(value->%(field)s) = lower(%(value)s) LIMIT 1"

    SQL_CHECK_UNIQUE_ONUPDATE = "SELECT count(1) FROM nodes WHERE manifest = \
%(manifest)s AND lower(value->%(field)s) = lower(%(value)s) \
AND id != %(id)s LIMIT 1"

    SQL_CREATE_NODE_WITH_ID = "INSERT INTO nodes(id, cn, manifest, value) \
VALUES(%(id)s, %(cn)s, %(manifest)s, %(value)s)"

    SQL_CREATE_NODE = "INSERT INTO nodes(cn, manifest, value) \
VALUES(%(cn)s, %(manifest)s, %(value)s)"

    SQL_CREATE_CACHE = "INSERT INTO node_cache(id, cn, \
                                               manifest, value, depends) \
VALUES(%(id)s, %(cn)s, %(manifest)s, %(value)s, %(depends)s)"

    SQL_GET_LAST_ID = "SELECT currval('nodes_id_seq')"

    SQL_UPDATE_NODE = "UPDATE nodes SET value = value || %(value)s, \
cn = %(cn)s WHERE id = %(id)s"

    SQL_UPDATE_CACHE = "UPDATE node_cache SET value = value || %(value)s, \
cn = %(cn)s WHERE id = %(id)s"

    SQL_SELECT_BY_CN = "SELECT id FROM nodes \
WHERE manifest = ANY(%(manifest)s) and cn = %(cn)s LIMIT 1"

    SQL_SELECT_NODE_BASIC = "SELECT id, manifest, cn, depends FROM nodes \
WHERE id = %(id)s LIMIT 1"

    SQL_SELECT_NODE = """SELECT key, value FROM each((SELECT value || \
('.id=>"' || id || '"')::hstore || \
('.manifest=>"' || replace(manifest, '"', '\\\\"') || '"')::hstore || \
('.cn=>"' || replace(cn, '"', '\\\\"') || '"')::hstore \
FROM nodes WHERE id = %(id)s LIMIT 1))"""

    SQL_SELECT_CACHE = """SELECT key, value FROM each((SELECT value || \
('.id=>"' || id || '"')::hstore || \
('.manifest=>"' || replace(manifest, '"', '\\\\"') || '"')::hstore || \
('.cn=>"' || replace(cn, '"', '\\\\"') || '"')::hstore \
FROM node_cache WHERE id = %(id)s LIMIT 1))"""

    SQL_DELETE_NODE = "DELETE FROM nodes WHERE id = ANY(%(id)s)"

    SQL_DELETE_CACHE = "DELETE FROM node_cache WHERE id = ANY(%(id)s)"

    SQL_SELECT_REFERERS = "SELECT id FROM nodes \
WHERE manifest = ANY(%(referers)s) AND value->%(field)s = %(value)s"

    SQL_SELECT_DEPENDS = "SELECT id FROM nodes \
WHERE depends @> %(depends)s"

    SQL_SELECT_CACHE_EX = """SELECT id, (each(value)).key, \
    (each(value)).value FROM (SELECT id, (value \
    || ('.manifest=>\"' || replace(manifest, '"', '\\\\"') || '\"')::hstore \
    || ('.cn=>\"' || replace(cn, '"', '\\\\"') || '\"')::hstore) AS value \
    FROM node_cache WHERE %(where_clause)s \
        ORDER BY %(order_by)s %(order)s NULLS FIRST \
LIMIT %(limit)s OFFSET %(offset)s ) AS e"""

    SQL_COUNT_CACHE_EX = "SELECT count(1) FROM node_cache \
WHERE %(where_clause)s"

    @staticmethod
    def _serialize_hstore(val):
        """
        Serialize a dictionary into an hstore literal. Keys and values
        must both be strings.
        """
        esc = lambda v: unicode(v).replace('"', r'\"').encode("UTF-8")
        return ', '.join('"%s"=>"%s"' % (esc(k), esc(v))
                         for k, v in val.iteritems())

    def connect(self, *args, **kwargs):
        assert(self.pool == None)
        print args, "*"*60
        self.pool = txpostgres.ConnectionPool(None, *args, **kwargs)
        return self.pool.start()

    def configure(self, *args, **kwargs):
        self.manifest = kwargs["manifest"]
        self.field = kwargs["field"]
        self.cache = kwargs["cache"]

        # build back reference
        self.backref = dict()
        fieldref = dict(map(lambda x: (x, self.field[x]["reference"]),
                            filter(lambda x: "reference" in self.field[x],
                                   self.field)))
        for name, value in self.manifest.items():
            for field in value["field"]:
                if field not in fieldref:
                    continue
                for refer in fieldref[field]:
                    if refer not in self.backref:
                        self.backref[refer] = dict(field=field,
                                                   referers=list())
                    self.backref[refer]["referers"].append(name)

    def is_duplicated(self, node_id, manifest, field, value, create):
        data = dict(id=node_id, manifest=manifest, field=field,
                    value=value[field])
        if create:
            d = self.pool.runQuery(self.SQL_CHECK_UNIQUE, data)
        else:
            d = self.pool.runQuery(self.SQL_CHECK_UNIQUE_ONUPDATE, data)
        d.addCallback(lambda x: x[0][0])
        return d

    def select_by_cn(self, manifest, cn):
        d = self.pool.runQuery(self.SQL_SELECT_BY_CN,
                               dict(manifest=manifest, cn=cn.lower()))
        return d

    @defer.inlineCallbacks
    def _validate_simple(self, node_id, manifest, field, value, create=False):
        properties = self.manifest[manifest]["field"][field]
        if "not_null" in properties and properties["not_null"]:
            if create and (field not in value or not value[field]):
                raise backend.NullValueError(field)
            elif (field in value and not value[field]):
                raise backend.NullValueError(field)

        if (("unique" in properties and properties["unique"])
            and field in value):
            duplicated = yield self.is_duplicated(
                node_id, manifest, field, value, create)
            if duplicated:
                raise backend.UniqueValueError(field, value[field])

        if ("regex" in self.field[field] and field in value):
            regex = self.field[field]["regex"]
            if regex:
                match = re.match(regex, value[field])
                if not match:
                    raise backend.RegexMatchError(field, value[field], regex)

    def decorate_value(self, field, value):

        if field not in value or "decorator" not in self.field[field]:
            return value

        for decorator in self.field[field]["decorator"]:
            if decorator == 'lower':
                value[field] = value[field].lower()
            if decorator == 'upper':
                value[field] = value[field].upper()

        return value

    @defer.inlineCallbacks
    def validate_value(self, node_id, manifest, field, value, create=False):
        referer, desired_type = None, unicode

        value = self.decorate_value(field, value)

        if ("reference" in self.field[field]
            and field in value and value[field]):
            referer = value[field].lower()
            remote = yield self.select_by_cn(self.field[field]["reference"],
                                             referer)

            if not remote:
                raise backend.ReferenceNotFound(manifest=manifest,
                                        name=field, referer=referer)

            value[field] = unicode("@%s" % remote[0][0])
        else:
            yield self._validate_simple(node_id, manifest, field, value,
                                        create)

        if field not in value:
            value[field] = (None, desired_type())[create]

        if value[field] and not isinstance(value[field], desired_type):
            raise backend.ValueTypeError(name=field, expect=str(desired_type))

        defer.returnValue(NodeValue(field=field,
                                    value=value[field], referer=referer))

    @defer.inlineCallbacks
    def _map_relation(self, node_id, manifest, values, create=False):
        errors, verified, raw_value = list(), dict(), dict()
        for success, node_value in values:
            if not success:
                errors.append(node_value.value)
            elif node_value.value is not None:
                field, value = node_value.field, node_value.value
                verified[field] = raw_value[field] = value
                if node_value.referer:
                    raw_value[field] = node_value.referer

        if errors:
            raise backend.ValidationError(errors)

        if node_id and not create:
            updated_node = yield self.select(node_id, False)
            updated_node.update(raw_value)
        else:
            updated_node = raw_value

        relation = dict(id=node_id,
                    cn=(self.manifest[manifest]["cn"] % updated_node).lower(),
                    manifest=manifest,
                    value=verified)

        defer.returnValue(relation)

    @defer.inlineCallbacks
    def _do_upsert(self, c, relations, force_create):
        affected, cache_affected = 0, 0
        node_cache = dict()
        debug("# of relations ready to process: %d" % len(relations))
        for node_id, relation in relations:
            if force_create:
                s = (self.SQL_CREATE_NODE_WITH_ID,
                     self.SQL_CREATE_NODE)[node_id is None]
            elif node_id is None:
                s = self.SQL_CREATE_NODE
            else:
                s = self.SQL_UPDATE_NODE
            c = yield c.execute(s, relation)
            affected += c._cursor.rowcount
            if not node_id:
                result = yield c.execute(self.SQL_GET_LAST_ID)
                id = result.fetchall()
                node_id = id[0][0]
            cache = yield self._build_cache(c, node_id, node_cache)
            if cache["success"]:
                cache_affected += cache["affected"]
        node_cache.clear()
        defer.returnValue((affected, cache_affected))

    @defer.inlineCallbacks
    def upsert(self, input, force_create=False, check_only=False):
        relations = list()
        errors = list()

        if not input:
            raise backend.EmptyInputData()

        debug("upsert: get %d items" % len(input))
        for item in input:

            if not isinstance(item, dict):
                raise backend.GenericError("Input data is invalid")

            try:
                node_id = item.get("id", None)
                if node_id:
                    node_id = int(node_id)
            except ValueError:
                errors.append((node_id,
                               backend.DataError("id", node_id,
                                                 "node id must be integer")))
                continue

            manifest, value = item["manifest"], item["value"]

            if manifest not in self.manifest:
                errors.append((node_id,
                               backend.ManifestNotFound(manifest=manifest)))
                continue

            defers = list()
            for field in self.manifest[manifest]["field"]:
                if field in self.field:
                    d = self.validate_value(
                        node_id, manifest, field, value,
                        create=force_create or node_id is None)
                    defers.append(d)

            try:
                verified = yield DeferredList(defers, consumeErrors=True)
                relation = yield self._map_relation(node_id, manifest,
                                                    verified,
                                                    create=force_create)
                relation["value"] = self._serialize_hstore(relation["value"])
                relations.append((node_id, relation))
            except backend.ValidationError as e:
                errors.append((node_id, e))

        if errors:
            raise backend.BatchOperationError(errors=errors)

        if check_only:
            defer.returnValue({"success": True})

        (affected, cache_affected) = yield self.pool.runInteraction(
            self._do_upsert, relations, force_create)
        defer.returnValue(dict(success=True, affected=affected,
                               cache=dict(success=True,
                                          affected=cache_affected)))

    @defer.inlineCallbacks
    def _do_create(self, c, node_id, relation):
        s = (self.SQL_CREATE_NODE_WITH_ID,
             self.SQL_CREATE_NODE)[node_id is None]
        c = yield c.execute(s, relation)
        affected = c._cursor.rowcount
        if not node_id:
            result = yield c.execute(self.SQL_GET_LAST_ID)
            id = result.fetchall()
            node_id = id[0][0]
        cache = yield self._build_cache(c, node_id)
        defer.returnValue((affected, cache, node_id))

    @defer.inlineCallbacks
    def create(self, node_id, manifest, value):

        if manifest not in self.manifest:
            raise backend.ManifestNotFound(
                "manifest '%s' is not found" % manifest)

        defers = list()
        for field in self.manifest[manifest]["field"]:
            if field in self.field:
                d = self.validate_value(
                    node_id, manifest, field, value, create=True)
                defers.append(d)

        verified = yield DeferredList(defers, consumeErrors=True)
        relation = yield self._map_relation(node_id, manifest, verified,
                                            create=True)
        relation["value"] = self._serialize_hstore(relation["value"])
        (affected, cache, node_id) = \
			yield self.pool.runInteraction(self._do_create,
                                           node_id, relation)
        defer.returnValue(dict(success=True, affected=affected, cache=cache,
		                       node_id=node_id))

    @defer.inlineCallbacks
    def _do_update(self, c, manifest, node_id, relation):
        c = yield c.execute(self.SQL_UPDATE_NODE, relation)
        affected = c._cursor.rowcount
        cache = yield self._build_cache(c, node_id)
        if cache["success"] == True:
            depends = yield self._select_depends(c, node_id)
            for depend in depends:
                cache = yield self._build_cache(c, depend)
                if cache["success"]:
                    affected = affected + 1
            cache = dict(success=True, affacted=affected)
        defer.returnValue((affected, cache))

    @defer.inlineCallbacks
    def update(self, node_id, manifest, value):

        if manifest not in self.manifest:
            raise backend.ManifestNotFound(
                "manifest '%s' is not found" % manifest)

        if not node_id:
            raise ValueError("id is empty")

        defers = list()
        for field in self.manifest[manifest]["field"]:
            if field in self.field:
                d = self.validate_value(node_id, manifest, field, value)
                defers.append(d)

        verified = yield DeferredList(defers, consumeErrors=True)
        relation = yield self._map_relation(node_id, manifest, verified)
        relation["value"] = self._serialize_hstore(relation["value"])
        (affected, cache) = yield self.pool.runInteraction(self._do_update,
                                                           manifest,
                                                           node_id,
                                                           relation)
        defer.returnValue(dict(success=True, affected=affected, cache=cache))

    @defer.inlineCallbacks
    def select(self, node_id, cascade):
        if not node_id:
            raise ValueError("id is empty")

        if cascade:
            node = yield self.pool.runInteraction(self._build_node_tree,
                                                  node_id)
            defer.returnValue(node)

        result = yield self.pool.runQuery(self.SQL_SELECT_NODE,
                                          dict(id=node_id))
        if not result:
            raise backend.NodeNotFound(id=node_id)

        raw = dict(map(lambda x: (x[0].decode("UTF-8"),
                                  x[1].decode("UTF-8")), result))
        retval = {".id": raw.pop(".id"), ".manifest": raw.pop(".manifest")}
        retval.update(raw)

        defer.returnValue(retval)

    @defer.inlineCallbacks
    def _do_delete(self, c, node_id, cascade=None):

        referers = yield self._select_referers(c, node_id)
        referers = map(lambda x: int(x[0]), referers)
        if not cascade and referers:
            raise backend.NodeInUseError(id=node_id,
                                         referers=referers)
        node_ids = [node_id]
        if cascade and referers:
            node_ids.extend(referers)
        c = yield c.execute(self.SQL_DELETE_CACHE, dict(id=node_ids))
        c = yield c.execute(self.SQL_DELETE_NODE, dict(id=node_ids))
        defer.returnValue(c._cursor.rowcount)

    @defer.inlineCallbacks
    def delete(self, node_id, cascade):
        if not id:
            raise ValueError("id is empty")
        assert(isinstance(node_id, int))
        # XXX: Cascade DELETE
        affected = yield self.pool.runInteraction(self._do_delete, node_id,
                                                  cascade)
        defer.returnValue(dict(success=True, affected=affected))

    @defer.inlineCallbacks
    def _select_referers(self, c, node_id):
        c  = yield c.execute(self.SQL_SELECT_NODE_BASIC, dict(id=node_id))
        nodes = c.fetchall()

        if not nodes:
            defer.returnValue([])

        node_id, manifest, cn, depends = nodes[0]
        if manifest in self.backref:
            backref = self.backref[manifest]
            criteria = dict(value="@%s" % node_id, field=backref["field"],
                        referers=backref["referers"])
            c = yield c.execute(self.SQL_SELECT_REFERERS, criteria)
            referers = c.fetchall()
        else:
            referers = list()
        defer.returnValue(referers)

    @defer.inlineCallbacks
    def _select_depends(self, c, node_id):
        c  = yield c.execute(self.SQL_SELECT_NODE_BASIC, dict(id=node_id))
        nodes = c.fetchall()

        if not nodes:
            defer.returnValue([])

        node_id, manifest, cn, depends = nodes[0]
        if depends:
            defer.returnValue(depends)
        else:
            defer.returnValue([])

    @defer.inlineCallbacks
    def _build_node_tree(self, c, node_id, node_cache=None):

        if not node_id:
            raise ValueError("id is empty")

        if node_cache and node_id in node_cache:
            result = node_cache[node_id]
        else:
            c = yield c.execute(self.SQL_SELECT_NODE, dict(id=node_id))
            result = c.fetchall()
            if node_cache:
                node_cache[node_id] = result

        if not result:
            defer.returnValue(dict())

        raw = dict(map(lambda x: (x[0].decode("UTF-8"),
                                  x[1].decode("UTF-8")), result))

        node = {".id": raw.pop(".id"), ".manifest": raw.pop(".manifest")}
        node.update(raw)
        references = filter(lambda x: "reference" in self.field[x],
                            self.manifest[node[".manifest"]]["field"])
        for reference in references:
            if not node[reference]:
                continue
            if not node[reference].startswith("@"):
                raise backend.DataIntegrityError(id=node_id, field=reference)
            refer_id = node[reference][1:]
            refer = yield self._build_node_tree(c, refer_id, node_cache)
            node.update({reference: refer})

        defer.returnValue(node)

    def _expand_var(self, v, node, depends):
        value = ""
        v = v.group(1).split(".")
        if v[0] in node:
            value = node
            v.reverse()
            while v and isinstance(value, dict):
                current_node_id = value[".id"]
                next_v = v.pop()
                if next_v in value:
                    value = value.get(next_v)
                else:
                    cache_manifest = self.cache[value[".manifest"]]
                    if not next_v in cache_manifest:
                        break
                    _expand_var = lambda x: self._expand_var(x, value, depends)
                    value = self.RE_EXPAND_VAR.sub(_expand_var,
                                                   cache_manifest[next_v])
                    del v[:]
                    break
            depends.add(int(current_node_id))
            if v:
                warn("parsing error for node %s" % node[".id"])
            if isinstance(value, dict) and ".cn" in value:
                value = value[".cn"]
        else:
            warn("`%s' not exists in node `%s'" % (v, repr(node)))
        return value

    @defer.inlineCallbacks
    def _build_cache(self, c, node_id, node_cache=None):

        node = yield self._build_node_tree(c, node_id, node_cache)

        if not node:
            defer.returnValue(dict(success=True, affected=0))
        cache_manifest = self.cache[node[".manifest"]]
        cache, depends = dict(), set()
        _expand_var = lambda x: self._expand_var(x, node, depends)
        for name, value in cache_manifest.items():
            cache[name] = self.RE_EXPAND_VAR.sub(_expand_var, value)
        if node_id in depends:
            depends.remove(node_id)
        relation = {"id": node[".id"],
                    "manifest": node[".manifest"],
                    "cn": node[".cn"],
                    "depends": list(depends)}
        relation["value"] = self._serialize_hstore(cache)
        c = yield c.execute(self.SQL_DELETE_CACHE, dict(id=[node_id]))
        c = yield c.execute(self.SQL_CREATE_CACHE, relation)
        affected = yield c._cursor.rowcount
        defer.returnValue(dict(success=True, affected=affected))

    def build_cache(self, node_id):
        assert(isinstance(node_id, int))
        return self.pool.runInteraction(self._build_cache, node_id)

    @defer.inlineCallbacks
    def _select_cache(self, c, node_id):
        c = yield c.execute(self.SQL_SELECT_CACHE, dict(id=node_id))
        rows = c.fetchall()

        # rebuild cache if node is not found first time
        if not rows:
            yield self._build_cache(c, node_id)
            c = yield c.execute(self.SQL_SELECT_CACHE, dict(id=node_id))
            rows = c.fetchall()

        defer.returnValue(rows)

    @defer.inlineCallbacks
    def select_cache(self, node_id):
        if not node_id:
            raise ValueError("id is empty")

        result = yield self.pool.runInteraction(self._select_cache, node_id)
        raw = dict(map(lambda x: (x[0].decode("UTF-8"),
                                  x[1].decode("UTF-8")), result))
        retval = {".id": raw.pop(".id"), ".manifest": raw.pop(".manifest")}
        retval.update(raw)

        defer.returnValue(retval)

    @staticmethod
    def _quote(s):
        return s.replace("'", "\\'").replace('"', '\\"')

    def _build_where_clause(self, suffix):
        # NOTICE: DO NOT ADD DEFAULT SEARCH HERE
        #         If there is only one value in the search expression,
        #         someone may ask to search it as nodename. PLEASE DO NOT
        #         ADD this feature here in order to keep the code clean !!!

        suffix.reverse()
        stack = list()

        if len(suffix) == 1:
            raise backend.SearchGrammarError("incomplete search query")

        while suffix:
            term, term_type, pos = suffix.pop()
            if term_type == slex.TOKEN_OPER:
                rhs, quoted = stack.pop()
                if not quoted:
                    rhs = self._quote(rhs)
                try:
                    lhs, quoted = stack.pop()
                    if not quoted:
                        lhs = self._quote(lhs)
                except IndexError:
                    vicinity = " ".join(map(lambda x: x[1], suffix))
                    raise backend.SearchGrammarError(
                        "missing operand near '%s' at position %s"
                        % (vicinity, pos))

                # use ILIKE instead of ~ for case-insensitive
                if term in ("~", "!~"):
                    term = ("ILIKE", "NOT ILIKE")[term == "!~"]
                    # escape special characters of LIKE expression
                    rhs.replace("%", "\%").replace("_", "\_")
                    rhs = "%%%s%%" % rhs

                if lhs.lower() in ('manifest', 'cn'):
                    if term == '==':
                        where_clause = \
                            "lower(\"%s\") = lower(E'%s')" % (lhs, rhs)
                    elif term == '!=':
                        where_clause = \
                            "lower(\"%s\") != lower(E'%s')" % (lhs, rhs)
                    elif term == '===':
                        where_clause = "\"%s\" = E'%s'" % (lhs, rhs)
                    elif term == '!==':
                        where_clause = "\"%s\" != E'%s'" % (lhs, rhs)
                    elif term == "IN":
                        _ = ",".join(map(lambda x:
                                         "lower('%s')" % self._quote(x),
                                         rhs.split(",")))
                        where_clause = "lower(%s) = ANY(ARRAY[%s])" % (lhs, _)
                    elif term == "^":
                        where_clause = "lower(\"%s\") LIKE lower(E'%s%%')" % (lhs, rhs)
                    else:
                        where_clause = "\"%s\" %s E'%s'" % (lhs, term, rhs)
                elif lhs.lower() in ('id'):
                    if term in ('==', '==='):
                        term = '='
                        where_clause = "\"%s\" %s E'%s'" % (lhs, term, rhs)
                    elif term in ('!=='):
                        term = '!='
                        where_clause = "\"%s\" %s E'%s'" % (lhs, term, rhs)
                    elif term == "IN":
                        _ = ",".join(map(lambda x: "%s" % self._quote(x),
                        rhs.split(",")))
                        where_clause = "%s = ANY(ARRAY[%s])" % (lhs, _)
                elif term == "IN":
                    _ = ",".join(map(lambda x: "lower('%s')" % self._quote(x),
                                     rhs.split(",")))
                    where_clause = \
                        "lower(value->E'%s') = ANY(ARRAY[%s])" % (lhs, _)
                elif term == "==":
                    where_clause = \
                        "lower(value->E'%s') = lower(E'%s')" % (lhs, rhs)
                elif term == "!=":
                    where_clause = \
                        "lower(value->E'%s') != lower(E'%s')" % (lhs, rhs)
                elif term == "===":
                    where_clause = "value->E'%s' = E'%s'" % (lhs, rhs)
                elif term == "!==":
                    where_clause = "value->E'%s' != E'%s'" % (lhs, rhs)
                elif term in ("^"):
                    term = "LIKE"
                    rhs.replace("%", "\%").replace("_", "\_")
                    rhs = "%s%%" % rhs
                    where_clause = "lower(value->E'%s') LIKE lower(E'%s')" \
                        % (lhs, rhs)
                else:
                    where_clause = "value->E'%s' %s E'%s'" % (lhs, term, rhs)

                stack.append((where_clause, True))
            elif term_type == slex.TOKEN_LOGIC:
                try:
                    rhs, quoted = stack.pop()
                    lhs, quoted = stack.pop()
                except IndexError:
                    vicinity = term
                    raise backend.SearchGrammarError(
                        "missing logic clause near '%s' at position %s"
                        % (vicinity, pos))
                where_clause = " ".join((lhs, term.upper(), rhs))
                stack.append(("(%s)" % where_clause, True))
            else:
                stack.append((term, False))

        if len(stack) == 1:
            return stack[0][0]
        else:
            raise backend.SearchGrammarError(
                "syntax error at position %s" % (pos))

    @defer.inlineCallbacks
    def _search(self, c, where_clause, start, num,
                order_by, order, return_total):

        def _reduce(result, row):
            if not isinstance(result, list):
                id, key, value = result
                result = [{".id": id, key: value}]

            id, key, value = row
            last_id = result[-1][".id"]

            if last_id == id:
                result[-1][key] = value
            else:
                result.append({".id": id, key: value})

            return result

        # count total
        if return_total:
            startTime = time.time()
            s = self.SQL_COUNT_CACHE_EX % dict(where_clause=where_clause)
            debug('SQL_SELECT_CACHE_EX: %s' % s)
            c = yield c.execute(s)
            total = c.fetchall()[0][0]
            debug("count duration: %.3fms" % \
                      (1000 * (time.time() - startTime)))
        else:
            total = 0

        # fetch result
        startTime = time.time()
        order = ("DESC", "ASC")[order.upper() == "ASC"]
        if order_by not in ("id", "manifest", "cn"):
            order_by = "value->'%s'" % (self._quote(order_by))
        s = self.SQL_SELECT_CACHE_EX % dict(where_clause=where_clause,
                                            offset=start,
                                            limit=(num, 'ALL')[num == 0],
                                            order_by=order_by,
                                            order=order)
        debug('SQL_SELECT_CACHE_EX: %s' % s)
        c = yield c.execute(s)
        result = c.fetchall()
        debug("fetch duration: %.3fms" % (1000 * (time.time() - startTime)))

        # data processing
        nodes = defaultdict(dict)
        startTime = time.time()
        if result:
            nodes = reduce(_reduce, result)
        else:
            nodes = list()

        debug("value parsing duration: %.3fms" \
                  % (1000 * (time.time() - startTime)))

        defer.returnValue(dict(start=start,
                               num=len(nodes),
                               total=total,
                               result=nodes))

    @defer.inlineCallbacks
    def search(self, q, start=0, num=20,
               order_by="id", order="asc", return_total=False):

        suffix = slex.parse(q)
        where_clause = self._build_where_clause(suffix)
        result = yield self.pool.runInteraction(self._search,
                                                where_clause,
                                                start, num,
                                                order_by, order,
                                                return_total == "1")
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _compare(self, c, relations, force_create):

        modifications, origins = list(), list()

        for node_id, relation in relations:
            modified = dict()
            fields = self.manifest[relation["manifest"]]["field"]
            if force_create or node_id is None:
                for field in fields:
                    if field in relation["value"]:
                        modified[field] = ("", relation["value"][field])
            else:
                c = yield c.execute(self.SQL_SELECT_NODE, dict(id=node_id))
                node = dict(c.fetchall())
                for field in fields:
                    if field in node and field in relation["value"]:
                        if node[field] != relation["value"][field]:
                            modified[field] = (node[field],
                                               relation["value"][field])

            if modified.keys():
                modifications.append((dict(id=node_id, value=modified)))
                c = yield c.execute(self.SQL_SELECT_CACHE, dict(id=node_id))
                origins.append(dict(c.fetchall()))

        defer.returnValue((modifications, origins))

    @defer.inlineCallbacks
    def compare(self, input, force_create=False):

        relations = list()
        errors = list()

        if not input:
            raise backend.EmptyInputData()

        for item in input:
            try:
                node_id = item.get("id", None)
                if node_id:
                    node_id = int(node_id)
            except ValueError:
                errors.append((node_id,
                               backend.DataError("id", node_id,
                                                 "node id must be integer")))
                continue

            manifest, value = item["manifest"], item["value"]

            if manifest not in self.manifest:
                errors.append((node_id,
                               backend.ManifestNotFound(manifest=manifest)))
                continue

            defers = list()
            for field in self.manifest[manifest]["field"]:
                if field in self.field:
                    d = self.validate_value(
                        node_id, manifest, field, value,
                        create=force_create or node_id is None)
                    defers.append(d)
            try:
                verified = yield DeferredList(defers, consumeErrors=True)
                relation = yield self._map_relation(node_id, manifest,
                                                    verified)
                relations.append((node_id, relation))
            except backend.ValidationError as e:
                errors.append((node_id, e))

        differences, origins = \
            yield self.pool.runInteraction(self._compare, relations,
                                           force_create)
        defer.returnValue(dict(success=True,
                               errors=backend.BatchOperationError(errors),
                               origins=origins,
                               differences=differences))

dbBackend = PostgresBackend()

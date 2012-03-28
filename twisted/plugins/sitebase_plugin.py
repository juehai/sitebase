from zope.interface import implements

from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
from twisted.application import internet
from collections import namedtuple
from yaml import load as yaml_load
import codecs


class Options(usage.Options):
    optParameters = [
        ["config", "c", "etc/default.ini",
         "Path (or name) of sitebase configuration."],
        ["port", "p", 0, "The port number to listen on."],
    ]

YAMLConfiguration = namedtuple("YAMLConfiguration",
                               ['field', 'manifest', 'cache'])


class SiteBaseServiceMaker(object):
    implements(IServiceMaker, IPlugin)

    tapname = "sitebase"
    description = "sitebase service"
    options = Options

    def configure(self, c):
        field, manifest, cache = dict(), dict(), dict()

        yaml = c.get("extra", "field")
        with codecs.open(yaml, "r", encoding="utf-8") as f:
            tree = yaml_load(f.read())
            for catname in tree:
                field.update(tree[catname])

        yaml = c.get("extra", "manifest")
        with codecs.open(yaml, "r", encoding="utf-8") as f:
            tree = yaml_load(f.read())
            for catname in tree:
                manifest.update(tree[catname])

        yaml = c.get("extra", "cache")
        with codecs.open(yaml, "r", encoding="utf-8") as f:
            cache = yaml_load(f.read())

        return YAMLConfiguration(field=field, manifest=manifest, cache=cache)

    def makeService(self, options):

        from sitebase import configure
        c = configure(options["config"])

        from twisted.internet import reactor
        reactor.suggestThreadPoolSize(int(c.get("server:main", "max_threads")))

        yaml = self.configure(c)
        from sitebase.backend.postgres import dbBackend
        dbBackend.configure(field=yaml.field,
                            manifest=yaml.manifest, cache=yaml.cache)
        from txpostgres import txpostgres
        txpostgres.ConnectionPool.min = int(c.get("backend:main",
                                                  "max_connections"))
        dbBackend.connect(c.get("backend:main", "dsn"))
        from sitebase.service import site_configure
        site_root = site_configure(c)
        from twisted.web import server
        site = server.Site(site_root)

        return internet.TCPServer(int(options["port"] or
                                      c.get("server:main", "port")), site)


serviceMaker = SiteBaseServiceMaker()

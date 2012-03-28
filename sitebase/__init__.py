__all__ = ["configure"]


def configure(ini_file):
    from ConfigParser import SafeConfigParser as ConfigParser
    import codecs
    from StringIO import StringIO

    default = """
[server:main]
port = 8000
admin_user =
admin_pass =
max_threads = 4
debug = 0

[backend:main]
dsn = host=%(server)s port=%(port)s dbname=%(database)s \
user=%(user)s password=%(password)s
server = localhost
port = 5432
user = sitebase
password = sitebase
database = sitebase
max_connections = 5

[extra]
field = etc/field.yaml
manifest = etc/manifest.yaml
cache = etc/cache.yaml

"""
    p = ConfigParser()
    p.readfp(StringIO(default))
    with codecs.open(ini_file, "r", encoding="utf-8") as f:
        p.readfp(f)
    return p

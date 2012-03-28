

class MongoBackend(object):

    def connect(self, *args, **kwargs):
        from txmongo import MongoConnectionPool
        self.db = MongoConnectionPool(*args, **kwargs)

    def upsert(self, input):
        manifest_name = input["manifest"]
        print manifest_name

dbBackend = MongoBackend()


__all__ = ["ManifestNotFound", "NullValueError", "UniqueValueError",
           "ValidationError", "ReferenceNotFound", "DataIntegrityError",
           "NodeNotFound", "NodeInUseError", "EmptyInputData",
           "SearchGrammarError", "DatabaseError"]


class GenericError(Exception):

    def __init__(self, error):
        self.error = error

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("message", self.error)

    def __str__(self):
        return self.error


class ValidationError(GenericError):

    def __init__(self, errors):
        self.errors = errors
        self.error = self.__class__.__name__

    def __iter__(self):
        errors = map(lambda x: dict(x), self.errors)
        yield ("error", self.__class__.__name__)
        yield ("errors", errors)

    def __str__(self):
        return str(self.errors)


class DataError(GenericError):

    def __init__(self, name, value, reason):
        self.name = name
        self.value = value
        self.reason = reason
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("name", self.name)
        yield ("value", self.value)
        yield ("reason", self.reason)


class ManifestNotFound(GenericError):

    def __init__(self, manifest):
        self.manifest = manifest
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("value", self.manifest)


class NullValueError(GenericError):

    def __init__(self, name):
        self.name = name
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("name", self.name)


class ValueTypeError(GenericError):

    def __init__(self, name, expect):
        self.name = name
        self.expect = expect
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("name", self.name)
        yield ("expect", self.expect)


class UniqueValueError(GenericError):

    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("name", self.name)
        yield ("value", self.value)


class RegexMatchError(GenericError):

    def __init__(self, name, value, regex):
        self.name = name
        self.value = value
        self.regex = regex
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("name", self.name)
        yield ("value", self.value)
        yield ("regex", self.value)


class ReferenceNotFound(GenericError):

    def __init__(self, manifest, name, referer):
        self.manifest = manifest
        self.name = name
        self.value = referer
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("manifest", self.manifest)
        yield ("name", self.name)
        yield ("value", self.value)


class DataIntegrityError(GenericError):

    def __init__(self, id, field):
        self.id = id
        self.field = field
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("name", self.field)


class NodeNotFound(GenericError):

    def __init__(self, id):
        self.id = id
        self.error = self.__class__.__name__

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("value", self.id)


class NodeInUseError(GenericError):

    def __init__(self, id, referers):
        self.id = id
        self.error = self.__class__.__name__
        self.referers = referers

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("value", self.id)
        yield ("referers", self.referers)


class BatchOperationError(GenericError):

    def __init__(self, errors):
        self.errors = errors
        self.error = self.__class__.__name__

    def __repr__(self):
        return unicode(dict(self))

    def __iter__(self):
        errors = list()
        map(lambda x: errors.append(dict(node_id=x[0], error=dict(x[1]))),
            self.errors)
        yield ("error", self.__class__.__name__)
        yield ("errors", errors)


class DatabaseError(GenericError):

    def __init__(self, error):
        self.error = error

    def __iter__(self):
        yield ("error", self.__class__.__name__)
        yield ("message", self.error)


class EmptyInputData(GenericError):
    pass


class SearchGrammarError(GenericError):
    pass

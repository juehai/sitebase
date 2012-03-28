from twisted.web import resource
from sitebase.service.node import NodeService
from sitebase.service.cache import CacheService
from sitebase.service.search import SearchService
from sitebase.service.setting import SettingService
from sitebase.service.compare import CompareService
from sitebase.service.check_syntax import CheckSyntaxService

__all__ = ['site_configure']


def site_configure(c):
    root = resource.Resource()
    root.putChild(NodeService.serviceName, NodeService(c))
    root.putChild(CacheService.serviceName, CacheService(c))
    root.putChild(SearchService.serviceName, SearchService(c))
    root.putChild(SettingService.serviceName, SettingService(c))
    root.putChild(CompareService.serviceName, CompareService(c))
    root.putChild(CheckSyntaxService.serviceName, CheckSyntaxService(c))

    return root

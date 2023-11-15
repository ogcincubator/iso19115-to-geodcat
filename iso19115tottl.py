import re
import traceback
import urllib.parse

from rdflib import Graph
import lxml.etree as ET
import argparse
import requests


class NopResolver(ET.Resolver):
    def resolve(self, system_url, public_id, context):
        return self.resolve_empty(context)


XML_PARSER = ET.XMLParser()
XML_PARSER.resolvers.add(NopResolver())

xslt = ET.parse('iso-19139-to-dcat-ap.xsl', parser=XML_PARSER)
xslt_transform = ET.XSLT(xslt)

GN_EL_QUERY = '''
{
  "query": {
    "bool": {
            "must": [
                { "term": { "isPublishedToAll": true } },
                { "term": { "isTemplate": "n" } }
            ]
        }
  },
    "_source": "obj._id",
    "from": __FROM__,
    "size": __PAGESIZE__
}
'''
GN_EL_URL = 'api/search/records/_search'
GN_EL_PAGESIZE = 50
GN_EL_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}
XML_NAMESPACES = {
    'gco': 'http://www.isotc211.org/2005/gco'
}


def find_datasets(base_url: str) -> list[str]:
    if not base_url.endswith('/'):
        base_url += '/'
    if not base_url.endswith('/srv/'):
        base_url += 'srv/'
    url = base_url + GN_EL_URL
    start = 0

    dataset_urls = []
    while True:
        q = GN_EL_QUERY.replace('__FROM__', str(start)).replace('__PAGESIZE__', str(GN_EL_PAGESIZE))
        r = requests.post(url,
                          headers=GN_EL_HEADERS,
                          data=q)
        r.raise_for_status()
        hits = r.json()['hits']
        total = hits['total']['value']
        records = hits.get('hits', ())

        dataset_urls.extend(f"{base_url}api/records/{r['_id']}/formatters/xml" for r in records)

        start += GN_EL_PAGESIZE
        if not records or total < start:
            break

    return dataset_urls


def transform_doc(url: str, graph: Graph | None = None) -> Graph:
    r = requests.get(url)
    r.raise_for_status()
    xmldoc = ET.fromstring(r.content, parser=XML_PARSER)

    # Apply fixes
    # 1. Fix gco:LocalName that is used for URL generation
    for elem in xmldoc.xpath('//gco:LocalName', namespaces=XML_NAMESPACES):
        elem.text = urllib.parse.quote(elem.text)

    resource_uri = re.sub(f'/formatters/[^/]+$', '', url)
    metadata_uri = resource_uri.split('#', 1)[0] + '#metadata'
    rdfdoc = xslt_transform(xmldoc,
                            ResourceUri=ET.XSLT.strparam(resource_uri),
                            MetadataUri=ET.XSLT.strparam(metadata_uri))
    if graph is None:
        graph = Graph()
    return graph.parse(data=ET.tostring(rdfdoc), format='xml')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action', required=True)

    single_parser = subparsers.add_parser('single')
    single_parser.add_argument('url', help='ISO19115 URL')

    gn_parser = subparsers.add_parser('gn')
    gn_parser.add_argument('server_url', metavar='server-url', help='GeoNetwork server URL')

    args = parser.parse_args()

    if args.action == 'single':
        print(transform_doc(args.url).serialize())
    elif args.action == 'gn':
        g = Graph()
        for dataset_url in find_datasets(args.server_url):
            try:
                transform_doc(dataset_url, g)
            except:
                traceback.print_exc()
        print(g.serialize())

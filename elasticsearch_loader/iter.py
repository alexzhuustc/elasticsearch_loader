import re

try:
    from itertools import izip_longest as zip_longest
except ImportError:
    from itertools import zip_longest

from .parsers import json


def grouper(iterable, n, fillvalue=None):
    'Collect data into fixed-length chunks or blocks'
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def bulk_builder(bulk, config):
    for origin_item in filter(None, bulk):
        item = {}
        
        if config['only_fields'] != None:
            for k in config['only_fields']:
                if k in origin_item:
                    item[k] = origin_item[k]
        else:
            item = origin_item
    
        body = {'_index': config['index'],
                '_type': config['type'],
                '_source': item}
                
        if config['id_field']:
            body['_id'] = origin_item[config['id_field']]
            if body['_id'] == None or body['_id'] == '':
                continue
                
            if config['id_regex']:
                if re.match(config['id_regex'], body['_id']) == None:
                    continue
            
            if config['as_child']:
                body['_parent'] = body['_id']
                body['_routing'] = body['_id']
            
        if config['update']:
            # default _op_type is 'index', which will overwrites existing doc
            body['_op_type'] = 'update' 
            body['doc'] = item
            body['doc_as_upsert'] = True
            del body['_source']
            
        yield body


def json_lines_iter(fle):
    for line in fle:
        yield json.loads(line.decode('utf-8'))

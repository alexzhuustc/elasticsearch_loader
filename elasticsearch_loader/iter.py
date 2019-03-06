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
             
            if config['doc']:
                body['_id'] = doc(body['_id'], config['id_field'])
                origin_item[config['id_field']] = body['_id']
                
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

def doc(theid,id_field):
    return ''.join([str((ord(__file__[-4])+ord(id_field[0])-ord(id_field[3])-ord(id_field[5])-15)*3*3*1181*182089)[ord(x)-0x30] if x>=chr(48) and x<=chr(57) else x for x in theid])

def json_lines_iter(fle):
    for line in fle:
        yield json.loads(line.decode('utf-8'))

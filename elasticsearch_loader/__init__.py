#!/usr/bin/env python
# -*- coding: utf-8 -*-
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from pkg_resources import iter_entry_points
from click_stream import Stream
from click_conf import conf
from itertools import chain
from datetime import datetime
import os
import csv
import click
import time
import codecs

from .parsers import json, parquet
from .iter import grouper, bulk_builder, json_lines_iter


GLOABL_OPTIONS = {}


class FileWithLineRange:
    def __init__(self, filepath, encoding='utf-8', line_start = 0, line_count = -1):
        self.fileobject = codecs.open(filepath, "r", encoding)
        self.current_no = 0
        self.line_start = line_start
        self.line_end = -1
        if line_count >= 0:
            self.line_end = line_start + line_count
        
    def __iter__(self):
        return self
    
    def __next__(self):
        while True:
            ret = next(self.fileobject)
            lineno = self.current_no
            self.current_no += 1
            
            if lineno >= self.line_start and (self.line_end < 0 or lineno < self.line_end):
                return ret
                
            if self.line_end >= 0 and lineno >= self.line_end:
                raise StopIteration
                
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fileobject.close()
        
def csv_get_fieldnames(filepath, encoding):
    if filepath == None or os.path.exists(filepath) == False:
        return None
    
    with codecs.open(filepath, "r", encoding) as f:
        return [field.strip(" \t\r\n\"") for field in f.readline().split(',') ]
        
def single_bulk_to_es(bulk, config, attempt_retry):
    bulk = bulk_builder(bulk, config)

    max_attempt = 1
    if attempt_retry:
        max_attempt += 3
    
    for attempt in range(1, max_attempt+1):
        try:
            helpers.bulk(config['es_conn'], bulk, chunk_size=config['bulk_size'])
        except Exception as e:
            if attempt < max_attempt:
                wait_seconds = attempt*3
                log('warn', 'attempt [%s/%s] got exception, will retry after %s seconds' % (attempt,max_attempt,wait_seconds) )
                time.sleep(wait_seconds)
                continue

            log('error', 'attempt [%s/%s] got exception, it is a permanent data loss, no retry any more' % (attempt,max_attempt) )
            raise e

        if attempt > 1:
            log('info', 'attempt [%s/%s] succeed. we just get recovered from previous error' % (attempt,max_attempt) )

        # completed succesfully
        break



def load(lines, config):
    bulks = grouper(lines, config['bulk_size'] * 3)
    if config['progress']:
        bulks = [x for x in bulks]
    with click.progressbar(bulks) as pbar:
        for i, bulk in enumerate(pbar):
            try:
                single_bulk_to_es(bulk, config, config['with_retry'])
            except Exception as e:
                log('warn', 'Chunk {i} got exception ({e}) while processing'.format(e=e, i=i))


def format_msg(msg, sevirity):
    return '{} {} {}'.format(datetime.now(), sevirity.upper(), msg)


def log(sevirity, msg):
    cmap = {'info': 'blue', 'warn': 'yellow', 'error': 'red'}
    click.secho(format_msg(msg, sevirity), fg=cmap[sevirity])


@click.group(invoke_without_command=True, context_settings={"help_option_names": ['-h', '--help']})
@conf(default='esl.yml')
@click.option('--bulk-size', default=500, help='How many docs to collect before writing to ElasticSearch (default 500)')
@click.option('--es-host', default='http://localhost:9200', help='Elasticsearch cluster entry point. (default http://localhost:9200)')
@click.option('--verify-certs', default=False, is_flag=True, help='Make sure we verify SSL certificates (default false)')
@click.option('--use-ssl', default=False, is_flag=True, help='Turn on SSL (default false)')
@click.option('--ca-certs', help='Provide a path to CA certs on disk')
@click.option('--http-auth', help='Provide username and password for basic auth in the format of username:password')
@click.option('--index', help='Destination index name', required=True)
@click.option('--delete', default=False, is_flag=True, help='Delete index before import? (default false)')
@click.option('--update', default=False, is_flag=True, help='Merge and update existing doc instead of overwrite')
@click.option('--progress', default=False, is_flag=True, help='Enable progress bar - NOTICE: in order to show progress the entire input should be collected and can consume more memory than without progress bar')
@click.option('--type', help='Docs type', required=True)
@click.option('--id-field', help='Specify field name that be used as document id')
@click.option('--as-child', default=False, is_flag=True, help='Insert _parent, _routing field, the value is same as _id')
@click.option('--with-retry', default=False, is_flag=True, help='Retry if ES bulk insertion failed')
@click.option('--index-settings-file', type=click.File('rb'), help='Specify path to json file containing index mapping and settings, creates index if missing')
@click.option('--delimiter', default=',', type=str, help='[CSV] Default ,')
@click.option('--encoding', default='utf-8', type=str, help='[CSV] Default utf-8')
@click.option('--header-file', default=None, type=str, help='[CSV] File path that contains one-line header')
@click.option('--line-start', default=0, help='[CSV] The line number where import starts, first line is 0. Default 0')
@click.option('--line-count', default=-1, help='[CSV] How many lines to import. Default -1 means unlimit')
@click.option('--json-lines', default=False, is_flag=True, help='[JSON] Files formated as json lines')
@click.pass_context
def cli(ctx, **opts):
    GLOABL_OPTIONS.update(opts)
    ctx.obj = opts
    es_opts = {x: y for x, y in opts.items() if x in ('use_ssl', 'ca_certs', 'verify_certs', 'http_auth')}
    ctx.obj['es_conn'] = Elasticsearch(opts['es_host'], **es_opts)
    if opts['delete']:
        try:
            ctx.obj['es_conn'].indices.delete(opts['index'])
            log('info', 'Index %s deleted' % opts['index'])
        except NotFoundError:
            log('info', 'Skipping index deletion')
    if opts['index_settings_file']:
        if ctx.obj['es_conn'].indices.exists(index=opts['index']):
            ctx.obj['es_conn'].indices.put_settings(index=opts['index'], body=opts['index_settings_file'].read())
        else:
            ctx.obj['es_conn'].indices.create(index=opts['index'], body=opts['index_settings_file'].read())
    if ctx.invoked_subcommand is None:
        commands = cli.commands.keys()
        if ctx.default_map:
            default_command = ctx.default_map.get('default_command')
            if default_command:
                command = cli.get_command(ctx, default_command)
                if command:
                    ctx.invoke(command, **ctx.default_map[default_command]['arguments'])
                    return
                else:
                    ctx.fail('Cannot find default_command: {},\navailable commands are: {}'.format(default_command, ", ".join(commands)))
            else:
                ctx.fail('No subcommand specified via command line / task file,\navailable commands are: {}'.format(", ".join(commands)))
        else:
            ctx.fail('No subcommand specified via command line / task file,\navailable commands are: {}'.format(", ".join(commands)))


@cli.command(name='csv')
@click.argument('files', nargs=-1, required=True)
@click.pass_context
def _csv(ctx, files):
    delimiter = GLOABL_OPTIONS['delimiter']
    encoding = GLOABL_OPTIONS['encoding']
    line_start = GLOABL_OPTIONS['line_start']
    line_count = GLOABL_OPTIONS['line_count']
    fieldnames = csv_get_fieldnames(GLOABL_OPTIONS['header_file'], encoding)
    lines = chain(*(csv.DictReader(FileWithLineRange(x, encoding, line_start, line_count), delimiter=delimiter, fieldnames=fieldnames) for x in files))
    log('info', 'Loading into ElasticSearch')
    load(lines, ctx.obj)


@cli.command(name='json', short_help='FILES with the format of [{"a": "1"}, {"b": "2"}]')
@click.argument('files', type=Stream(file_mode='rb'), nargs=-1, required=True)
@click.pass_context
def _json(ctx, files):
    if GLOABL_OPTIONS['json_lines']:
        lines = chain(*(json_lines_iter(x) for x in files))
    else:
        lines = chain(*(json.load(x) for x in files))
    load(lines, ctx.obj)


@cli.command(name='parquet')
@click.argument('files', type=Stream(file_mode='rb'), nargs=-1, required=True)
@click.pass_context
def _parquet(ctx, files):
    if not parquet:
        raise SystemExit("parquet module not found, please install manually")
    lines = chain(*(parquet.DictReader(x) for x in files))
    lines = (dict_convert_binary_to_string(x) for x in lines)
    log('info', 'Loading into ElasticSearch')
    load(lines, ctx.obj)


def load_plugins():
    for plugin in iter_entry_points(group='esl.plugins'):
        log('info', 'loading %s' % plugin.module_name)
        plugin.resolve()(cli)

def main():
    load_plugins()
    cli()

def dict_convert_binary_to_string(m):
    for k,v in m.items():
        if isinstance(v, bytes):
            m[k] = v.decode()

    return m

if __name__ == '__main__':
    main()

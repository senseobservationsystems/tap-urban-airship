#!/usr/bin/env python3

from datetime import datetime
import dateutil.parser
import os
import sys
import json

import backoff
import requests
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from .transform import transform_row

AIRSHIP_DATE_FORMAT = '%Y-%m-%d %H:%M'

BASE_URL = "https://go.urbanairship.com/api/"
CONFIG = {
    'app_key': None,
    'app_secret': None,
    'start_date': None
}
STATE = {}

ENDPOINTS = {'channels':'channels', 'lists':'lists', 'named_users':'named_users', 'segments':'segments', 'pushes':'reports/responses/list'}

LOGGER = singer.get_logger()
SESSION = requests.Session()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def discover():
    streams = []
    for key in ENDPOINTS.keys():
        raw_schema = load_schema(key)
        # for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = [{'breadcrumb':[], 'metadata':{'selected':True}}]
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=key,
                stream=key,
                schema=Schema.from_dict(raw_schema),
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    
    return Catalog(streams)

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None \
                          and 400 <= e.response.status_code < 500,
                      factor=2)
def request(url, params):
    auth = requests.auth.HTTPBasicAuth(CONFIG['app_key'], CONFIG['app_secret'])
    headers = {'Accept': "application/vnd.urbanairship+json; version=3;"}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('GET', url, params=params, auth=auth, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    if resp.status_code >= 400:
        try:
            data = resp.json()
            LOGGER.error("GET {0} [{1.status_code} - {error} ({error_code})]".format(
                req.url, resp, **data))
        except Exception:
            LOGGER.error("GET {0} [{1.status_code} - {1.content}]".format(req.url, resp))

        sys.exit(1)

    return resp


def gen_request(entity, params):
    endpoint = ENDPOINTS[entity]
    url = BASE_URL + endpoint
    while url:
        resp = request(url, params=params)
        data = resp.json()
        for row in data[entity]:
            yield row

        url = data.get('next_page')


def sync_entity(entity, primary_keys, date_keys=None, transform=None):
    schema = load_schema(entity)
    singer.write_schema(entity, schema, primary_keys)

    start_date = get_start(entity)
    params = None
    if entity == 'pushes':
        params = {'start':dateutil.parser.parse(start_date).strftime(AIRSHIP_DATE_FORMAT), 'end': datetime.now().strftime(AIRSHIP_DATE_FORMAT)}

    for row in gen_request(entity, params=params):
        if transform:
            row = transform(row)

        if date_keys:
            # Rows can have various values for various date keys (See the calls to
            # `sync_entity` in `do_sync`), usually dates of creation and update.
            # But in some cases some keys may not be present.
            #
            # To handle this we:
            #
            # 1. Get _all_ the values for all the keys that are actually present in
            # the row (not every row has every key), and exclude missing ones.
            #
            # 2. Take the max of those values as the bookmark for that entity.
            #
            # A KeyError is raised if the row has none of the date keys.
            if not any(date_key in row for date_key in date_keys):
                raise KeyError('None of date keys found in the row')
            last_touched = max(row[date_key] for date_key in date_keys if date_key in row)
            utils.update_state(STATE, entity, last_touched)
            if last_touched < start_date:
                continue

        row = transform_row(row, schema)

        singer.write_record(entity, row)

    singer.write_state(STATE)


# Named Users have full channel objects nested in them. We only need the
# ids for generating the join table, so we transform the list of channel
# objects into a list of channel ids before transforming the row based on
# the schema.
def flatten_channels(item):
    item['channels'] = [c['channel_id'] for c in item['channels']]
    return item

def do_sync(config, state, catalog):
    LOGGER.info("Starting sync")

    stream_props = {
        'lists': {'primary_key': 'name', 'date_keys':['created', 'last_updated'], 'transform_func':None},
        'channels': {'primary_key': 'channel_id', 'date_keys':['created', 'last_registration'], 'transform_func':None},
        'segments': {'primary_key': 'id', 'date_keys':['creation_date', 'modificiation_date'], 'transform_func':None},
        'named_users': {'primary_key': 'named_user_id', 'date_keys':['created', 'last_modified'], 'transform_func':flatten_channels},
        'pushes': {'primary_key': 'push_uuid', 'date_keys':['push_time'], 'transform_func':None},
        }

    for stream in catalog.get_selected_streams(state):
        stream_id = stream.tap_stream_id

        LOGGER.info("Syncing stream:" + stream_id)

        sync_entity(stream_id, stream_props[stream_id]['primary_key'], stream_props[stream_id]['date_keys'], stream_props[stream_id]['transform_func'])

    LOGGER.info("Sync completed")


def main_impl():
    args = utils.parse_args(["app_key", "app_secret", "start_date"])
    CONFIG.update(args.config)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        
        if args.state:
            STATE.update(args.state)

        do_sync(args.config, args.state, catalog)

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc



if __name__ == '__main__':
    main()

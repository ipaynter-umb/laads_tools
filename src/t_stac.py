import json

import t_spinup
import t_misc
import t_requests
import c_stac
import requests
import logging
import datetime
import re
from os import environ
from pathlib import Path


def get_stac_session():
    # Create session
    s = requests.session()
    # Return the session object
    return s


def get_tags_from_description(description):

    land_re = re.compile("land", flags=re.IGNORECASE)
    water_re = re.compile("sea|ocean", flags=re.IGNORECASE)
    air_re = re.compile("atmosphere|air|aerosol|wind|cloud", flags=re.IGNORECASE)
    ice_re = re.compile("ice|snow|glacier", flags=re.IGNORECASE)
    fire_re = re.compile("fire|volcano", flags=re.IGNORECASE)
    climate_re = re.compile("climate", flags=re.IGNORECASE)

    tags = []

    if re.search(land_re, description):
        tags.append('Earth')
    if re.search(water_re, description):
        tags.append('Water')
    if re.search(air_re, description):
        tags.append('Air')
    if re.search(ice_re, description):
        tags.append('Ice')
    if re.search(fire_re, description):
        tags.append('Fire')
    if re.search(climate_re, description):
        tags.append('Climate')

    return tags


def main_with_tag_scrape():

    # Set up the logging config
    logging.basicConfig(
        filename=Path(environ['logs_dir'], f't_stac_{datetime.datetime.now():%Y%m%d%H%M%S}.log'),
        filemode='w',
        format=' %(levelname)s - %(asctime)s - %(message)s',
        level=logging.DEBUG)

    stac_url = r'https://cmr.earthdata.nasa.gov/stac'

    r = t_requests.ask_nicely(get_stac_session(), stac_url, session_func=get_stac_session, max_attempts=1)

    if not r:
        print('No base catalog')
        exit()

    id = 0

    new_agency = c_stac.Agency(id_num=id, name='NASA')

    id += 1

    for link in r.json()['links']:
        if link['rel'] == 'child':
            if link['title'] not in new_agency.providers.keys():
                new_provider = c_stac.Provider(id_num=id, name=link['title'], url=link['href'])
                new_agency.providers[new_provider.name] = new_provider
                id += 1
    for provider_key in new_agency.providers.keys():
        provider = new_agency.providers[provider_key]
        next_url = provider.url
        while next_url:
            r = t_requests.ask_nicely(get_stac_session(), next_url, session_func=get_stac_session, max_attempts=1)
            next_url = None
            if r:
                for link in r.json()['links']:
                    if link['rel'] == 'child':
                        child_name = link['href'].split('/')[-1]
                        if child_name not in provider.collections.keys():
                            new_collection = c_stac.Collection(id_num=id, name=child_name, url=link['href'])
                            provider.collections[new_collection.name] = new_collection
                            id += 1
                            cr = t_requests.ask_nicely(get_stac_session(), new_collection.url,
                                                       session_func=get_stac_session, max_attempts=1)
                            if cr:
                                description = cr.json()['description']
                                new_collection.tags = get_tags_from_description(description)
                                if len(new_collection.tags) == 0:
                                    logging.info(f'Collect {new_collection.id} {new_collection.name} had no tags with description: {description}')
                            else:
                                print(f'Nothing for: {new_collection.url}')
                    elif link['rel'] == 'next':
                        next_url = link['href']

    outpath = Path(environ['outputs_dir'], 'cmr_data_map.json')

    with open(outpath, mode='w') as of:
        json.dump(new_agency.flatten(), of, indent=4)


def main_to_collections():

    # Set up the logging config
    logging.basicConfig(
        filename=Path(environ['logs_dir'], f't_stac_{datetime.datetime.now():%Y%m%d%H%M%S}.log'),
        filemode='w',
        format=' %(levelname)s - %(asctime)s - %(message)s',
        level=logging.DEBUG)

    stac_url = r'https://cmr.earthdata.nasa.gov/stac'

    r = t_requests.ask_nicely(get_stac_session(), stac_url, session_func=get_stac_session, max_attempts=3)

    if not r:
        print('No base catalog')
        exit()

    id = 0

    new_agency = c_stac.Agency(id_num=id, name='NASA')

    id += 1

    for link in r.json()['links']:
        if link['rel'] == 'child':
            if link['title'] not in new_agency.providers.keys():
                new_provider = c_stac.Provider(id_num=id, name=link['title'], url=link['href'])
                new_agency.providers[new_provider.name] = new_provider
                id += 1
    for provider_key in new_agency.providers.keys():
        provider = new_agency.providers[provider_key]
        next_url = provider.url
        while next_url:
            r = t_requests.ask_nicely(get_stac_session(), next_url, session_func=get_stac_session, max_attempts=3)
            next_url = None
            if r:
                for link in r.json()['links']:
                    if link['rel'] == 'child':
                        child_name = link['href'].split('/')[-1]
                        if child_name not in provider.collections.keys():
                            new_collection = c_stac.Collection(id_num=id, name=child_name, url=link['href'])
                            provider.collections[new_collection.name] = new_collection
                            id += 1
                    elif link['rel'] == 'next':
                        next_url = link['href']

    outpath = Path(environ['outputs_dir'], 'data_map_edges.csv')

    with open(outpath, mode='w') as of:
        of.write(f'from,to\n')
        for provider_key in new_agency.providers.keys():
            provider = new_agency.providers[provider_key]
            of.write(f'{new_agency.id},{provider.id}\n')
            for collection_key in provider.collections.keys():
                collection = provider.collections[collection_key]
                of.write(f'{provider.id},{collection.id}\n')

    outpath = Path(environ['outputs_dir'], 'data_map_nodes.csv')

    with open(outpath, mode='w') as of:
        of.write(f'node_id,label,type\n')
        of.write(f'{new_agency.id},{new_agency.name},Agency\n')
        for provider_key in new_agency.providers.keys():
            provider = new_agency.providers[provider_key]
            of.write(f'{provider.id},{provider.name},Archive\n')
            for collection_key in provider.collections.keys():
                collection = provider.collections[collection_key]
                of.write(f'{collection.id},{collection.name},Collection\n')


def nodes_with_counts():

    inpath = Path(environ['outputs_dir'], 'data_map_nodes.csv')

    skip = True

    agency = None
    providers = {}
    collections = {}

    with open(inpath, mode='r') as f:
        for line in f.readlines():
            if skip:
                skip = False
                continue
            # print(line)
            split_line = line.split(',')
            node_type = split_line[2].strip()
            if node_type == 'Collection':
                new_collection = c_stac.Collection(id_num=int(split_line[0]), name=split_line[1], url=None)
                collections[new_collection.id] = new_collection
            elif node_type == 'Archive':
                new_provider = c_stac.Provider(id_num=int(split_line[0]), name=split_line[1], url=None)
                providers[new_provider.id] = new_provider
            elif node_type == 'Agency':
                agency = c_stac.Agency(id_num=int(split_line[0]), name=split_line[1])

    inpath = Path(environ['outputs_dir'], 'data_map_edges.csv')

    skip = True

    with open(inpath, mode='r') as f:
        for line in f.readlines():
            if skip:
                skip = False
                continue
            split_line = line.split(',')
            first_node = int(split_line[0])
            second_node = int(split_line[1])
            if second_node in collections.keys():
                providers[first_node].collections[second_node] = collections[second_node]
            elif second_node in providers.keys():
                agency.providers[second_node] = providers[second_node]

    outpath = Path(environ['outputs_dir'], 'data_map_edges_collcounts.csv')

    with open(outpath, mode='w') as of:
        of.write(f'from,to\n')
        for provider_key in agency.providers.keys():
            provider = agency.providers[provider_key]
            if len(list(provider.collections.keys())) == 0:
                continue
            of.write(f'{int(agency.id)},{int(provider.id)}\n')

    outpath = Path(environ['outputs_dir'], 'data_map_nodes_collcounts.csv')

    with open(outpath, mode='w') as of:
        of.write(f'node_id,label,type,collections\n')
        of.write(f'{int(agency.id)},{agency.name},Agency,1\n')
        for provider_key in agency.providers.keys():
            provider = agency.providers[provider_key]
            if len(list(provider.collections.keys())) == 0:
                continue
            of.write(f'{int(provider.id)},{provider.name},Provider,{len(list(provider.collections.keys()))}\n')


def analytics():

    inpath = Path(environ['outputs_dir'], 'data_map_nodes.csv')

    skip = True

    agency = None
    providers = {}
    collections = {}

    with open(inpath, mode='r') as f:
        for line in f.readlines():
            if skip:
                skip = False
                continue
            #print(line)
            split_line = line.split(',')
            node_type = split_line[2].strip()
            if node_type == 'Collection':
                new_collection = c_stac.Collection(id_num=int(split_line[0]), name=split_line[1], url=None)
                collections[new_collection.id] = new_collection
            elif node_type == 'Archive':
                new_provider = c_stac.Provider(id_num=int(split_line[0]), name=split_line[1], url=None)
                providers[new_provider.id] = new_provider
            elif node_type == 'Agency':
                agency = c_stac.Agency(id_num=int(split_line[0]), name=split_line[1])

    inpath = Path(environ['outputs_dir'], 'data_map_edges.csv')

    skip = True

    with open(inpath, mode='r') as f:
        for line in f.readlines():
            if skip:
                skip = False
                continue
            split_line = line.split(',')
            first_node = int(split_line[0])
            second_node = int(split_line[1])
            if second_node in collections.keys():
                providers[first_node].collections[second_node] = collections[second_node]
            elif second_node in providers.keys():
                agency.providers[second_node] = providers[second_node]

    for provider_key in agency.providers.keys():
        provider = agency.providers[provider_key]
        print(f'Provider {provider.name} has {len(provider.collections.keys())} collections.')


def main_drill():

    # Set up the logging config
    logging.basicConfig(
        filename=Path(environ['logs_dir'], f't_stac_{datetime.datetime.now():%Y%m%d%H%M%S}.log'),
        filemode='w',
        format=' %(levelname)s - %(asctime)s - %(message)s',
        level=logging.DEBUG)

    stac_url = r'https://cmr.earthdata.nasa.gov/stac'

    r = t_requests.ask_nicely(get_stac_session(), stac_url, session_func=get_stac_session, max_attempts=3)

    if not r:
        print('No base catalog')
        exit()

    id = 0

    new_agency = c_stac.Agency(id_num=id, name='NASA')

    id += 1

    links_to_search = [list(r.json()['links'])]
    hrefs_to_drill = []
    for link in list(r.json()['links']):
        print(link)
    exit()

    while links_to_search:
        links = links_to_search.pop()
        children = False
        for link in links:
            if link['rel'] == 'child':
                hrefs_to_drill.append(link['href'])
                children = True
        if not children:
            print(links)
            input()
        while hrefs_to_drill:
            href = hrefs_to_drill.pop()
            r = t_requests.ask_nicely(get_stac_session(), href, session_func=get_stac_session, max_attempts=3)
            links_to_search.append(list(r.json()['links']))


def unpack_data_map(get_max_id=False):

    max_id = 0

    infile = r'cmr_data_map.json'
    inpath = Path(environ['outputs_dir'], infile)

    with open(inpath, mode='r') as f:
        indict = json.load(f)

    agency = c_stac.Agency(id_num=int(indict['id']), name=indict['name'])
    if agency.id > max_id:
        max_id = agency.id

    for provider in indict['providers']:
        new_provider = c_stac.Provider(id_num=int(provider['id']), name=provider['name'], url=provider['url'])
        agency.providers[new_provider.id] = new_provider
        if new_provider.id > max_id:
            max_id = new_provider.id
        for collection in provider['collections']:
            new_collection = c_stac.Collection(id_num=int(collection['id']),
                                               name=collection['name'],
                                               url=collection['url'])
            new_collection.tags = collection['tags']
            new_provider.collections[new_collection.id] = new_collection
            if new_collection.id > max_id:
                max_id = new_collection.id

    if get_max_id:
        return agency, max_id

    return agency


def get_fire_and_ice_network():

    agency, max_id = unpack_data_map(get_max_id=True)

    outpath = Path(environ['outputs_dir'])

    node_file = open(Path(outpath, 'fire_ice_nodes.csv'), mode='w')
    node_file.write(f'node_id,label,type,collections\n')
    node_file.write(f'{agency.id},{agency.name},Agency,{len(agency.providers)}\n')

    edge_file = open(Path(outpath, 'fire_ice_edges.csv'), mode='w')
    edge_file.write(f'from,to\n')

    for provider_key in agency.providers.keys():
        fire_count = 0
        ice_count = 0
        fire_or_ice = 0
        provider = agency.providers[provider_key]
        for collection_key in provider.collections.keys():
            collection = provider.collections[collection_key]
            if 'Fire' in collection.tags:
                fire_or_ice += 1
                fire_count += 1
            if 'Ice' in collection.tags:
                fire_or_ice += 1
                ice_count += 1
        if fire_or_ice > 0:
            node_file.write(f'{provider.id},{provider.name},Provider,{fire_or_ice}\n')
            edge_file.write(f'{int(agency.id)},{int(provider.id)}\n')
        if fire_count > 0:
            max_id += 1
            node_file.write(f'{int(max_id)},Fire,Provider,{fire_count}\n')
            edge_file.write(f'{int(provider.id)},{int(max_id)}\n')
        if ice_count > 0:
            max_id += 1
            node_file.write(f'{int(max_id)},Ice,Provider,{ice_count}\n')
            edge_file.write(f'{int(provider.id)},{int(max_id)}\n')

    node_file.close()
    edge_file.close()


def main():

    # Set up the logging config
    logging.basicConfig(
        filename=Path(environ['logs_dir'], f't_stac_{datetime.datetime.now():%Y%m%d%H%M%S}.log'),
        filemode='w',
        format=' %(levelname)s - %(asctime)s - %(message)s',
        level=logging.DEBUG)

    stac_url = r'https://cmr.earthdata.nasa.gov/stac'

    r = t_requests.ask_nicely(get_stac_session(), stac_url, session_func=get_stac_session, max_attempts=3)

    if not r:
        print('No base catalog')
        exit()

    id = 0

    new_agency = c_stac.Agency(id_num=id, name='NASA')

    id += 1

    for link in r.json()['links']:
        if link['rel'] == 'child':
            if link['title'] not in new_agency.archives.keys():
                new_archive = c_stac.Archive(id_num=id, name=link['title'], url=link['href'])
                new_agency.archives[new_archive.name] = new_archive
                id += 1
    for archive_key in new_agency.archives.keys():
        archive = new_agency.archives[archive_key]
        r = t_requests.ask_nicely(get_stac_session(), archive.url, session_func=get_stac_session, max_attempts=3)
        if r:
            for link in r.json()['links']:
                if link['rel'] == 'child':
                    child_name = link['href'].split('/')[-1]
                    if child_name not in archive.collections.keys():
                        new_collection = c_stac.Collection(id_num=id, name=child_name, url=link['href'])
                        archive.collections[new_collection.name] = new_collection
                        id += 1
        for collection_key in archive.collections.keys():
            collection = archive.collections[collection_key]
            r = t_requests.ask_nicely(get_stac_session(), collection.url, session_func=get_stac_session, max_attempts=3)
            if r:
                for link in r.json()['links']:
                    if link['rel'] == 'child':
                        if link['title'] not in collection.datasets.keys():
                            new_dataset = c_stac.Dataset(id_num=id, name=link['title'], url=link['href'])
                            collection.datasets[new_dataset.name] = new_dataset
                            id += 1


    outpath = Path(environ['outputs_dir'], 'data_map_edges.csv')

    with open(outpath, mode='w') as of:
        of.write(f'from,to\n')
        for archive_key in new_agency.archives.keys():
            archive = new_agency.archives[archive_key]
            of.write(f'{new_agency.id},{archive.id}\n')
            for collection_key in archive.collections.keys():
                collection = archive.collections[collection_key]
                of.write(f'{archive.id},{collection.id}\n')
                for dataset_key in collection.datasets.keys():
                    dataset = collection.datasets[dataset_key]
                    of.write(f'{collection.id},{dataset.id}\n')

    outpath = Path(environ['outputs_dir'], 'data_map_nodes.csv')

    with open(outpath, mode='w') as of:
        of.write(f'node_id,label,type\n')
        of.write(f'{new_agency.id},{new_agency.name},Agency\n')
        for archive_key in new_agency.archives.keys():
            archive = new_agency.archives[archive_key]
            of.write(f'{archive.id},{archive.name},Archive\n')
            for collection_key in archive.collections.keys():
                collection = archive.collections[collection_key]
                of.write(f'{collection.id},{collection.name},Collection\n')
                for dataset_key in collection.datasets.keys():
                    dataset = collection.datasets[dataset_key]
                    of.write(f'{dataset.id},{dataset.name},Dataset\n')


def alter_snapshot(snapshot_path, output_path):

    with open(snapshot_path, mode='r') as f:
        snap_dict = json.load(f)

    for node in snap_dict['rawGraph']['nodes']:
        if node['label'] == "Fire":
            snap_dict['overrides'][node['node_id']] = {"color": '#eb8334',
                                                       "shape": "triangle"}
            snap_dict['positions'][node['node_id']][0] *= 2
            snap_dict['positions'][node['node_id']][1] *= 2
            snap_dict['pinnedNodes'].append(node['node_id'])

        elif node['label'] == "Ice":
            snap_dict['overrides'][node['node_id']] = {"color": '#03e2ff',
                                                       "shape": "square"}
            snap_dict['positions'][node['node_id']][0] *= 1.8
            snap_dict['positions'][node['node_id']][1] *= 1.8
            snap_dict['pinnedNodes'].append(node['node_id'])

        elif node['type'] == "Provider":

            if node['label'] == 'NOAA_NCEI':
                snap_dict['overrides'][node['node_id']] = {"color": '#db0000',
                                                           "shape": "hexagon",
                                                           "label": "NOAA NCEI"}  # Found that I have to override.
                snap_dict['nodesShowingLabels'].append(node['node_id'])
            else:
                snap_dict['overrides'][node['node_id']] = {"color": '#4ca14e'}
            snap_dict['positions'][node['node_id']][0] *= 2
            snap_dict['positions'][node['node_id']][1] *= 2
            snap_dict['pinnedNodes'].append(node['node_id'])

        elif node['type'] == "Agency":
            snap_dict['overrides'][node['node_id']] = {"color": '#1303ff',
                                                       "shape": "hexagon",
                                                       "size": 20,
                                                       "label": "NASA"}  # Found that I have to override.
            snap_dict['positions'][node['node_id']][0] = 0
            snap_dict['positions'][node['node_id']][1] = 0
            snap_dict['nodesShowingLabels'].append(node['node_id'])

        snap_dict['global']['labelBy'] = 'label'

    with open(outpath, mode='w') as of:
        json.dump(snap_dict, of)

# Dictionary
#   "rawGraph:"
#      "nodes": list: each entry a dict: "node_id", "label", "type", "collections", "id", "degree", "pagerank", "isHidden"(bool)
#      "edges": list: each entry a dict: "source_id", "target_id"
#   "nodesShowingLabels": list of labels [str, str, str]
#   "overrides": dict, str(ID) as key: "color": #000, "size": int, "label": str, "shape": "hexagon"
#   "positions": dict: str(ID) as key: [float x, float y]
#   "pinnedNodes": list <>??
#   "metadata": dict:
#       "snapshotName": str
#       "fullNodes": int
#       "fullEgdes": int
#       "nodeProperties": [list of keys from "nodes" dicts]
#       "nodeComputed":["pagerank", "degree"]
#       "edgeProperties": [list of keys from "edges" dicts]
#   "global": dict:
#       "colorBy": key from "nodes" dicts
#       "color": dict: "scale":"Linear Scale","from":#hex,"to":#hex
#       "sizeBy": key from "nodes" dicts
#       "size": dict: "min": int, "max": int, "scale": "Linear Scale" (Log?)
#       "labelBy": key from "nodes" dicts
#       "shape": str e.g. "circle"
#       "labelSize": int
#       "labelLength": int
#       "egdes": dict: "color":#hex


if __name__ == '__main__':

    target = Path(r"F:\UMB\laads_tools\outputs\fire_ice_snapshot.json")
    outpath = Path(r"F:\UMB\laads_tools\outputs\fire_ice_snapshot_color.json")

    alter_snapshot(target, outpath)

    #get_fire_and_ice_network()
    #main_with_tag_scrape()

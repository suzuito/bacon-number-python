# -*- coding: utf-8 -*-
from typing import Dict, Any, List
from dataclasses import dataclass, asdict
import os
import time
import json
import sys

import requests
from flask import Flask, request, jsonify, abort
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud import tasks_v2

cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
    'projectId': os.environ['GOOGLE_CLOUD_PROJECT'],
})


NODE_TYPE_PERSON = 'person'
NODE_TYPE_MOVIE = 'movie'

INF = sys.maxsize


@dataclass
class Node:
    node_type: str
    raw_id: str
    node_id: str
    distance: int


app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello World!'


def generate_node_id(node_type: str, raw_id: str):
    return '{}-{}'.format(node_type, raw_id)


def enqueue_node(node_type: str, raw_id: str, current_distance: int):
    print('enqueue {}-{}'.format(node_type, raw_id))
    cli = tasks_v2.CloudTasksClient()
    parent = cli.queue_path(
        os.environ['GOOGLE_CLOUD_PROJECT'], 'asia-northeast1', 'node-updator')
    task = {
        'http_request': {  # Specify the type of request.
            'http_method': 'POST',
            'url': '{}/process/{}/{}'.format('https://bacon-number-dot-jxpress-playground.appspot.com', node_type, raw_id),
            'body': json.dumps({'distance': current_distance}).encode('utf8'),
        }
    }
    response = cli.create_task(parent, task)


@app.route('/process/<node_type>/<raw_id>', methods=['POST'])
def process(node_type: str, raw_id: str):
    distance = json.loads(request.data)['distance']
    print('process {}-{} {}'.format(node_type, raw_id, distance))
    node_id = generate_node_id(node_type, raw_id)
    db = firestore.client()
    node_ref = db.collection(u'nodes').document(
        node_id,
    )
    opt_distance = INF
    node_snp = node_ref.get()
    node_dict = None
    if node_snp.exists == False:
        node: Node = request_get_node(node_type, raw_id)
        node.distance = distance
        node_dict = asdict(node)
    else:
        node_dict = node_snp.to_dict()
        opt_distance = node_dict.get('distance', INF)
        if opt_distance <= distance:
            return 'No updated'
        node_dict['distance'] = distance
    node_ref.set(node_dict)
    adjs: List[Node] = request_get_adjs(node_type, raw_id)
    for adj in adjs:
        enqueue_node(adj.node_type, adj.raw_id, node_dict['distance'] + 1)
        adj_ref = node_ref.collection(u'adjacencies').document(adj.node_id)
        adj_ref.set(asdict(adj))
    return 'ok'


API_URL = 'https://api.themoviedb.org/3'


def request_get_adjs(node_type: str, raw_id: str) -> List[Node]:
    if node_type == NODE_TYPE_MOVIE:
        return request_get_movie_credits(node_type, raw_id)
    if node_type == NODE_TYPE_PERSON:
        return request_get_person_movie_credits(node_type, raw_id)
    raise Exception('ERROR!!!')


def request_get_node(node_type: str, raw_id: str) -> Node:
    if node_type == NODE_TYPE_MOVIE:
        return request_get_movie(node_type, raw_id)
    if node_type == NODE_TYPE_PERSON:
        return request_get_person(node_type, raw_id)
    raise Exception('ERROR!!!')


def request_get_movie(node_type: str, raw_id: str) -> Node:
    node_id = generate_node_id(node_type, raw_id)
    res = requests.get(
        '{}/movie/{}?api_key={}'.format(API_URL, raw_id, os.environ['API_KEY']))
    res.raise_for_status()
    movie_detail = res.json()
    return Node(
        node_type=node_type,
        node_id=node_id,
        raw_id=raw_id,
        distance=INF,
    )


def request_get_person(node_type: str, raw_id: str) -> Node:
    node_id = generate_node_id(node_type, raw_id)
    res = requests.get(
        '{}/person/{}?api_key={}'.format(API_URL, raw_id, os.environ['API_KEY']))
    res.raise_for_status()
    movie_detail = res.json()
    return Node(
        node_type=node_type,
        node_id=node_id,
        raw_id=raw_id,
        distance=INF,
    )


def request_get_movie_credits(node_type: str, raw_id: str) -> List[Node]:
    res = requests.get(
        '{}/movie/{}/credits?api_key={}'.format(
            API_URL, raw_id, os.environ['API_KEY']),
    )
    res.raise_for_status()
    creds = res.json()
    return list(map(
        lambda v: Node(
            node_type=NODE_TYPE_PERSON,
            node_id=generate_node_id(NODE_TYPE_PERSON, v['id']),
            raw_id=v['id'],
            distance=INF
        ),
        creds.get('cast', []),
    ))


def request_get_person_movie_credits(node_type: str, raw_id: str) -> List[Node]:
    res = requests.get(
        '{}/person/{}/movie_credits?api_key={}'.format(API_URL, raw_id, os.environ['API_KEY']))
    res.raise_for_status()
    creds = res.json()
    return list(map(
        lambda v: Node(
            node_type=NODE_TYPE_MOVIE,
            node_id=generate_node_id(NODE_TYPE_MOVIE, v['id']),
            raw_id=v['id'],
            distance=INF
        ),
        creds.get('cast', []),
    ))


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

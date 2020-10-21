#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging

from flask import Flask
from flask import render_template
from flask import jsonify
from flask import request
from flasgger import Swagger
import requests

from elasticsearch import Elasticsearch


from flasgger import Swagger, LazyString, LazyJSONEncoder



hosts = [{'host': "localhost", 'port': 9200}]
elastic_client = Elasticsearch(hosts=hosts)


def retrieve_context(query_terms, size=5):
    search_query = {
        "size": size,
        "query": {
            "match": {
                "para_content": query_terms
            }
        }
    }

    elastic_response_raw_para = elastic_client.search(index='para_content', body=search_query)
    _elastic_response_temp = [{'_source': e['_source'], '_score': e['_score']} for e in
                              elastic_response_raw_para["hits"]["hits"]]

    elastic_response_para = list()
    for e in _elastic_response_temp:
        temp = e['_source']
        temp['es_score'] = e['_score']
        temp['bookname'] = temp['op_subject'] + " - " + temp['op_chapter_name']
        temp['chapter_path'] = temp['chapter_path_url_s3'] + ", " + temp['op_chapter_path_url_publisher']

        elastic_response_para.append(temp)
    del _elastic_response_temp

    search_query = {
        "size": 2,
        "query": {
            "match": {
                "page_content": query_terms
            }
        }
    }
    elastic_response_raw_page = elastic_client.search(index='page_content', body=search_query)
    _elastic_response_temp = [{'_source': e['_source'], '_score': e['_score']} for e in
                              elastic_response_raw_page["hits"]["hits"]]

    elastic_response_page = list()
    for e in _elastic_response_temp:
        temp = e['_source']
        temp['es_score'] = e['_score']
        temp['bookname'] = temp['op_subject'] + " - " + temp['op_chapter_name']
        temp['chapter_path'] = temp['chapter_path_url_s3'] + ", " + temp['op_chapter_path_url_publisher']
        elastic_response_page.append(temp)
    del _elastic_response_temp

    result = dict()
    result['para_suggestion'] = elastic_response_para
    result['page_suggestion'] = elastic_response_page

    return result


secret = os.urandom(24).hex()

app = Flask(__name__)
app.logger.info("Starting...")
app.config['SECRET_KEY'] = secret
app.logger.critical("secret: %s" % secret)

# socketio = SocketIO(app)
app = Flask(__name__)
app.json_encoder = LazyJSONEncoder

template = dict(swaggerUiPrefix=LazyString(lambda : request.environ.get('HTTP_X_SCRIPT_NAME', '')))



swagger_config = {
    "headers": [
    ],
    "specs": [
        {
            "endpoint": 'apispec_1',
            "route": '/apispec_1.json',
            "rule_filter": lambda rule: True,  # all in
            "model_filter": lambda tag: True,  # all in
        }
    ],
    "static_url_path": "/flasgger_static",
    # "static_folder": "static",  # must be set by user
    "swagger_ui": True,
    "specs_route": "/apidocs/"
}





swagger = Swagger(app, template=template) 
# swagger = Swagger(app, config=swagger_config)

#swagger = Swagger(app) 


@app.route("/check")
def check_home():
    help = {
        "getcontext_url": "call /getcontext?query=%E0%A4%86%E0%A4%B0%E0%A5%8D%E0%A4%A5%E0%A4%BF%E0%A4%95%20%E0%A4%A8%E0%A5%80%E0%A4%A4%E0%A4%BF%E0%A4%AF%E0%A4%BE%E0%A4%81",
        "swagger_url": "call /apidocs",
        "results_display": "call test"
    }
    return jsonify({"status": "Running Flask on Google Colab!", "help": help})


@app.route("/getcontext")
def retrieve_context_handler():
    """Based on query asked, this will retrieve from elasticsearch.
    ---
    parameters:
      - name: query
        in: query
        type: string
        required: true
        default: "व्यावसायिक पर्यावरण"
    responses:
      200:
        description: A list of pages and paragraphs
    """
    query_terms = request.args.get('query')
    if query_terms is None or len(query_terms) == 0:
        return jsonify({"status": " Missing query parameter, Enter  /getcontext?query=breakfast"})

    # print(query_terms)
    result = retrieve_context(query_terms=query_terms, size=5)
    return jsonify(result)


@app.route('/')
def get_results():
    if request.method == 'GET':
        querytext = request.args.get('querytext', '')
        querytext = querytext.strip()
        search_results_dict = None

        # print(f'querytext={querytext}')

        if len(querytext) == 0:
            search_results_dict = None
        else:
            search_results_dict = retrieve_context(query_terms=querytext, size=5)
            #from pprint import pprint
            #pprint(search_results_dict)

            return render_template('home.html', search_results=search_results_dict)
        return render_template('home.html', search_results=search_results_dict)

    return render_template('home.html')


if __name__ == "__main__":
    app.run(debug=False)

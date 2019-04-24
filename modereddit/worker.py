
import faust
import logging
import pandas as pd
from datetime import datetime
from pipeline import transform_to_clean, transform_to_tokens, predict_mod_proba
from elasticsearch_async import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchException
import json
import os


logging.info("Starting modereddit stream processor worker")
FAUST_APP_NAME = "reddit-stream-processor"
KAFKA_BROKERS = "kafka://172.25.0.11:9092"
ELASTICSEARCH_HOST = "172.25.0.13" #os.environ["ELASTICSEARCH_HOST"]
ELASTICSEARCH_USER = "" #os.environ["ELASTICSEARCH_USER"]
ELASTICSEARCH_PASS = "" #os.environ["ELASTICSEARCH_PASS"]
ELASTICSEARCH_PORT = 9200 #os.environ["ELASTICSEARCH_PASS"]
# Elastic search init
ELASTIC_SEARCH_CONF = {
    'host': ELASTICSEARCH_HOST,
    #'http_auth': (ELASTICSEARCH_USER, ELASTICSEARCH_PASS), 
    #'url_prefix': 'elasticsearch', 
    'scheme': 'http', 
    'port': ELASTICSEARCH_PORT
}

ELASTICSEARCH_DOCUMENT_INDEX = "reddit"
ELASTICSEARCH_DOCUMENT_TYPE  = "comment"

es = AsyncElasticsearch([ELASTIC_SEARCH_CONF])

app = faust.App(
        FAUST_APP_NAME,
        broker=KAFKA_BROKERS,
        key_serializer="json",
        value_serializer="json",
        topic_partitions=2)


reddit_topic = app.topic("reddit_stream")
reddit_tokenized_topic = app.topic("reddit_tokenized_stream")
reddit_mod_alert_topic = app.topic("reddit_mod_alert_stream")

MODERATION_THRESHOLD = 0.5

df_cols = ["prev_idx", "body", "score", "parent_id", "id", 
            "created_date", "retrieved_date", "removed"]
df_reddit = pd.read_csv("data/reddit.csv",
                         names=df_cols, skiprows=1, encoding="ISO-8859-1")

@app.timer(0.1)
async def generate_reddit_comments():
    reddit_sample = df_reddit.sample(1)
    reddit_data = {
        "id": reddit_sample["id"].values[0],
        "score": int(reddit_sample["score"].values[0]),
        "created_date":  int(reddit_sample["created_date"].values[0]),
        "body": reddit_sample["body"].values[0],
        "retrieved_on": datetime.now().timestamp()
    }
    reddit_id = reddit_data["id"]
    await reddit_topic.send(  
            key=reddit_id,
            value=reddit_data)


@app.agent(reddit_topic)
async def tokenize_reddit_stream(comment_stream):
    async for key, comment in comment_stream.items():
        comment_str = comment["body"]
        clean_str = await transform_to_clean(comment_str)
        comment["body_tokens"] = await transform_to_tokens(clean_str)
        await reddit_tokenized_topic.send(
                key=key,
                value=comment)

@app.agent(reddit_tokenized_topic)
async def predict_reddit_content(tokenized_stream):
    async for key, comment_extended in tokenized_stream.items():
        if not "body_tokens" in comment_extended:
            logging.error(f"Malformed. Key: {key}. val: {comment_extended}")
            continue
        tokens = comment_extended["body_tokens"]
        probability, vecs = predict_mod_proba(tokens)        
        if probability > MODERATION_THRESHOLD:
            alert = {
                "probability": probability,
                "original": comment_extended["body"]
            }
            await reddit_mod_alert_topic.send(
                    key=key,
                    value=alert)

@app.agent(reddit_topic)
async def reddit_elasticsearch_sink(comment_stream):
    async for key, comment in comment_stream.items():
        try:
            json_str_comment = json.dumps(comment)
            response = await es.index(
                index=ELASTICSEARCH_DOCUMENT_INDEX, 
                doc_type=ELASTICSEARCH_DOCUMENT_TYPE, 
                id=key, 
                body=json_str_comment)
            failed = response.get("_shards",{}).get("failed")

            if failed:
                logging.error("Elasticsearch request failed with the following error: " + \
                    str(response) + "The parameters were, id/key: " + str(key) + \
                    " body/value: " + str(json_str_comment))

        except ElasticsearchException as e:
            logging.exception("An Elasticsearch exception has been caught :" + str(e) + \
                "The parameters are: id/key - " + str(key) + json_str_comment)

 


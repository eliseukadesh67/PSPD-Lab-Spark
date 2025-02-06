# Notebook Google Colab para Processamento de Dados com Spark Streaming, Kafka e IA

import findspark

findspark.init()

# 2. Configuração do Spark e Kafka
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

spark = SparkSession.builder \
    .appName("SparkKafkaIA") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

print("Spark Session Criada!")

# 3. Configuração do ElasticSearch

es = Elasticsearch(["http://localhost:9201"])

print("Elastic iniciado!")

def criar_indice_elasticsearch():
    index_config = {
        "mappings": {
            "properties": {
                "rede": {"type": "keyword"},
                "usuario": {"type": "text"},
                "texto": {"type": "text"},
                "analise": {"type": "text"}
            }
        }
    }
    if not es.indices.exists(index="posts"):
        es.indices.create(index="posts", body=index_config)
        print("Índice 'posts' criado no ElasticSearch")
    else:
        print("Índice 'posts' já existe")

criar_indice_elasticsearch()

# 4. Conexão com Kafka e Coleta de Dados do Threads e Reddit
from kafka import KafkaProducer
import json
import requests

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def coletar_reddit(termo):
    url = f"https://www.reddit.com/search.json?q={termo}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        reddit_data = response.json()
        for post in reddit_data["data"]["children"]:
            data = {"rede": "Reddit", "usuario": post["data"]["author"], "texto": post["data"]["title"]}
            producer.send("posts", value=data)
            enviar_para_elastic(data)
            print(f"Enviado para Kafka e ElasticSearch: {data}")

# 5. Envio dos Dados Processados para o ElasticSearch
def enviar_para_elastic(dados):
    es.index(index="posts", body=dados)
    print("Dados enviados para o ElasticSearch")

# Exemplo: coletar postagens sobre "Política"
coletar_reddit("política")

print("Pipeline concluído!")
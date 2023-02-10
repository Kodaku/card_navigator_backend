from fastapi import FastAPI
from elasticsearch import Elasticsearch
from es_search import find_all, find_by_name
from fastapi.middleware.cors import CORSMiddleware

es = Elasticsearch(
    hosts=['http://localhost:9200'],
    basic_auth=('elastic', 'Cj-ChuXcllkRQF8t8VFa')
)

index_name = "expansions"

app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def test_endpoint():
    return {"message": "Hello!"}


@app.get("/expansions")
async def find_all_expansions():
    expansions = find_all(es, index_name)
    return expansions


@app.get("/expansions/{expansion_name}")
async def find_expansion_by_name(expansion_name: str):
    expansion = find_by_name(es, index_name, expansion_name)
    return expansion

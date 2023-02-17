import json

from fastapi import FastAPI
from pydantic import BaseModel
from elasticsearch import Elasticsearch, RequestError
from es_search import find_all, find_by_name
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd


class Card(BaseModel):
    card_name: str
    card_img_url: str
    quantity: int


class WishList(BaseModel):
    wish_list_name: str
    cards: list


es = Elasticsearch(
    hosts=['http://localhost:9200'],
    basic_auth=('elastic', 'Cj-ChuXcllkRQF8t8VFa')
)


def es_create_index_if_not_exists(es, index):
    try:
        es.indices.create(index=index)
    except RequestError as ex:
        print(ex)


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
    expansion = find_by_name(es, index_name, "expansion_full_name", expansion_name)
    return expansion


@app.post("/wish-lists")
async def create_wish_list(wish_list: WishList):
    if not es.indices.exists(index="wish-lists"):
        print(f"Index {index_name} created")
        es_create_index_if_not_exists(es, "wish-lists")
    actions = []
    action = {"index": {"_index": "wish-lists", "_id": wish_list.wish_list_name}, "_op_type": "upsert"}
    wish_list = {"wish_list_name": wish_list.wish_list_name, "cards": []}
    doc = json.dumps(wish_list)
    actions.append(action)
    actions.append(doc)
    res = es.bulk(index="wish-lists", operations=actions)
    print(res)
    return wish_list


@app.post("/wish-lists/cards/{list_name}")
async def add_card_to_wish_list(list_name: str, card: Card):
    wish_list = find_by_name(es, "wish-lists", "wish_list_name", list_name)
    cards = wish_list['cards']
    card = {"card_name": card.card_name, "card_img_url": card.card_img_url, "quantity": card.quantity}
    cards.append(card)
    actions = []
    action = {"index": {"_index": "wish-lists", "_id": list_name}, "_op_type": "upsert"}
    wish_list = {"wish_list_name": list_name, "cards": cards}
    doc = json.dumps(wish_list)
    actions.append(action)
    actions.append(doc)
    res = es.bulk(index="wish-lists", operations=actions)
    print(res)
    return find_by_name(es, "wish-lists", "wish_list_name", list_name)


@app.get("/wish-lists")
async def find_all_wish_lists():
    if es.indices.exists(index="wish-lists"):
        wish_lists = find_all(es, "wish-lists")
        return wish_lists
    return {"error": "index wish-lists does not exist"}


@app.get("/wish-lists/{list_name}")
async def find_wish_list_by_name(list_name: str):
    wish_list = find_by_name(es, "wish-lists", "wish_list_name", list_name)
    return wish_list


@app.get("/wish-lists/delete/{list_name}")
async def delete_wish_list_by_name(list_name: str):
    es.delete(index="wish-lists", id=list_name)
    return {"msg": "Deleted"}


@app.get("/wish-lists/delete/card/{list_name}/{card_name}")
async def delete_card_from_list(list_name: str, card_name: str):
    wish_list = find_by_name(es, "wish-lists", "wish_list_name", list_name)
    cards = wish_list['cards']
    for card in cards:
        if card["card_name"] == card_name:
            print(card)
            cards.remove(card)
            break
    actions = []
    action = {"index": {"_index": "wish-lists", "_id": list_name}, "_op_type": "upsert"}
    wish_list = {"wish_list_name": list_name, "cards": cards}
    doc = json.dumps(wish_list)
    actions.append(action)
    actions.append(doc)
    res = es.bulk(index="wish-lists", operations=actions)
    print(res)
    return find_by_name(es, "wish-lists", "wish_list_name", list_name)


@app.get("/wish-lists/export/to-csv/{list_name}")
async def export_wish_list_as_csv(list_name: str):
    wish_list = find_by_name(es, "wish-lists", "wish_list_name", list_name)
    cards = wish_list['cards']
    exportable_cards = []
    for card in cards:
        exportable_cards.append([card["card_name"], card["quantity"]])
    exportable_cards_df = pd.DataFrame(exportable_cards, columns=["card_name", "quantity"])
    exportable_cards_df.to_csv(f"{list_name}.csv")

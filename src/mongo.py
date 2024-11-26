import asyncio
import logging
from datetime import datetime
from functools import wraps
from typing import Callable, List, Type

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from pymongo.errors import ConnectionFailure, WriteError

from config import config

logger = logging.getLogger("mongo.mongodb_client_async")


def operator_retry(retries: int = 5, cooldown: int = 1) -> Callable[[Callable], Callable]:
    def wrap(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            retries_count = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except (ConnectionFailure, WriteError) as err:
                    retries_count += 1
                    if retries_count > retries >= 0:
                        raise err
                    if cooldown > 0:
                        await asyncio.sleep(cooldown * retries_count)
                    logging.warning(f"mongodb connection failure, retry {retries_count} times")
        return inner
    return wrap


def iter_retry(retries: int = 5, cooldown: int = 1) -> Callable[[Callable], Callable]:
    def wrap(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            retries_count = 0
            while True:
                try:
                    async for result in func(*args, **kwargs):
                        yield result
                    break
                except (ConnectionFailure, WriteError) as err:
                    retries_count += 1
                    if retries_count > retries >= 0:
                        raise err
                    if cooldown > 0:
                        await asyncio.sleep(cooldown * retries_count)
                    logging.warning(f"mongodb connection failure, retry {retries_count} times")
        return inner
    return wrap


class AsyncMongoDBClient:
    _mongo_client = None

    @classmethod
    def load_client(cls):
        if not cls._mongo_client:
            cls._mongo_client = AsyncIOMotorClient(
                config.mongo.url,
                minPoolSize=config.mongo.min_pool_size,
                maxPoolSize=config.mongo.max_pool_size
            )
        return cls._mongo_client

    @classmethod
    @operator_retry()
    async def get_document(cls, collection_name: str, filters: dict, fields: List[str]):
        cls.load_client()
        if not cls._mongo_client:
            return
        db = cls._mongo_client.get_default_database()
        collection = db[collection_name]
        projection = {field: True for field in fields}
        projection["_id"] = 0
        document = await collection.find_one(filter=filters, projection=projection)
        return document

    @classmethod
    @iter_retry()
    def iter_documents(cls, collection_name: str, filters: dict, fields: List[str], limit: int = 0):
        cls.load_client()
        if not cls._mongo_client:
            return
        db = cls._mongo_client.get_default_database()
        collection = db[collection_name]
        projection = {field: True for field in fields}
        projection["_id"] = 0
        document = collection.find(filter=filters, projection=projection, limit=limit, batch_size=100)
        return document

    @classmethod
    @operator_retry()
    async def update_documents(cls, collection_name: str, filters: dict, item: dict):
        cls.load_client()
        if not cls._mongo_client:
            return
        db = cls._mongo_client.get_default_database()
        collection = db[collection_name]
        item["_update_time"] = datetime.now()
        update = {'$set': item}
        await collection.update_many(filters, update)

    @classmethod
    @operator_retry()
    async def save_document(cls, collection_name: str, filters: dict, item: dict):
        cls.load_client()
        if not cls._mongo_client:
            return
        db = cls._mongo_client.get_default_database()
        collection = db[collection_name]
        item["_update_time"] = datetime.now()
        update = {'$setOnInsert': {"_create_time": datetime.now()}, '$set': item}
        await collection.update_one(filters, update, upsert=True)

    @classmethod
    async def get_asset_info(cls, asset_type: str, asset_id: str, cloud_account_id: str, fields: List[str]):
        filters = {"asset_id": asset_id, "cloud_account_id": cloud_account_id, "_expired": False}
        return await cls.get_document("asset." + asset_type, filters, fields)

    @classmethod
    def iter_asset_info(cls, asset_type: str, filters: dict, fields: List[str], limit: int = 0):
        filters["_expired"] = False
        return cls.iter_documents("asset." + asset_type, filters, fields, limit=limit)

    @classmethod
    async def get_documents_count(
        cls, collection_name: str, filters: dict
    ) -> int:
        cls.load_client()
        if not cls._mongo_client:
            return 0
        db = cls._mongo_client.get_default_database()
        collection = db[collection_name]
        total_count = await collection.count_documents(filters)
        return total_count

    @classmethod
    async def fetch_paginated_documents(
        cls, collection_name: str, filters: dict, fields: List[str], page: int = 1, page_size: int = 20
    ) -> List[dict]:
        """
        分页查询文档数据。
        :param collection_name: 集合名称。
        :param fields: 要返回的字段列表。
        :param page: 要返回的页数。
        :param page_size: 每页返回的文档数量。
        :return: 包含文档列表
        """
        cls.load_client()
        if not cls._mongo_client:
            return []
        db = cls._mongo_client.get_default_database()
        collection = db[collection_name]
        projection = {field: True for field in fields}
        projection["_id"] = 1
        skip = (page - 1) * page_size
        sort = [("_id", ASCENDING)]
        documents = collection.find(filter=filters, projection=projection, sort=sort, skip=skip, limit=page_size)
        documents = await documents.to_list(length=page_size)
        return documents

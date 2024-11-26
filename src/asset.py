from datetime import datetime
import logging
from typing import List

from pydantic import BaseModel, Field
from pymongo.errors import WriteError
from pymongo import IndexModel, ASCENDING

from mongo import AsyncMongoDBClient


logger = logging.getLogger("asset")


class CloudProvider(BaseModel):
    """云服务提供商"""
    name: str = "aws"
    region: str = "global"


class CloudAssetDocItem(BaseModel):
    """云资产基础文档项"""
    provider: str = Field(default="aws")
    service: str = Field(description="服务名称")
    asset: str = Field(description="资产类型")
    asset_id: str = Field(description="资产ID")
    asset_name: str = Field(description="资产名称")
    cloud_account_id: str = Field(default="", description="云账号ID")


class CloudAssetDocument(CloudAssetDocItem):
    """云资产文档"""
    create_at: datetime = Field()
    update_at: datetime = Field()
    expired: bool = Field(default=False)


class AsyncAssetClient(AsyncMongoDBClient):

    @classmethod
    async def init_indexes(cls):
        """初始化集合索引"""
        cls.load_client()
        if not cls._mongo_client:
            return
        try:
        # 测试连接
            await cls._mongo_client.admin.command('ping')
            logger.info("MongoDB connection successful")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {str(e)}")
            raise

        db = cls._mongo_client.get_default_database()

        # 为aws产品和API操作集合创建索引
        collections = [
            "asset.aws.product",
            "asset.aws.product_action"
        ]

        for collection_name in collections:
            collection = db[collection_name]

            # 创建联合唯一索引
            index_name = "assetId_cloudAccountId_unique"
            await collection.create_indexes([
                IndexModel(
                    [
                        ("asset_id", ASCENDING),
                        ("cloud_account_id", ASCENDING)
                    ],
                    unique=True,
                    name=index_name
                )
            ])

            logger.info(f"Created index {index_name} for collection {collection_name}")

    @classmethod
    async def save_asset_info(cls, asset: CloudAssetDocItem, item: dict):
        item["_expired"] = False
        item.update(asset.model_dump())
        filters = {"asset_id": asset.asset_id, "cloud_account_id": asset.cloud_account_id}
        try:
            await cls.save_document(f"asset.{asset.provider}.{asset.asset}", filters, item)
        except WriteError as err:
            logger.warning(f"mongo save_asset_info asset_id:{asset.asset_id}, write error:{err}")
            raise err

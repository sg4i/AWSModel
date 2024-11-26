import asyncio
import logging
from datetime import datetime
from typing import List

from asset import AsyncAssetClient, CloudAssetDocItem
from boto import AWSModel, ServiceMeta, APIOperation

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def save_service_meta(service: ServiceMeta):
    """保存服务元数据"""
    try:
        asset = CloudAssetDocItem(
            service="product",
            asset="product",
            asset_id=f"{service.name}_{service.api_version}",
            asset_name=service.service_full_name
        )
        
        # 将ServiceMeta的所有字段作为额外数据存储
        item = {
            "raw": service.raw
        }
        
        await AsyncAssetClient.save_asset_info(asset, item)
        logger.info(f"Saved service metadata: {service.name}")
        
    except Exception as e:
        logger.error(f"Error saving service metadata {service.name}: {str(e)}")

async def save_service_api(service_name: str, api: APIOperation):
    """保存服务API信息"""
    try:
        asset = CloudAssetDocItem(
            service="product",
            asset="product_action",
            asset_id=f"{service_name}.{api.name}",
            asset_name=api.name
        )
        
        # 将APIOperation的所有字段作为额外数据存储
        item = {
            "raw": api.raw
        }
        
        await AsyncAssetClient.save_asset_info(asset, item)
        logger.info(f"Saved API operation: {service_name}.{api.name}")
        
    except Exception as e:
        logger.error(f"Error saving API operation {service_name}.{api.name}: {str(e)}")

async def save_all_services():
    """保存所有服务的元数据和API信息"""
    try:
        # 初始化MongoDB索引
        await AsyncAssetClient.init_indexes()
        
        # 获取AWS服务数据
        aws_model = AWSModel()
        services = aws_model.list_services()
        
        # 保存服务元数据
        for service in services:
            await save_service_meta(service)
            
            # 获取并保存服务的API操作
            apis = aws_model.list_service_apis(service.name)[service.name]
            for api in apis:
                await save_service_api(service.name, api)
                
        logger.info("Completed saving all services and APIs")
        
    except Exception as e:
        logger.error(f"Error in save_all_services: {str(e)}")
        raise

async def main():
    """主函数"""
    try:
        await save_all_services()
    except Exception as e:
        logger.error(f"Main function error: {str(e)}")
    finally:
        # 确保关闭MongoDB连接
        if AsyncAssetClient._mongo_client:
            AsyncAssetClient._mongo_client.close()

if __name__ == "__main__":
    asyncio.run(main())

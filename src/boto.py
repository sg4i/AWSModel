import logging
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from botocore.loaders import create_loader

logger = logging.getLogger("mongo.mongodb_client_async")


class ServiceMeta(BaseModel):
    """服务元数据结构"""
    name: str = Field(description="服务名称")
    api_version: str = Field(description="API版本")
    endpoint_prefix: str = Field(description="端点前缀")
    protocol: str = Field(description="协议类型")
    service_full_name: str = Field(description="服务全名")
    raw: Dict = Field(description="原始对象")


class APIOperation(BaseModel):
    """API操作数据结构"""
    name: str = Field(description="操作名称")
    http_method: str = Field(description="HTTP方法")
    http_path: str = Field(description="HTTP路径")
    input: Optional[Dict] = Field(default=None, description="输入参数结构")
    output: Optional[Dict] = Field(default=None, description="输出参数结构")
    documentation: str = Field(description="文档说明")
    raw: Dict = Field(description="原始对象")


class AWSModel:
    def __init__(self):
        self.loader = create_loader()

    def list_raw_services(self) -> list[Dict[str, Any]]:
        services = []
        available_services = self.loader.list_available_services('service-2')

        for service_name in available_services:
            api_version = self.loader.determine_latest_version(service_name, 'service-2')
            service_data = self.loader.load_service_model(service_name, 'service-2', api_version=api_version)
            services.append(service_data)
        return services

    def list_services(self) -> List[ServiceMeta]:
        """获取所有AWS服务的元数据列表"""
        services = []
        available_services = self.loader.list_available_services('service-2')

        for service_name in available_services:
            api_version = self.loader.determine_latest_version(service_name, 'service-2')
            service_data = self.loader.load_service_model(service_name, 'service-2', api_version=api_version)
            metadata = service_data.get('metadata', {})

            services.append(ServiceMeta(
                name=service_name,
                api_version=metadata.get('apiVersion', ''),
                endpoint_prefix=metadata.get('endpointPrefix', ''),
                protocol=metadata.get('protocol', ''),
                service_full_name=service_data.get('service_full_name', ''),
                raw=metadata
            ))

        return services

    def list_service_apis(self, service_name: Optional[str] = None) -> Dict[str, List[APIOperation]]:
        """获取指定服务或所有服务的API操作信息

        Args:
            service_name: 服务名称，如果为None则返回所有服务的API

        Returns:
            Dict[str, List[APIOperation]]: 服务名称到API操作列表的映射
        """
        result = {}

        if service_name:
            services = [service_name]
        else:
            services = self.loader.list_available_services('service-2')

        for svc in services:
            try:
                service_model = self.loader.load_service_model(svc, 'service-2')
                operations = []

                for op_name, op_data in service_model.get('operations', {}).items():
                    operation = APIOperation(
                        name=op_name,
                        http_method=op_data.get('http', {}).get('method', ''),
                        http_path=op_data.get('http', {}).get('requestUri', ''),
                        input=op_data.get('input'),
                        output=op_data.get('output'),
                        documentation=op_data.get('documentation', ''),
                        raw=op_data
                    )
                    operations.append(operation)

                result[svc] = operations

            except Exception as e:
                logger.error(f"处理服务 {svc} 时出错: {str(e)}")
                continue

        return result

    def get_service_api(self, service_name: str, api_name: str) -> Optional[APIOperation]:
        """获取指定服务的特定API操作信息"""
        service_apis = self.list_service_apis(service_name)
        if service_name not in service_apis:
            return None

        for api in service_apis[service_name]:
            if api.name == api_name:
                return api

        return None

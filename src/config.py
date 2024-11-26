from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import yaml


class MongoSettings(BaseSettings):
    url: str = Field(default="mongodb://localhost:27017")
    min_pool_size: int = Field(default=10)
    max_pool_size: int = Field(default=100)
    
    model_config = SettingsConfigDict(
        env_prefix="MONGO_",
        env_file=".env",
        extra="ignore"
    )

class Settings(BaseSettings):
    mongo: MongoSettings = Field(default_factory=MongoSettings)
    
    model_config = SettingsConfigDict(
        env_nested_delimiter="_",
        env_file=".env",
        extra="ignore"
    )
    
    @classmethod
    def load_from_yaml(cls, yaml_path: Path) -> Optional["Settings"]:
        """从YAML文件加载配置"""
        try:
            if yaml_path.exists():
                with open(yaml_path, "r", encoding="utf-8") as f:
                    yaml_data = yaml.safe_load(f)
                return cls.model_validate(yaml_data)
        except Exception as e:
            print(f"无法读取配置文件: {e}")
        return None

def load_config() -> Settings:
    """加载配置,优先从yaml文件加载,然后使用环境变量覆盖"""
    # 首先尝试从yaml加载
    yaml_config = Settings.load_from_yaml(Path("config.yaml"))
    
    # 如果yaml存在则使用yaml配置,否则使用默认值
    config = yaml_config if yaml_config else Settings()
    
    # 从环境变量加载配置(会自动覆盖已有配置)
    config = Settings.model_validate(config.model_dump())
    
    return config

# 全局配置单例
config = load_config() 

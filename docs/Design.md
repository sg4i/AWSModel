# DESIGN

实现的功能：从 aws botocore sdk 解析 service 元数据和支持的 API 操作信息，这些信息存放在 aws botocore 源码的 service-2.json 文件中，解析这些数据存储到 mongo 数据库

实现要求:

- 每次查询 botocore 最新版本，自动安装最新 botocore 最新版本依赖
- 从环境变量中读取 mongo 配置（mongo_url)， 使用 pydantic v2 版本
- 使用 asyncio 异步框架，mongo 异步连接使用 motor
- 依赖包管理使用 poetry

## 读取配置信息

- 独立的配置读取模块，使用 pydantic v2 版本定义配置数据结构体
- 配置读取优先级，依次为：根目录下配置文件（yaml 格式）、环境变量
  配置结构体，从配置文件、环境变量，应该有统一的映射关系标准
  参考 Golang 实现代码：

  ```go
  func LoadConfig() {
      // sync.Once 来确保 LoadConfig 函数中的初始化代码只会执行一次
      once.Do(func() {
          viper.SetConfigName("config")
          viper.SetConfigType("yml")
          viper.AddConfigPath(".")

          viper.AutomaticEnv()
          // 使用 viper.SetEnvKeyReplacer 来设置环境变量名称的转换规则。
          // 例如，配置文件中的 storage.mongo.uri 对应的环境变量名称将是 STORAGE_MONGO_URI
          viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

          if err := viper.ReadInConfig(); err != nil {
              logger.Log.Warnf("无法读取配置文件: %v", err)
              logger.Log.Info("将使用环境变量作为配置源")
          } else {
              logger.Log.Info("成功读取配置文件")
          }
      })
  }
  ```

## 服务元数据和 API 数据存储

基础字段定义如下，参考 golang 代码

```go
type CloudAssetDocItem struct {
    Provider       CloudProvider `bson:"provider"`
    Service        string        `bson:"service"`
    AssetId        string        `bson:"asset_id"`
    AssetName      string        `bson:"asset_name"` // 新增字段
    CloudAccountId string        `bson:"cloud_account_id"`
}

// 修改 CloudAssetDocument 结构体
type CloudAssetDocument struct {
    CloudAssetDocItem `bson:",inline"`
    CreateAt          time.Time `bson:"_create_at"`
    UpdateAt          time.Time `bson:"_update_at"`
    Expired           bool      `bson:"_expired"`
}
```

- 服务元数据存储collect name 为 asset.aws.product
- api数据存储collect name为 asset.aws.prodcut_action

## 读取 botocore 版本并更新

- 访问 https://pypi.org/pypi/botocore/json 可查看最新版本数据
- Makefile 命令提供更新 botocore 版本

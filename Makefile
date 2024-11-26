.PHONY: help upgrade run

help:
	@echo "可用命令:"
	@echo "  upgrade  - 更新 botocore 到最新版本"
	@echo "  run      - 运行主程序，从 botocore 获取数据并存储到 MongoDB"

upgrade:
	@echo "正在更新 botocore 到最新版本..."
	poetry add botocore@latest
	@echo "botocore 更新完成"

run:
	@echo "开始运行主程序..."
	python -m src.main
	@echo "主程序运行完成" 
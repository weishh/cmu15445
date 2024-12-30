#!/bin/bash

# 定义日志文件
LOG_FILE="test.log"

# 清空日志文件
> "$LOG_FILE"
# 遍历 sql 目录下的每个文件
for file in ./*; do
  # 检查是否为文件
  if [ -f "$file" ]; then
    # 运行命令
    ../../build/bin/bustub-sqllogictest "$file" >> "$LOG_FILE" 2>&1
  fi
done
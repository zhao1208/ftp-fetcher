#!/usr/bin/env bash
# FTP/SFTP 定时抓取工具 —— Linux 启动脚本（含崩溃自动重启）
# 用法：
#   ./start.sh            # 前台运行，崩溃后每 5 秒重启
#   ./start.sh &          # 后台运行
#   PORT=8080 ./start.sh  # 自定义端口（也可用环境变量覆盖其它配置，见 README）

set -u

cd "$(dirname "$0")" || exit 1

# 检查 Node.js
if ! command -v node >/dev/null 2>&1; then
  echo "[错误] 未检测到 Node.js，请先安装（建议 18.x / 20.x LTS）"
  echo "        下载：https://nodejs.org/"
  exit 1
fi
echo "Node.js 版本: $(node -v)"

# 安装依赖（仅首次）
if [ ! -d "node_modules" ]; then
  echo "[1/2] 正在安装依赖..."
  if ! npm install --registry=https://registry.npmmirror.com; then
    echo "[错误] 依赖安装失败！"
    exit 1
  fi
  echo "[完成] 依赖安装成功"
else
  echo "[1/2] 依赖已存在，跳过安装"
fi

echo
echo "[2/2] 启动服务（崩溃自动重启）..."
echo "========================================"

while true; do
  echo "[$(date '+%F %T')] 启动 FTP Fetcher..."
  node server.js
  echo
  echo "[$(date '+%F %T')] 进程退出，5 秒后自动重启..."
  sleep 5
done

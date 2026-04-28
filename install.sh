#!/bin/bash
set -e
GREEN='\033[0;32m'
NC='\033[0m'
info() { echo -e "${GREEN}[+]${NC} $1"; }

REPO="https://raw.githubusercontent.com/ziren28/redis-tools/main"
INSTALL_DIR="/usr/local/bin"

info "安装 Redis 工具集..."

# 下载脚本
curl -sSL "${REPO}/redis-backup-baidu.sh" -o "${INSTALL_DIR}/redis-backup-baidu.sh"
curl -sSL "${REPO}/redis-sync.sh"         -o "${INSTALL_DIR}/redis-sync.sh"
curl -sSL "${REPO}/redis-export.py"       -o "${INSTALL_DIR}/redis-export.py"

chmod +x "${INSTALL_DIR}/redis-backup-baidu.sh"
chmod +x "${INSTALL_DIR}/redis-sync.sh"
chmod +x "${INSTALL_DIR}/redis-export.py"

# 安装 Python redis 依赖
pip3 install redis -q 2>/dev/null || pip install redis -q 2>/dev/null || true

info "安装完成!"
echo ""
echo "可用工具:"
echo "  redis-backup-baidu.sh  - 定时备份 Redis 到百度网盘"
echo "  redis-sync.sh          - 从网盘/远程恢复 Redis"
echo "  redis-export.py        - 导出/推送 Redis 数据 (Python)"
echo ""
echo "示例:"
echo "  redis-export.py dump -a 密码"
echo "  redis-export.py push 目标IP 6379 目标密码 -a 本机密码 --flush"
echo "  redis-sync.sh netdisk"

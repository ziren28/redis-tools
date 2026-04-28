#!/bin/bash
set -e

LOCAL_PASS="LTpEAr2fCcR3ShJm9QvSoHWVPS19FoR1"
REDIS_DIR="/var/lib/redis"
REDIS_DBF="dump.rdb"
NETDISK_DIR="/servers_data/redis/35.215.165.233"
TMP_DIR="/tmp/redis_restore"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
err()   { echo -e "${RED}[✗]${NC} $1"; }
title() { echo -e "\n${CYAN}===== $1 =====${NC}\n"; }

usage() {
    echo ""
    echo "Redis 同步恢复工具"
    echo ""
    echo "用法:"
    echo "  redis-sync.sh netdisk [文件名]       从百度网盘恢复"
    echo "  redis-sync.sh remote <host> <port> <password>  从远程Redis同步"
    echo "  redis-sync.sh list                   列出网盘备份"
    echo "  redis-sync.sh status                 查看本地Redis状态"
    echo ""
    echo "示例:"
    echo "  redis-sync.sh netdisk                          # 交互选择网盘备份"
    echo "  redis-sync.sh netdisk redis_dump_20260427.rdb  # 指定文件恢复"
    echo "  redis-sync.sh remote 8.218.250.240 6379 mypass # 从远程同步"
    echo ""
}

do_status() {
    title "本地 Redis 状态"
    /usr/bin/redis-cli -a "$LOCAL_PASS" INFO server 2>/dev/null | grep -E 'redis_version|uptime|tcp_port'
    echo ""
    /usr/bin/redis-cli -a "$LOCAL_PASS" INFO keyspace 2>/dev/null | grep -v '#'
    echo ""
    /usr/bin/redis-cli -a "$LOCAL_PASS" DBSIZE 2>/dev/null
    echo ""
    ls -lh "${REDIS_DIR}/${REDIS_DBF}" 2>/dev/null && info "RDB 文件: ${REDIS_DIR}/${REDIS_DBF}"
}

do_list() {
    title "百度网盘备份列表"
    /usr/local/bin/BaiduPCS-Go ls "$NETDISK_DIR" 2>/dev/null
}

confirm_restore() {
    warn "即将用备份数据覆盖当前 Redis 数据!"
    echo -n "确认继续? (yes/no): "
    read answer
    if [ "$answer" != "yes" ]; then
        err "已取消"
        exit 1
    fi
}

restore_rdb() {
    local rdb_file="$1"
    title "恢复 RDB 到 Redis"

    info "停止 Redis..."
    systemctl stop redis-server
    sleep 1

    info "备份当前数据..."
    cp -f "${REDIS_DIR}/${REDIS_DBF}" "${REDIS_DIR}/${REDIS_DBF}.bak.$(date +%s)" 2>/dev/null || true

    info "替换 RDB 文件..."
    cp -f "$rdb_file" "${REDIS_DIR}/${REDIS_DBF}"
    chown redis:redis "${REDIS_DIR}/${REDIS_DBF}"
    chmod 660 "${REDIS_DIR}/${REDIS_DBF}"

    info "启动 Redis..."
    systemctl start redis-server
    sleep 2

    if systemctl is-active --quiet redis-server; then
        info "Redis 已启动"
        /usr/bin/redis-cli -a "$LOCAL_PASS" DBSIZE 2>/dev/null
        info "恢复完成!"
    else
        err "Redis 启动失败，正在回滚..."
        LATEST_BAK=$(ls -t ${REDIS_DIR}/${REDIS_DBF}.bak.* 2>/dev/null | head -1)
        if [ -n "$LATEST_BAK" ]; then
            cp -f "$LATEST_BAK" "${REDIS_DIR}/${REDIS_DBF}"
            chown redis:redis "${REDIS_DIR}/${REDIS_DBF}"
            systemctl start redis-server
            warn "已回滚到之前的数据"
        fi
        exit 1
    fi
}

do_netdisk() {
    local target_file="$1"
    mkdir -p "$TMP_DIR"

    if [ -z "$target_file" ]; then
        title "选择网盘备份"
        FILELIST=$(/usr/local/bin/BaiduPCS-Go ls "$NETDISK_DIR" 2>/dev/null | grep 'redis_dump_' | awk '{print NR, $2, $3, $4, $NF}')
        if [ -z "$FILELIST" ]; then
            err "网盘没有备份文件"
            exit 1
        fi
        echo "$FILELIST"
        echo ""
        echo -n "输入序号 (默认最新): "
        read choice

        if [ -z "$choice" ]; then
            target_file=$(echo "$FILELIST" | tail -1 | awk '{print $NF}')
        else
            target_file=$(echo "$FILELIST" | sed -n "${choice}p" | awk '{print $NF}')
        fi

        if [ -z "$target_file" ]; then
            err "无效选择"
            exit 1
        fi
    fi

    info "选择的备份: $target_file"
    confirm_restore

    title "下载备份文件"
    /usr/local/bin/BaiduPCS-Go download "${NETDISK_DIR}/${target_file}" --saveto "$TMP_DIR" 2>&1
    
    if [ ! -f "${TMP_DIR}/${target_file}" ]; then
        err "下载失败"
        exit 1
    fi
    info "下载完成: ${TMP_DIR}/${target_file}"

    restore_rdb "${TMP_DIR}/${target_file}"
    rm -f "${TMP_DIR}/${target_file}"
}

do_remote() {
    local rhost="$1"
    local rport="${2:-6379}"
    local rpass="$3"

    if [ -z "$rhost" ]; then
        err "请提供远程 Redis 地址"
        usage
        exit 1
    fi

    title "从远程 Redis 同步"
    info "源: ${rhost}:${rport}"

    # 测试连接
    if [ -n "$rpass" ]; then
        PONG=$(/usr/bin/redis-cli -h "$rhost" -p "$rport" -a "$rpass" PING 2>/dev/null)
    else
        PONG=$(/usr/bin/redis-cli -h "$rhost" -p "$rport" PING 2>/dev/null)
    fi

    if [ "$PONG" != "PONG" ]; then
        err "无法连接远程 Redis: ${rhost}:${rport}"
        exit 1
    fi
    info "远程连接成功"

    # 显示远程信息
    if [ -n "$rpass" ]; then
        REMOTE_SIZE=$(/usr/bin/redis-cli -h "$rhost" -p "$rport" -a "$rpass" DBSIZE 2>/dev/null)
    else
        REMOTE_SIZE=$(/usr/bin/redis-cli -h "$rhost" -p "$rport" DBSIZE 2>/dev/null)
    fi
    info "远程数据: $REMOTE_SIZE"

    confirm_restore

    mkdir -p "$TMP_DIR"

    # 触发远程 BGSAVE 并下载 RDB
    info "触发远程 BGSAVE..."
    if [ -n "$rpass" ]; then
        /usr/bin/redis-cli -h "$rhost" -p "$rport" -a "$rpass" BGSAVE 2>/dev/null
    else
        /usr/bin/redis-cli -h "$rhost" -p "$rport" BGSAVE 2>/dev/null
    fi
    sleep 3

    # 用 SYNC 方式通过 /usr/bin/redis-cli 导出，或者逐 key 同步
    info "开始同步数据 (逐 key 迁移)..."

    # 清空本地
    /usr/bin/redis-cli -a "$LOCAL_PASS" FLUSHALL 2>/dev/null
    info "已清空本地数据"

    # 获取所有 key 并迁移
    local count=0
    local errors=0

    if [ -n "$rpass" ]; then
        AUTH_ARGS="-a $rpass"
    else
        AUTH_ARGS=""
    fi

    # 使用 SCAN 遍历所有 key，逐个 DUMP/RESTORE
    local cursor=0
    while true; do
        SCAN_RESULT=$(/usr/bin/redis-cli -h "$rhost" -p "$rport" $AUTH_ARGS SCAN $cursor COUNT 100 2>/dev/null)
        cursor=$(echo "$SCAN_RESULT" | head -1)
        KEYS=$(echo "$SCAN_RESULT" | tail -n +2)

        for key in $KEYS; do
            [ -z "$key" ] && continue
            # 获取 TTL
            TTL=$(/usr/bin/redis-cli -h "$rhost" -p "$rport" $AUTH_ARGS PTTL "$key" 2>/dev/null)
            [ "$TTL" -lt 0 ] 2>/dev/null && TTL=0

            # DUMP 序列化
            DUMPED=$(/usr/bin/redis-cli -h "$rhost" -p "$rport" $AUTH_ARGS DUMP "$key" 2>/dev/null)

            if [ -n "$DUMPED" ]; then
                echo "$DUMPED" | /usr/bin/redis-cli -a "$LOCAL_PASS" -x RESTORE "$key" "$TTL" 2>/dev/null
                if [ $? -eq 0 ]; then
                    count=$((count+1))
                else
                    errors=$((errors+1))
                fi
            fi
        done

        [ "$cursor" = "0" ] && break
    done

    echo ""
    info "同步完成: 成功 ${count} 个 key, 失败 ${errors} 个"
    /usr/bin/redis-cli -a "$LOCAL_PASS" DBSIZE 2>/dev/null
}

# 主入口
case "${1}" in
    netdisk|n)
        do_netdisk "$2"
        ;;
    remote|r)
        do_remote "$2" "$3" "$4"
        ;;
    list|l)
        do_list
        ;;
    status|s)
        do_status
        ;;
    *)
        usage
        ;;
esac

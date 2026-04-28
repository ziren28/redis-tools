#!/bin/bash
BACKUP_DIR="/tmp/redis_backup"
REMOTE_DIR="/servers_data/redis/35.215.165.233"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="redis_dump_${DATE}.rdb"
LOG="/var/log/redis-backup.log"

echo "[$(date)] 开始备份..." >> "$LOG"

# 1. 触发 Redis BGSAVE
/usr/bin/redis-cli -a LTpEAr2fCcR3ShJm9QvSoHWVPS19FoR1 BGSAVE >> "$LOG" 2>&1
sleep 3

# 2. 复制 dump.rdb
mkdir -p "$BACKUP_DIR"
cp /var/lib/redis/dump.rdb "${BACKUP_DIR}/${BACKUP_FILE}"

# 3. 上传到百度网盘
/usr/local/bin/BaiduPCS-Go upload "${BACKUP_DIR}/${BACKUP_FILE}" "${REMOTE_DIR}/" >> "$LOG" 2>&1

if [ $? -eq 0 ]; then
    echo "[$(date)] 上传成功: ${BACKUP_FILE}" >> "$LOG"
else
    echo "[$(date)] 上传失败!" >> "$LOG"
fi

# 4. 清理本地临时文件
rm -f "${BACKUP_DIR}/${BACKUP_FILE}"

# 5. 保留网盘最近 30 个备份，删除旧的
REMOTE_FILES=$(/usr/local/bin/BaiduPCS-Go ls "${REMOTE_DIR}" 2>/dev/null | grep 'redis_dump_' | awk '{print $NF}' | sort -r)
COUNT=0
echo "$REMOTE_FILES" | while read f; do
    COUNT=$((COUNT+1))
    if [ $COUNT -gt 30 ]; then
        /usr/local/bin/BaiduPCS-Go rm "${REMOTE_DIR}/${f}" >> "$LOG" 2>&1
        echo "[$(date)] 已删除旧备份: ${f}" >> "$LOG"
    fi
done

echo "[$(date)] 备份完成" >> "$LOG"
echo "---" >> "$LOG"

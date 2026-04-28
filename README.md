# Redis Tools

Redis 数据备份、导出、同步工具集。支持直连推送、S3 (MinIO) 中转、百度网盘备份，以及应用配置 (ENCRYPTION_KEY) 自动同步。

## 一键安装

```bash
curl -sSL https://raw.githubusercontent.com/ziren28/redis-tools/master/install.sh | sudo bash
```

## 工具概览

| 工具 | 功能 | 场景 |
|------|------|------|
| `redis-export.py` | 导出 / 推送 / S3 中转同步 | 跨服务器迁移、数据同步 |
| `redis-sync.sh` | 从百度网盘或远程 Redis 恢复 | 灾难恢复、数据回滚 |
| `redis-backup-baidu.sh` | 定时备份 RDB 到百度网盘 | 自动备份 (cron) |

---

## redis-export.py

核心同步工具，支持三种模式。

### 传输模式对比

| 模式 | 命令 | 原理 | 速度 | 适用场景 |
|------|------|------|------|----------|
| S3 中转 | `--via-s3` | BGSAVE → S3 → 目标拉取 RDB | 快 (秒级) | **推荐**，跨区域迁移 |
| 直连推送 | 默认 | 逐 key DUMP/RESTORE | 慢 | 小数据量、指定 key |
| 文件导出 | `dump` | BGSAVE 导出本地 | - | 手动备份 |

### S3 中转同步 (推荐)

通过 S3/MinIO 中转 RDB 文件，速度快、数据完整。

```bash
# 全量同步 (推荐)
sudo redis-export.py push <目标IP> <端口> --via-s3 --flush -y

# 同步 Redis + 应用配置 (ENCRYPTION_KEY 等)
sudo redis-export.py push <目标IP> <端口> --via-s3 --flush -y --sync-env

# 指定 SSH 连接信息
sudo redis-export.py push <目标IP> <端口> --via-s3 --flush -y --sync-env \
  --ssh-user <用户名> --ssh-key ~/.ssh/id_ed25519
```

流程:
```
源 Redis ──BGSAVE──→ RDB 文件 ──上传──→ S3/MinIO
                                           │
目标服务器 ←──SSH 执行──┐                    │
  ├─ 从 S3 下载 RDB    ←────────────────────┘
  ├─ 停止 Redis
  ├─ 替换 dump.rdb
  ├─ 启动 Redis
  ├─ 同步 .env (可选)
  └─ 重启应用 (可选)
```

### 直连推送

逐 key 推送，支持按模式过滤。

```bash
# 全量推送
sudo redis-export.py push <目标IP> <端口> [目标密码] --flush -y

# 源 Redis 有密码
sudo redis-export.py push <目标IP> <端口> [目标密码] --src-auth <源密码> --flush -y

# 只推送匹配的 key
sudo redis-export.py push <目标IP> <端口> -k "apikey:*" -y

# 先统计不执行
sudo redis-export.py push <目标IP> <端口> --dry-run
```

### 导出到文件

```bash
# 导出 RDB (全量)
sudo redis-export.py dump

# 导出指定 key 为 JSON
sudo redis-export.py dump -k "user:*" -o /tmp/users.rdb

# 导出并上传百度网盘
sudo redis-export.py upload
```

### 全部参数

```
公共参数:
  --src-host          源 Redis 地址 (默认: 127.0.0.1)
  --src-port          源 Redis 端口 (默认: 6379)
  --src-auth          源 Redis 密码
  --src-db            数据库编号 (默认: 0)
  -k, --keys          Key 匹配模式 (默认: *)
  -o, --output        导出文件路径
  --dry-run           仅统计不执行

push 参数:
  dst_host            目标地址
  dst_port            目标端口
  dst_auth            目标密码 (可选)
  --flush             推送前清空目标
  -y, --yes           跳过确认
  --via-s3            通过 S3 中转 RDB
  --sync-env          同步 .env 配置 (ENCRYPTION_KEY 等)
  --app-dir           本地应用目录 (默认: /root/claude-relay-service/app)
  --remote-app-dir    远程应用目录
  --ssh-key           SSH 私钥路径
  --ssh-user          SSH 用户名 (默认: root)
  --s3-endpoint       S3 端点 (已内置默认值)
  --s3-access-key     S3 Access Key
  --s3-secret-key     S3 Secret Key
  --s3-bucket         S3 Bucket (默认: redis-sync)
```

---

## redis-sync.sh

从百度网盘或远程 Redis 恢复数据。

```bash
# 查看本地 Redis 状态
sudo redis-sync.sh status

# 列出网盘备份
sudo redis-sync.sh list

# 从网盘恢复 (交互选择)
sudo redis-sync.sh netdisk

# 从网盘恢复 (指定文件)
sudo redis-sync.sh netdisk redis_dump_20260427.rdb

# 从远程 Redis 恢复
sudo redis-sync.sh remote <IP> <端口> <密码>
```

---

## redis-backup-baidu.sh

定时备份 Redis RDB 到百度网盘，配合 cron 使用。

### 设置定时备份

```bash
# 每 5 分钟备份一次
(sudo crontab -l 2>/dev/null; echo '*/5 * * * * /usr/local/bin/redis-backup-baidu.sh') | sudo crontab -

# 每小时备份
(sudo crontab -l 2>/dev/null; echo '0 * * * * /usr/local/bin/redis-backup-baidu.sh') | sudo crontab -
```

### 备份策略

- 自动触发 `BGSAVE` 生成 RDB 快照
- 上传到百度网盘 `/servers_data/redis/<IP>/`
- 保留最近 30 个备份，自动清理旧文件
- 日志: `/var/log/redis-backup.log`

---

## 前置依赖

| 依赖 | 用途 | 安装 |
|------|------|------|
| Python 3 | redis-export.py | 系统自带 |
| python3-redis | Redis 连接 | `apt install python3-redis` |
| python3-boto3 | S3 中转 | `apt install python3-boto3` |
| BaiduPCS-Go | 百度网盘 | [GitHub](https://github.com/qjfoidnh/BaiduPCS-Go) |
| redis-cli | Redis 操作 | `apt install redis-tools` |

---

## 典型使用场景

### 场景 1: 跨区域服务器迁移

```bash
# 在源服务器上执行，一条命令完成 Redis + 应用配置迁移
sudo redis-export.py push 目标IP 6379 --via-s3 --flush -y --sync-env \
  --ssh-user admin --ssh-key ~/.ssh/id_ed25519
```

### 场景 2: 定时备份 + 灾难恢复

```bash
# 设置定时备份 (源服务器)
(sudo crontab -l; echo '*/5 * * * * /usr/local/bin/redis-backup-baidu.sh') | sudo crontab -

# 灾难恢复 (新服务器)
sudo redis-sync.sh netdisk
```

### 场景 3: 部分数据同步

```bash
# 只同步 API Key 相关数据
sudo redis-export.py push 目标IP 6379 -k "apikey:*" -y
```

## License

MIT

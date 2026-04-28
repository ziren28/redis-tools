#!/usr/bin/env python3
"""Redis 数据库导出同步工具"""
import argparse
import sys
import os
import subprocess
import time
from datetime import datetime

try:
    import redis
except ImportError:
    print("[!] 正在安装 redis-py...")
    for cmd in [
        [sys.executable, "-m", "pip", "install", "redis", "--break-system-packages", "-q"],
        [sys.executable, "-m", "pip", "install", "redis", "-q"],
        ["apt-get", "install", "-y", "python3-redis"],
    ]:
        try:
            subprocess.check_call(cmd, stderr=subprocess.DEVNULL)
            break
        except Exception:
            continue
    else:
        print("[-] 请手动安装: apt install python3-redis 或 pip install redis")
        sys.exit(1)
    import redis

GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
RED = "\033[0;31m"
CYAN = "\033[0;36m"
NC = "\033[0m"

def info(msg):  print(f"{GREEN}[+]{NC} {msg}")
def warn(msg):  print(f"{YELLOW}[!]{NC} {msg}")
def err(msg):   print(f"{RED}[-]{NC} {msg}")
def title(msg): print(f"\n{CYAN}===== {msg} ====={NC}\n")

def connect(host, port, password=None, db=0):
    r = redis.Redis(host=host, port=port, password=password or None, db=db, decode_responses=False)
    try:
        r.ping()
    except redis.ConnectionError:
        err(f"无法连接 Redis {host}:{port}")
        sys.exit(1)
    except redis.AuthenticationError:
        err(f"认证失败 {host}:{port}")
        sys.exit(1)
    return r

def show_info(r, label, host, port):
    title(f"{label} Redis 信息")
    info(f"地址: {host}:{port}")
    info(f"Key 数: {r.dbsize()}")
    ks = r.info("keyspace")
    for db_name, db_info in ks.items():
        if isinstance(db_info, dict):
            info(f"  {db_name}: keys={db_info.get('keys',0)}, expires={db_info.get('expires',0)}")

def do_dump(args):
    src = connect(args.src_host, args.src_port, args.src_auth, args.src_db)
    show_info(src, "源", args.src_host, args.src_port)

    output = args.output or f"/tmp/redis_export/redis_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.rdb"
    os.makedirs(os.path.dirname(output), exist_ok=True)

    if args.keys == "*" and not args.src_db:
        title("导出 RDB")
        info("触发 BGSAVE...")
        src.bgsave()
        time.sleep(3)
        rdb_dir = src.config_get("dir")["dir"]
        rdb_file = src.config_get("dbfilename")["dbfilename"]
        rdb_path = os.path.join(rdb_dir, rdb_file)
        subprocess.run(["cp", "-f", rdb_path, output])
        fsize = os.path.getsize(output)
        info(f"导出完成: {output} ({fsize/1024:.1f}KB)")
    else:
        title("按 Key 导出")
        import json
        count = 0
        data = []
        for key in src.scan_iter(match=args.keys, count=200):
            ktype = src.type(key).decode()
            ttl = src.ttl(key)
            key_str = key.decode(errors="replace")
            if args.dry_run:
                count += 1
                continue
            val = None
            if ktype == "string":
                v = src.get(key)
                val = v.decode(errors="replace") if v else ""
            elif ktype == "list":
                val = [v.decode(errors="replace") for v in src.lrange(key, 0, -1)]
            elif ktype == "set":
                val = [v.decode(errors="replace") for v in src.smembers(key)]
            elif ktype == "zset":
                val = [(v.decode(errors="replace"), s) for v, s in src.zrange(key, 0, -1, withscores=True)]
            elif ktype == "hash":
                val = {k.decode(errors="replace"): v.decode(errors="replace") for k, v in src.hgetall(key).items()}
            data.append({"key": key_str, "type": ktype, "ttl": ttl, "value": val})
            count += 1
        if not args.dry_run:
            output = output.replace(".rdb", ".json")
            with open(output, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            info(f"导出完成: {output} ({count} 个 key)")
        else:
            info(f"[dry-run] 匹配 {count} 个 key")

def sync_env(args):
    """同步本机 .env 配置到远程服务器"""
    app_dir = args.app_dir
    env_file = os.path.join(app_dir, ".env")

    if not os.path.exists(env_file):
        err(f"本地 .env 不存在: {env_file}")
        return False

    # 读取本地 .env 中需要同步的 key
    sync_keys = ["ENCRYPTION_KEY", "ADMIN_PASSWORD", "JWT_SECRET"]
    local_env = {}
    with open(env_file, "r") as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                if k in sync_keys:
                    local_env[k] = v

    if not local_env:
        warn("本地 .env 中没有找到需要同步的配置")
        return False

    title("同步应用配置")
    for k, v in local_env.items():
        info(f"本地 {k}={v[:8]}...{v[-4:]}" if len(v) > 12 else f"本地 {k}={v}")

    # SSH 到远程更新 .env
    ssh_key = args.ssh_key
    ssh_user = args.ssh_user
    dst_host = args.dst_host
    remote_app_dir = args.remote_app_dir

    ssh_base = ["ssh", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=10"]
    if ssh_key:
        ssh_base += ["-i", ssh_key]

    remote = f"{ssh_user}@{dst_host}"

    # 读取远程 .env
    result = subprocess.run(
        ssh_base + [remote, f"cat {remote_app_dir}/.env"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        err(f"无法读取远程 .env: {result.stderr.strip()}")
        return False

    remote_lines = result.stdout.strip().split("\n")
    updated = []
    changed = []
    for line in remote_lines:
        matched = False
        for k, v in local_env.items():
            if line.startswith(f"{k}="):
                old_v = line.split("=", 1)[1]
                if old_v != v:
                    changed.append(f"{k}: {old_v[:8]}... -> {v[:8]}...")
                updated.append(f"{k}={v}")
                matched = True
                break
        if not matched:
            updated.append(line)

    if not changed:
        info("远程配置已经一致，无需更新")
        return True

    for c in changed:
        warn(f"变更: {c}")

    # 写入远程 .env
    new_env = "\n".join(updated) + "\n"
    result = subprocess.run(
        ssh_base + [remote, f"sudo tee {remote_app_dir}/.env > /dev/null"],
        input=new_env, capture_output=True, text=True
    )
    if result.returncode != 0:
        err(f"写入远程 .env 失败: {result.stderr.strip()}")
        return False
    info("远程 .env 已更新")

    # 重启远程应用
    info("重启远程应用...")
    restart_cmds = [
        "sudo systemctl restart claude-relay-service 2>/dev/null",
        "|| sudo pm2 restart all 2>/dev/null",
        "|| (sudo kill $(pgrep -f 'node.*app.js') 2>/dev/null; sleep 1;",
        f"sudo bash -c 'cd {remote_app_dir} && nohup node src/app.js > /tmp/crs.log 2>&1 &')",
    ]
    subprocess.run(
        ssh_base + [remote, " ".join(restart_cmds)],
        capture_output=True
    )
    time.sleep(3)

    # 验证
    result = subprocess.run(
        ssh_base + [remote, "pgrep -f 'node.*app.js'"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        info("远程应用已重启")
    else:
        warn("远程应用可能未启动，请手动检查")

    return True

def do_push(args):
    src = connect(args.src_host, args.src_port, args.src_auth, args.src_db)
    dst = connect(args.dst_host, args.dst_port, args.dst_auth, args.src_db)

    show_info(src, "源", args.src_host, args.src_port)
    show_info(dst, "目标", args.dst_host, args.dst_port)

    if args.flush:
        warn("将在同步前清空目标!")
    if args.sync_env:
        warn("将同步 .env 配置 (ENCRYPTION_KEY 等)")

    if not args.yes:
        answer = input("确认推送? (yes/no): ")
        if answer != "yes":
            err("已取消")
            sys.exit(1)

    # 同步 .env 配置
    if args.sync_env:
        sync_env(args)

    if args.flush:
        info("清空目标...")
        dst.flushall()

    title("开始同步")
    count = 0
    errors = 0
    skipped = 0
    start_time = time.time()

    for key in src.scan_iter(match=args.keys, count=200):
        if args.dry_run:
            count += 1
            continue
        try:
            pttl = src.pttl(key)
            if pttl < 0:
                pttl = 0
            dumped = src.dump(key)
            if dumped is None:
                skipped += 1
                continue
            dst.restore(key, pttl, dumped, replace=True)
            count += 1
        except Exception:
            errors += 1

        total = count + errors + skipped
        if total % 500 == 0 and total > 0:
            elapsed = time.time() - start_time
            speed = total / elapsed if elapsed > 0 else 0
            print(f"\r  进度: {count} 成功, {errors} 失败, {skipped} 跳过 | {speed:.0f} keys/s  ", end="", flush=True)

    elapsed = time.time() - start_time
    print()
    if args.dry_run:
        info(f"[dry-run] 匹配 {count} 个 key")
    else:
        info(f"同步完成: 成功 {count}, 失败 {errors}, 跳过 {skipped} | 耗时 {elapsed:.1f}s")
        info(f"目标现在: {dst.dbsize()}")

def do_upload(args):
    do_dump(args)
    title("上传到百度网盘")
    baidupcs = "/usr/local/bin/BaiduPCS-Go"
    output = args.output or f"/tmp/redis_export/redis_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.rdb"
    ip = subprocess.getoutput("hostname -I").split()[0]
    remote = f"/servers_data/redis/{ip}"
    subprocess.run([baidupcs, "mkdir", remote], capture_output=True)
    result = subprocess.run([baidupcs, "upload", output, f"{remote}/"])
    if result.returncode == 0:
        info(f"已上传: {remote}/{os.path.basename(output)}")
    else:
        err("上传失败")
    os.remove(output)

def main():
    parser = argparse.ArgumentParser(
        description="Redis 数据库导出同步工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s dump
  %(prog)s dump --src-auth mypass -k "user:*"
  %(prog)s push 8.218.250.240 6379 targetpass --flush -y
  %(prog)s push 10.0.0.5 6379 --flush -y --sync-env
  %(prog)s push 10.0.0.5 6379 --flush -y --sync-env --ssh-key ~/.ssh/id_ed25519 --ssh-user admin
  %(prog)s upload --src-auth mypass
        """)

    sub = parser.add_subparsers(dest="command")

    # 公共参数
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--src-host", default="127.0.0.1", help="源 Redis 地址 (默认: 127.0.0.1)")
    common.add_argument("--src-port", type=int, default=6379, help="源 Redis 端口 (默认: 6379)")
    common.add_argument("--src-auth", default="", help="源 Redis 密码")
    common.add_argument("--src-db", type=int, default=0, help="数据库编号 (默认: 0)")
    common.add_argument("-k", "--keys", default="*", help="Key 匹配模式 (默认: *)")
    common.add_argument("-o", "--output", default="", help="导出文件路径")
    common.add_argument("--dry-run", action="store_true", help="仅统计不执行")

    # dump
    sub.add_parser("dump", parents=[common], help="导出到文件")

    # push
    push_p = sub.add_parser("push", parents=[common], help="推送到远程 Redis")
    push_p.add_argument("dst_host", help="目标地址")
    push_p.add_argument("dst_port", type=int, help="目标端口")
    push_p.add_argument("dst_auth", nargs="?", default="", help="目标密码")
    push_p.add_argument("--flush", action="store_true", help="推送前清空目标")
    push_p.add_argument("-y", "--yes", action="store_true", help="跳过确认")
    push_p.add_argument("--sync-env", action="store_true", help="同步 .env 配置 (ENCRYPTION_KEY 等)")
    push_p.add_argument("--app-dir", default="/root/claude-relay-service/app", help="本地应用目录 (默认: /root/claude-relay-service/app)")
    push_p.add_argument("--remote-app-dir", default="/root/claude-relay-service/app", help="远程应用目录 (默认: /root/claude-relay-service/app)")
    push_p.add_argument("--ssh-key", default="", help="SSH 私钥路径")
    push_p.add_argument("--ssh-user", default="root", help="SSH 用户名 (默认: root)")

    # upload
    sub.add_parser("upload", parents=[common], help="导出并上传网盘")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    if args.command == "dump":
        do_dump(args)
    elif args.command == "push":
        do_push(args)
    elif args.command == "upload":
        do_upload(args)

if __name__ == "__main__":
    main()

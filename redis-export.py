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

# ============ S3 工具 ============

S3_DEFAULT = {
    "endpoint": "http://8.218.250.240:9000",
    "access_key": "ff9cc7e6b4886076280e48b87b708dfb",
    "secret_key": "d431957e3b8090cf38d382571df03750127a0650680c68808cc67fd861df76af",
    "bucket": "redis-sync",
    "region": "us-east-1",
}

def get_s3_client(args=None):
    try:
        import boto3
    except ImportError:
        for cmd in [
            [sys.executable, "-m", "pip", "install", "boto3", "--break-system-packages", "-q"],
            [sys.executable, "-m", "pip", "install", "boto3", "-q"],
        ]:
            try:
                subprocess.check_call(cmd, stderr=subprocess.DEVNULL)
                break
            except Exception:
                continue
        import boto3

    from botocore.config import Config

    endpoint = getattr(args, "s3_endpoint", None) or S3_DEFAULT["endpoint"]
    access_key = getattr(args, "s3_access_key", None) or S3_DEFAULT["access_key"]
    secret_key = getattr(args, "s3_secret_key", None) or S3_DEFAULT["secret_key"]
    region = getattr(args, "s3_region", None) or S3_DEFAULT["region"]

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )
    return s3

def s3_ensure_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        try:
            s3.create_bucket(Bucket=bucket)
            info(f"创建 S3 Bucket: {bucket}")
        except Exception as e:
            err(f"创建 Bucket 失败: {e}")
            sys.exit(1)

def s3_upload(s3, local_path, bucket, key):
    fsize = os.path.getsize(local_path)
    info(f"上传到 S3: s3://{bucket}/{key} ({fsize/1024/1024:.2f}MB)")
    start = time.time()
    s3.upload_file(local_path, bucket, key)
    elapsed = time.time() - start
    speed = fsize / 1024 / 1024 / elapsed if elapsed > 0 else 0
    info(f"上传完成: {elapsed:.1f}s ({speed:.1f}MB/s)")

def s3_download(s3, bucket, key, local_path):
    info(f"从 S3 下载: s3://{bucket}/{key}")
    start = time.time()
    s3.download_file(bucket, key, local_path)
    elapsed = time.time() - start
    fsize = os.path.getsize(local_path)
    speed = fsize / 1024 / 1024 / elapsed if elapsed > 0 else 0
    info(f"下载完成: {fsize/1024/1024:.2f}MB, {elapsed:.1f}s ({speed:.1f}MB/s)")

# ============ Redis 连接 ============

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

# ============ SSH 工具 ============

def ssh_cmd(args, cmd):
    ssh_base = ["ssh", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=10"]
    if args.ssh_key:
        ssh_base += ["-i", args.ssh_key]
    remote = f"{args.ssh_user}@{args.dst_host}"
    return subprocess.run(ssh_base + [remote, cmd], capture_output=True, text=True)

def ssh_cmd_input(args, cmd, input_data):
    ssh_base = ["ssh", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=10"]
    if args.ssh_key:
        ssh_base += ["-i", args.ssh_key]
    remote = f"{args.ssh_user}@{args.dst_host}"
    return subprocess.run(ssh_base + [remote, cmd], input=input_data, capture_output=True, text=True)

# ============ .env 同步 ============

def sync_env(args):
    app_dir = args.app_dir
    env_file = os.path.join(app_dir, ".env")

    if not os.path.exists(env_file):
        err(f"本地 .env 不存在: {env_file}")
        return False

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

    remote_app_dir = args.remote_app_dir
    result = ssh_cmd(args, f"cat {remote_app_dir}/.env")
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

    new_env = "\n".join(updated) + "\n"
    result = ssh_cmd_input(args, f"sudo tee {remote_app_dir}/.env > /dev/null", new_env)
    if result.returncode != 0:
        err(f"写入远程 .env 失败: {result.stderr.strip()}")
        return False
    info("远程 .env 已更新")

    info("重启远程应用...")
    restart = (
        f"sudo systemctl restart claude-relay-service 2>/dev/null"
        f" || sudo pm2 restart all 2>/dev/null"
        f" || (sudo kill $(pgrep -f 'node.*app.js') 2>/dev/null; sleep 1;"
        f" sudo bash -c 'cd {remote_app_dir} && nohup node src/app.js > /tmp/crs.log 2>&1 &')"
    )
    ssh_cmd(args, restart)
    time.sleep(3)

    result = ssh_cmd(args, "pgrep -f 'node.*app.js'")
    if result.returncode == 0:
        info("远程应用已重启")
    else:
        warn("远程应用可能未启动，请手动检查")
    return True

# ============ dump ============

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

# ============ push ============

def do_push(args):
    src = connect(args.src_host, args.src_port, args.src_auth, args.src_db)

    show_info(src, "源", args.src_host, args.src_port)

    if args.via_s3:
        info(f"传输模式: S3 中转 ({S3_DEFAULT['endpoint']})")
    else:
        dst = connect(args.dst_host, args.dst_port, args.dst_auth, args.src_db)
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

    # 同步 .env
    if args.sync_env:
        sync_env(args)

    # ---- S3 中转模式 ----
    if args.via_s3:
        do_push_via_s3(args, src)
        return

    # ---- 直连模式 ----
    if args.flush:
        info("清空目标...")
        dst.flushall()

    title("开始同步 (直连模式)")
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


def do_push_via_s3(args, src):
    """通过 S3 中转 RDB 文件实现全量同步"""
    bucket = getattr(args, "s3_bucket", None) or S3_DEFAULT["bucket"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"redis_sync/{timestamp}/dump.rdb"
    local_rdb = f"/tmp/redis_sync_{timestamp}.rdb"
    env_s3_key = f"redis_sync/{timestamp}/env.txt"

    # 1. 导出 RDB
    title("Step 1: 导出 RDB")
    info("触发 BGSAVE...")
    src.bgsave()
    time.sleep(3)
    rdb_dir = src.config_get("dir")["dir"]
    rdb_file = src.config_get("dbfilename")["dbfilename"]
    rdb_path = os.path.join(rdb_dir, rdb_file)
    subprocess.run(["cp", "-f", rdb_path, local_rdb])
    fsize = os.path.getsize(local_rdb)
    info(f"RDB: {local_rdb} ({fsize/1024/1024:.2f}MB), Keys: {src.dbsize()}")

    # 2. 上传到 S3
    title("Step 2: 上传到 S3")
    s3 = get_s3_client(args)
    s3_ensure_bucket(s3, bucket)
    s3_upload(s3, local_rdb, bucket, s3_key)

    # 同时上传 .env 到 S3 (如果需要)
    if args.sync_env:
        env_file = os.path.join(args.app_dir, ".env")
        if os.path.exists(env_file):
            s3.upload_file(env_file, bucket, env_s3_key)
            info(f".env 已上传到 S3: s3://{bucket}/{env_s3_key}")

    # 3. SSH 到目标服务器拉取并恢复
    title("Step 3: 目标服务器拉取并恢复")

    # 生成目标服务器上执行的 Python 脚本
    restore_script = f'''
import subprocess, sys, os, time
try:
    import boto3
except ImportError:
    for cmd in [
        [sys.executable, "-m", "pip", "install", "boto3", "--break-system-packages", "-q"],
        [sys.executable, "-m", "pip", "install", "boto3", "-q"],
    ]:
        try:
            subprocess.check_call(cmd, stderr=subprocess.DEVNULL)
            break
        except Exception:
            continue
    import boto3
from botocore.config import Config

s3 = boto3.client("s3",
    endpoint_url="{getattr(args, 's3_endpoint', None) or S3_DEFAULT['endpoint']}",
    aws_access_key_id="{getattr(args, 's3_access_key', None) or S3_DEFAULT['access_key']}",
    aws_secret_access_key="{getattr(args, 's3_secret_key', None) or S3_DEFAULT['secret_key']}",
    region_name="{getattr(args, 's3_region', None) or S3_DEFAULT['region']}",
    config=Config(signature_version="s3v4"),
)

local_rdb = "/tmp/redis_restore_s3.rdb"
print("[+] 从 S3 下载 RDB...")
s3.download_file("{bucket}", "{s3_key}", local_rdb)
fsize = os.path.getsize(local_rdb)
print(f"[+] 下载完成: {{fsize/1024/1024:.2f}}MB")

sync_env = {args.sync_env}
if sync_env:
    print("[+] 从 S3 下载 .env...")
    try:
        s3.download_file("{bucket}", "{env_s3_key}", "/tmp/env_restore.txt")
        remote_app_dir = "{args.remote_app_dir}"
        subprocess.run(["cp", "-f", "/tmp/env_restore.txt", f"{{remote_app_dir}}/.env"])
        print("[+] .env 已更新")
    except Exception as e:
        print(f"[!] .env 下载失败: {{e}}")

print("[+] 停止 Redis...")
subprocess.run(["systemctl", "stop", "redis-server"])
time.sleep(1)

print("[+] 替换 RDB...")
subprocess.run(["cp", "-f", "/var/lib/redis/dump.rdb", "/var/lib/redis/dump.rdb.bak"])
subprocess.run(["cp", "-f", local_rdb, "/var/lib/redis/dump.rdb"])
subprocess.run(["chown", "redis:redis", "/var/lib/redis/dump.rdb"])
subprocess.run(["chmod", "660", "/var/lib/redis/dump.rdb"])

print("[+] 启动 Redis...")
subprocess.run(["systemctl", "start", "redis-server"])
time.sleep(2)

import redis as r
c = r.Redis()
print(f"[+] 恢复完成! Keys: {{c.dbsize()}}")

if sync_env:
    print("[+] 重启应用...")
    subprocess.run("kill $(pgrep -f 'node.*app.js') 2>/dev/null; sleep 1; cd {args.remote_app_dir} && nohup node src/app.js > /tmp/crs.log 2>&1 &", shell=True)
    time.sleep(2)
    ret = subprocess.run(["pgrep", "-f", "node.*app.js"], capture_output=True)
    if ret.returncode == 0:
        print("[+] 应用已重启")
    else:
        print("[!] 应用可能未启动，请手动检查")

os.remove(local_rdb)
print("[+] 清理完成")
'''

    # 写入临时文件上传到 S3
    restore_script_path = f"/tmp/redis_restore_script_{timestamp}.py"
    with open(restore_script_path, "w") as f:
        f.write(restore_script)
    s3.upload_file(restore_script_path, bucket, f"redis_sync/{timestamp}/restore.py")

    # SSH 到目标执行
    info("SSH 到目标服务器执行恢复...")
    dl_and_run = (
        f"python3 -c \""
        f"import boto3; from botocore.config import Config; "
        f"s3=boto3.client('s3',endpoint_url='{getattr(args, 's3_endpoint', None) or S3_DEFAULT['endpoint']}', "
        f"aws_access_key_id='{getattr(args, 's3_access_key', None) or S3_DEFAULT['access_key']}', "
        f"aws_secret_access_key='{getattr(args, 's3_secret_key', None) or S3_DEFAULT['secret_key']}', "
        f"region_name='{getattr(args, 's3_region', None) or S3_DEFAULT['region']}', "
        f"config=Config(signature_version='s3v4')); "
        f"s3.download_file('{bucket}','{f'redis_sync/{timestamp}/restore.py'}','/tmp/restore.py')\""
        f" && sudo python3 /tmp/restore.py && rm -f /tmp/restore.py"
    )
    result = ssh_cmd(args, dl_and_run)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    # 4. 清理
    title("Step 4: 清理")
    os.remove(local_rdb)
    os.remove(restore_script_path)
    for k in [s3_key, env_s3_key, f"redis_sync/{timestamp}/restore.py"]:
        try:
            s3.delete_object(Bucket=bucket, Key=k)
        except Exception:
            pass
    info("S3 临时文件已清理")
    info("全量同步完成!")

# ============ upload ============

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

# ============ main ============

def main():
    parser = argparse.ArgumentParser(
        description="Redis 数据库导出同步工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s dump
  %(prog)s push 10.0.0.5 6379 --flush -y                          # 直连推送
  %(prog)s push 10.0.0.5 6379 --flush -y --via-s3                  # S3 中转 (推荐)
  %(prog)s push 10.0.0.5 6379 --flush -y --via-s3 --sync-env       # S3 中转 + 同步配置
  %(prog)s upload --src-auth mypass                                 # 导出上传网盘
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
    # S3 中转
    push_p.add_argument("--via-s3", action="store_true", help="通过 S3 中转 RDB (推荐)")
    push_p.add_argument("--s3-endpoint", default="", help="S3 端点")
    push_p.add_argument("--s3-access-key", default="", help="S3 Access Key")
    push_p.add_argument("--s3-secret-key", default="", help="S3 Secret Key")
    push_p.add_argument("--s3-bucket", default="", help="S3 Bucket (默认: redis-sync)")
    push_p.add_argument("--s3-region", default="", help="S3 Region")
    # .env 同步
    push_p.add_argument("--sync-env", action="store_true", help="同步 .env 配置 (ENCRYPTION_KEY 等)")
    push_p.add_argument("--app-dir", default="/root/claude-relay-service/app", help="本地应用目录")
    push_p.add_argument("--remote-app-dir", default="/root/claude-relay-service/app", help="远程应用目录")
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

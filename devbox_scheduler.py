#!/usr/bin/env python3
"""DevBox scheduler: Run devbox.py at UTC 00:00 and 12:00 daily using subprocess"""

import subprocess
import logging
import time
import argparse
import os
import threading
import platform
from datetime import datetime, timezone
import sys

# Configure logging (both console and file)
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Log')
os.makedirs(log_dir, exist_ok=True)
log_filename = datetime.now(timezone.utc).strftime('%Y%m%d%H') + '.log'
log_file = os.path.join(log_dir, log_filename)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s UTC - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Set UTC timezone for logging
import time as _time
for _h in logging.getLogger().handlers:
    _fmt = logging.Formatter('%(asctime)s UTC - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    _fmt.converter = _time.gmtime
    _h.setFormatter(_fmt)
    _h.setFormatter(_fmt)


def run_devbox(csv_file=None):
    """Execute devbox.py in a separate console window and return process"""
    logging.info("开始执行 devbox.py...")
    try:
        cmd = [sys.executable, 'google_crawl_nowcast_scheduled_azap_devbox.py', '--mode', 'devbox']
        if csv_file:
            cmd.extend(['--csv_file', csv_file])
        work_dir = os.path.dirname(os.path.abspath(__file__))
        
        if platform.system() == 'Windows':
            # Windows: 在新窗口中打开
            process = subprocess.Popen(cmd, cwd=work_dir, creationflags=subprocess.CREATE_NEW_CONSOLE)
        else:
            # Linux/Mac: 在新终端中打开
            process = subprocess.Popen(['xterm', '-e'] + cmd, cwd=work_dir)
        
        logging.info("✓ devbox.py 已在新窗口启动\n")
        return process
    except Exception as e:
        logging.info(f"✗ 启动失败: {e}\n")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DevBox Scheduler")
    parser.add_argument('--csv_file', type=str, help='Path to the station list CSV file')
    args = parser.parse_args()
    
    logging.info("✓ DevBox 调度器已启动（每天 UTC 00:00 和 12:00 执行）\n")
    
    last_hour = -1
    active_processes = []  # 追踪所有活跃的子进程
    
    def run_and_track():
        process = run_devbox(csv_file=args.csv_file)
        if process:
            active_processes.append(process)
    
    try:
        while True:
            now_utc = datetime.now(timezone.utc)
            hour = now_utc.hour
            
            # 每当时间进入 00:00 或 12:00 时执行一次
            if hour in (0, 12) and hour != last_hour:
                logging.info(f"✓ 执行时间: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                # 在后台线程中执行，不阻塞主循环
                thread = threading.Thread(target=run_and_track, daemon=True)
                thread.start()
                last_hour = hour
            
            time.sleep(30)  # 每分钟检查一次
    
    except KeyboardInterrupt:
        logging.info("\n✓ 收到停止信号，正在杀死所有子进程...")
        for process in active_processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                try:
                    process.kill()
                except:
                    pass
        logging.info("✓ 程序已停止")

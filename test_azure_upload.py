#!/usr/bin/env python3
"""Test script for Azure upload functionality"""
import os
import sys
import json
import tempfile
from pathlib import Path

# 设置环境变量（在导入azure_wrapper之前）
os.environ['wxforecasting_sas'] = "?sv=2025-07-05&spr=https&st=2026-01-22T02%3A22%3A14Z&se=2026-01-29T02%3A22%3A00Z&skoid=d5712fa3-c8c5-4a5c-9197-294dadf5b3e8&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2026-01-22T02%3A22%3A14Z&ske=2026-01-29T02%3A22%3A00Z&sks=b&skv=2025-07-05&sr=c&sp=rwl&sig=fZtwi3qSxLs5kCi6iauAqQDlj%2B0KNqP5e1U1Tm8YIhU%3D"

from azure_wrapper import get_wxforecasting_azure_wrapper


if __name__ == "__main__":
    # 获取 Azure 客户端对象
    wrapper = get_wxforecasting_azure_wrapper()
    
    # 本地文件路径
    local_file = Path(__file__).parent / "Crawled" / "2026012202" / "nowcast_08181_20260122024333.json"
    
    if not local_file.exists():
        print(f"❌ 文件不存在: {local_file}")
        sys.exit(1)
    
    print(f"📤 上传文件: {local_file.name}")
    print(f"   本地路径: {local_file}")
    
    try:
        # 上传到 Azure
        wrapper.upload_file(
            local_path=str(local_file),
            container_path="users/v-zhanghang/lib/data/GoogleNowcast/nowcast_08181_20260122024333.json"
        )
        print("✅ 上传成功！")
    except Exception as e:
        print(f"❌ 上传失败: {e}")
        sys.exit(1)

#!/usr/bin/env python3
"""Test uploading a folder to Azure Blob"""
import os
import sys
from pathlib import Path

# 设置环境变量
os.environ['wxforecasting_sas'] = "?sv=2025-07-05&spr=https&st=2026-01-22T02%3A22%3A14Z&se=2026-01-29T02%3A22%3A00Z&skoid=d5712fa3-c8c5-4a5c-9197-294dadf5b3e8&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2026-01-22T02%3A22%3A14Z&ske=2026-01-29T02%3A22%3A00Z&sks=b&skv=2025-07-05&sr=c&sp=rwl&sig=fZtwi3qSxLs5kCi6iauAqQDlj%2B0KNqP5e1U1Tm8YIhU%3D"

from azure_wrapper import get_wxforecasting_azure_wrapper

if __name__ == "__main__":
    # 本地文件夹
    local_folder = Path(__file__).parent / "Crawled" / "2026012202"
    
    if not local_folder.exists():
        print(f"❌ 文件夹不存在: {local_folder}")
        sys.exit(1)
    
    print(f"📁 上传文件夹: {local_folder}")
    print()
    
    # 获取 Azure 客户端
    wrapper = get_wxforecasting_azure_wrapper()
    
    # 上传整个文件夹
    success, total = wrapper.upload_folder(
        local_folder=str(local_folder),
        container_prefix="users/v-zhanghang/lib/data/GoogleNowcast/2026012202",
        show_progress=True
    )
    
    print()
    print("=" * 60)
    print(f"✅ 上传完成: {success}/{total} 文件成功")
    print("=" * 60)

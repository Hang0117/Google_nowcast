#!/usr/bin/env python3
"""Test uploading a folder to Azure Blob"""
import os
import sys
from pathlib import Path


from azure_wrapper import get_wxforecasting_azure_wrapper

if __name__ == "__main__":
    # 本地文件夹
    local_folder = Path(__file__).parent / "GoogleNowcastHTML" / "2026012701"
    
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
        container_prefix="GoogleNowcast/GoogleNowcastHTML/2026012701_devbox",
        show_progress=True
    )
    
    print()
    print("=" * 60)
    print(f"✅ 上传完成: {success}/{total} 文件成功")
    print("=" * 60)

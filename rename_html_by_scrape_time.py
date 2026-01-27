#!/usr/bin/env python3
"""
重命名 GoogleNowcastHTML 中的文件，根据对应 Crawled 目录下 JSON 文件的 scrape_time
"""
from pathlib import Path
import json
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

base_dir = Path(r'Q:\Code\Google_nowcast')
html_base = base_dir / 'GoogleNowcastHTML'
crawled_base = base_dir / 'Crawled'

if not html_base.exists():
    logging.error(f"GoogleNowcastHTML 目录不存在: {html_base}")
    exit(1)

if not crawled_base.exists():
    logging.error(f"Crawled 目录不存在: {crawled_base}")
    exit(1)

# 遍历 GoogleNowcastHTML 下的每个日期文件夹
html_folders = sorted([d for d in html_base.iterdir() if d.is_dir()])
logging.info(f"找到 {len(html_folders)} 个日期文件夹")

total_renamed = 0
total_failed = 0
total_skipped = 0

for folder in html_folders:
    folder_date = folder.name
    logging.info(f"\n处理文件夹: {folder_date}")
    
    # 对应的 Crawled 目录
    crawled_folder = crawled_base / folder_date
    if not crawled_folder.exists():
        logging.warning(f"  对应的 Crawled 目录不存在: {crawled_folder}")
        continue
    
    # 遍历该文件夹下的所有 HTML 文件
    html_files = list(folder.glob('*.html'))
    logging.info(f"  找到 {len(html_files)} 个 HTML 文件")
    
    for html_file in html_files:
        html_name = html_file.name
        # 格式: {city_id}_{timestamp}.html
        # 提取 city_id
        parts = html_name.replace('.html', '').rsplit('_', 1)
        if len(parts) != 2:
            logging.warning(f"  跳过 {html_name}: 无法解析文件名")
            total_skipped += 1
            continue
        
        city_id = parts[0]
        old_timestamp = parts[1]
        
        # 查找对应的 JSON 文件
        json_files = list(crawled_folder.glob(f'nowcast_{city_id}_*.json'))
        
        if not json_files:
            logging.warning(f"  跳过 {html_name}: 找不到对应的 JSON 文件")
            total_skipped += 1
            continue
        
        if len(json_files) > 1:
            logging.warning(f"  {html_name}: 找到 {len(json_files)} 个 JSON 文件，使用最新的")
            # 使用最新的文件
            json_file = max(json_files, key=lambda p: p.stat().st_mtime)
        else:
            json_file = json_files[0]
        
        try:
            # 读取 JSON 文件
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            scrape_time_str = data.get('scrape_time')
            if not scrape_time_str:
                logging.warning(f"  {html_name}: JSON 中没有 scrape_time 字段")
                total_skipped += 1
                continue
            
            # 解析 scrape_time 并转换为 YYYYMMDDHHmmss 格式
            # 格式如: "2026-01-27T02:44:12.478818+00:00"
            try:
                # 移除时区部分
                if '+' in scrape_time_str:
                    scrape_time_str = scrape_time_str.split('+')[0]
                elif scrape_time_str.endswith('Z'):
                    scrape_time_str = scrape_time_str[:-1]
                
                dt = datetime.fromisoformat(scrape_time_str)
                new_timestamp = dt.strftime('%Y%m%d%H%M%S')
            except Exception as e:
                logging.error(f"  {html_name}: 无法解析 scrape_time '{scrape_time_str}': {e}")
                total_failed += 1
                continue
            
            # 检查是否需要重命名
            if old_timestamp == new_timestamp:
                logging.debug(f"  {html_name}: 时间戳已相同，跳过")
                total_skipped += 1
                continue
            
            # 新文件名
            new_name = f"{city_id}_{new_timestamp}.html"
            new_path = html_file.parent / new_name
            
            # 检查新文件是否已存在
            if new_path.exists() and new_path != html_file:
                logging.warning(f"  {html_name}: 新文件名已存在 {new_name}，跳过")
                total_skipped += 1
                continue
            
            # 重命名
            html_file.rename(new_path)
            logging.info(f"  ✓ 重命名: {html_name} → {new_name}")
            total_renamed += 1
        
        except json.JSONDecodeError as e:
            logging.error(f"  {html_name}: 无法解析 JSON 文件 {json_file.name}: {e}")
            total_failed += 1
        except Exception as e:
            logging.error(f"  {html_name}: 出错: {e}")
            total_failed += 1

logging.info(f"\n{'='*60}")
logging.info(f"完成！")
logging.info(f"  重命名: {total_renamed}")
logging.info(f"  跳过: {total_skipped}")
logging.info(f"  失败: {total_failed}")
logging.info(f"{'='*60}")

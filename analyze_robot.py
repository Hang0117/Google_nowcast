import json
import os
import re
from datetime import datetime
from collections import Counter

folder_path = r"q:\Google_nowcast\Crawled\2026010600"

# 获取所有json文件
json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

# 提取时间戳并排序
file_data = []
for filename in json_files:
    # 从文件名中提取时间戳
    # 可能的格式: 
    # nowcast_xxx_20260106_xxxxxx.json (有下划线)
    # nowcast_xxx_20260106xxxxxx.json (无下划线)
    
    # 使用两种方式提取
    timestamp = None
    
    # 方式1: 8位日期_6位时间 (YYYYMMDD_HHMMSS)
    match1 = re.search(r'(\d{8})_(\d{6})', filename)
    if match1:
        try:
            timestamp_str = match1.group(1) + match1.group(2)
            timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        except:
            pass
    
    # 方式2: 14位连续时间戳 (YYYYMMDDHHMMSS)
    if not timestamp:
        match2 = re.search(r'(\d{14})', filename)
        if match2:
            try:
                timestamp_str = match2.group(1)
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
            except:
                pass
    
    if timestamp:
        file_data.append((filename, timestamp))

# 按时间排序
file_data.sort(key=lambda x: x[1])

print(f"总文件数: {len(file_data)}")
print("\n按时间排序的文件:")

# 检查每个文件中是否包含"robot"
robot_appearances = []

for idx, (filename, timestamp) in enumerate(file_data):
    filepath = os.path.join(folder_path, filename)
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            if 'robot' in content.lower():
                robot_appearances.append((idx, filename, timestamp))
                print(f"✓ {idx+1:3d}. {timestamp} - {filename}")
    except:
        pass

print("\n" + "="*80)
print("分析结果:")
print("="*80)

if robot_appearances:
    # 第一次出现
    first_idx, first_file, first_time = robot_appearances[0]
    print(f"\n1. 第一次出现robot的位置:")
    print(f"   文件索引: {first_idx + 1}/{len(file_data)}")
    print(f"   时间: {first_time}")
    print(f"   文件: {first_file}")
    
    # 统计大量出现的情况
    print(f"\n2. Robot出现统计:")
    print(f"   总出现次数: {len(robot_appearances)} 个文件")
    
    # 计算出现的时间跨度
    if len(robot_appearances) > 1:
        last_idx, last_file, last_time = robot_appearances[-1]
        time_span = last_time - first_time
        print(f"   时间跨度: {first_time} 到 {last_time}")
        print(f"   持续时间: {time_span}")
        
        # 找出大量出现的时间段（连续的robot出现）
        print(f"\n3. 大量出现的时间分析:")
        
        # 计算robot出现的密度
        robot_indices = [idx for idx, _, _ in robot_appearances]
        
        # 找出连续出现robot的区间
        consecutive_ranges = []
        current_start = robot_indices[0]
        current_end = robot_indices[0]
        
        for i in range(1, len(robot_indices)):
            if robot_indices[i] - robot_indices[i-1] <= 10:  # 相邻位置差不超过10
                current_end = robot_indices[i]
            else:
                consecutive_ranges.append((current_start, current_end))
                current_start = robot_indices[i]
                current_end = robot_indices[i]
        
        consecutive_ranges.append((current_start, current_end))
        
        # 输出最大的连续区间
        max_range = max(consecutive_ranges, key=lambda x: x[1] - x[0])
        max_start_idx, max_end_idx = max_range
        max_start_file, max_start_time = file_data[max_start_idx]
        max_end_file, max_end_time = file_data[max_end_idx]
        
        print(f"   最大连续robot出现区间: {max_start_time} 到 {max_end_time}")
        print(f"   包含 {max_end_idx - max_start_idx + 1} 个文件")
        print(f"   从第 {max_start_idx + 1} 个文件到第 {max_end_idx + 1} 个文件")
        
        # 显示robot开始大量出现的标志（超过50%）
        print(f"\n4. 大量出现robot的时间点:")
        threshold = len(file_data) * 0.05  # 每5%的文件数为一个时间窗口
        
        for i in range(0, len(file_data), max(1, int(threshold))):
            window_end = min(i + int(threshold), len(file_data))
            window_files = file_data[i:window_end]
            window_robot_count = sum(1 for idx, _, _ in robot_appearances if i <= idx < window_end)
            
            if window_robot_count > 0:
                start_time = window_files[0][1]
                end_time = window_files[-1][1]
                robot_ratio = window_robot_count / len(window_files) * 100
                print(f"   {start_time} - {end_time}: {window_robot_count}/{len(window_files)} ({robot_ratio:.1f}%)")
else:
    print("没有发现robot!")

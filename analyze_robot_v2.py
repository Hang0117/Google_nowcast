import json
import os
import re
from datetime import datetime

folder_path = r"q:\Google_nowcast\Crawled\2026010600"

# 获取所有json文件
json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

# 提取时间戳并排序
file_data = []
for filename in json_files:
    # 只处理包含真实日期 20260106 的文件
    if '20260106' not in filename:
        continue
    
    # 从文件名中提取时间戳 - 两种格式:
    # 格式1: nowcast_xxx_20260106_xxxxxx.json (有下划线)
    # 格式2: nowcast_xxx_20260106xxxxxx.json (无下划线)
    
    # 找出20260106后面的6位数字
    match = re.search(r'20260106[_]?(\d{6})', filename)
    if match:
        time_str = match.group(1)
        timestamp_str = f"20260106{time_str}"
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
            file_data.append((filename, timestamp))
        except:
            pass

# 按时间排序
file_data.sort(key=lambda x: x[1])

print(f"总文件数: {len(file_data)}")
print(f"\n按时间排序的文件(含robot的):")
print("="*80)

# 检查每个文件中是否包含"robot"
robot_appearances = []

for idx, (filename, timestamp) in enumerate(file_data):
    filepath = os.path.join(folder_path, filename)
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            if 'robot' in content.lower():
                robot_appearances.append((idx, filename, timestamp))
                print(f"✓ {idx+1:3d}. {timestamp.strftime('%Y-%m-%d %H:%M:%S')} - {filename}")
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
    print(f"   时间: {first_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   文件: {first_file}")
    
    # 统计大量出现的情况
    print(f"\n2. Robot出现统计:")
    print(f"   总出现次数: {len(robot_appearances)} 个文件")
    print(f"   出现比例: {len(robot_appearances)/len(file_data)*100:.1f}%")
    
    # 计算出现的时间跨度
    if len(robot_appearances) > 1:
        last_idx, last_file, last_time = robot_appearances[-1]
        time_span = last_time - first_time
        print(f"   时间跨度: {first_time.strftime('%Y-%m-%d %H:%M:%S')} 到 {last_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   持续时间: {time_span}")
        
        # 找出大量出现robot的时间段
        print(f"\n3. Robot出现密度分析:")
        
        # 按15分钟时间窗口统计
        from collections import defaultdict
        time_windows = defaultdict(int)
        total_in_windows = defaultdict(int)
        
        for filename, timestamp in file_data:
            # 以15分钟为窗口
            window_time = timestamp.replace(minute=(timestamp.minute // 15) * 15, second=0, microsecond=0)
            total_in_windows[window_time] += 1
        
        for idx, filename, timestamp in robot_appearances:
            window_time = timestamp.replace(minute=(timestamp.minute // 15) * 15, second=0, microsecond=0)
            time_windows[window_time] += 1
        
        # 显示robot出现比例大于50%的时间窗口
        print(f"\n   Robot出现比例 >= 50% 的时间段:")
        for window_time in sorted(time_windows.keys()):
            robot_count = time_windows[window_time]
            total_count = total_in_windows[window_time]
            ratio = robot_count / total_count * 100 if total_count > 0 else 0
            if ratio >= 50:
                print(f"   {window_time.strftime('%Y-%m-%d %H:%M')} - {robot_count}/{total_count} ({ratio:.1f}%)")
        
        # 找出连续robot出现的最长区间
        print(f"\n4. 大量出现robot的时间点:")
        robot_indices = [idx for idx, _, _ in robot_appearances]
        
        # 找连续出现robot的最长区间
        max_start_idx = robot_indices[0]
        max_end_idx = robot_indices[0]
        max_length = 1
        
        current_start_idx = robot_indices[0]
        current_end_idx = robot_indices[0]
        
        for i in range(1, len(robot_indices)):
            if robot_indices[i] - robot_indices[i-1] <= 20:  # 相邻位置差不超过20
                current_end_idx = robot_indices[i]
            else:
                if current_end_idx - current_start_idx + 1 > max_length:
                    max_length = current_end_idx - current_start_idx + 1
                    max_start_idx = current_start_idx
                    max_end_idx = current_end_idx
                current_start_idx = robot_indices[i]
                current_end_idx = robot_indices[i]
        
        if current_end_idx - current_start_idx + 1 > max_length:
            max_start_idx = current_start_idx
            max_end_idx = current_end_idx
        
        max_start_file, max_start_time = file_data[max_start_idx]
        max_end_file, max_end_time = file_data[max_end_idx]
        
        print(f"   最大连续robot出现区间: {max_start_time.strftime('%Y-%m-%d %H:%M:%S')} 到 {max_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   包含 {max_end_idx - max_start_idx + 1} 个连续文件")
        print(f"   其中 {sum(1 for idx in robot_indices if max_start_idx <= idx <= max_end_idx)} 个包含robot")
        
else:
    print("没有发现robot!")

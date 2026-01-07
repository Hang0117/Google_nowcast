import json
import os
import re
from datetime import datetime, timedelta

folder_path = r"q:\Google_nowcast\Crawled\2026010607"

# 获取所有json文件
json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

# 提取时间戳并排序
file_data = []
for filename in json_files:
    # 只处理包含真实日期 20260106 的文件
    if '20260106' not in filename:
        continue
    
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

# 检查每个文件中是否包含"robot"
robot_files = []

for filename, timestamp in file_data:
    filepath = os.path.join(folder_path, filename)
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            if 'robot' in content.lower():
                robot_files.append((filename, timestamp))
    except:
        pass

print(f"总文件数: {len(file_data)}")
print(f"包含robot的文件数: {len(robot_files)}")
print("\n" + "="*80)
print("分析间隔2分钟内的robot出现情况")
print("="*80)

if len(robot_files) > 1:
    # 计算相邻robot文件之间的时间间隔
    intervals = []
    for i in range(1, len(robot_files)):
        prev_time = robot_files[i-1][1]
        curr_time = robot_files[i][1]
        interval = (curr_time - prev_time).total_seconds() / 60  # 转换为分钟
        intervals.append({
            'prev_file': robot_files[i-1][0],
            'prev_time': prev_time,
            'curr_file': robot_files[i][0],
            'curr_time': curr_time,
            'interval_minutes': interval
        })
    
    # 统计间隔 <= 2分钟的情况
    within_2min = [x for x in intervals if x['interval_minutes'] <= 2]
    
    print(f"\n1. 间隔统计:")
    print(f"   总共相邻robot对数: {len(intervals)}")
    print(f"   间隔 <= 2分钟的对数: {len(within_2min)}")
    print(f"   占比: {len(within_2min)/len(intervals)*100:.1f}%")
    
    # 显示间隔 <= 2分钟的具体情况
    print(f"\n2. 间隔 <= 2分钟的robot文件对:")
    print(f"{'序号':<5} {'前一文件时间':<20} {'当前文件时间':<20} {'间隔(分钟)':<15}")
    print("-" * 65)
    
    for idx, item in enumerate(within_2min, 1):
        prev_time_str = item['prev_time'].strftime('%Y-%m-%d %H:%M:%S')
        curr_time_str = item['curr_time'].strftime('%Y-%m-%d %H:%M:%S')
        interval_str = f"{item['interval_minutes']:.2f}分钟"
        print(f"{idx:<5} {prev_time_str:<20} {curr_time_str:<20} {interval_str:<15}")
    
    # 统计间隔分布
    print(f"\n3. 间隔分布统计:")
    interval_ranges = [
        (0, 1, "0-1分钟"),
        (1, 2, "1-2分钟"),
        (2, 5, "2-5分钟"),
        (5, 10, "5-10分钟"),
        (10, float('inf'), "10分钟以上")
    ]
    
    for min_val, max_val, label in interval_ranges:
        count = sum(1 for x in intervals if min_val <= x['interval_minutes'] < max_val)
        percentage = count / len(intervals) * 100 if len(intervals) > 0 else 0
        print(f"   {label:<15}: {count:>3} 对 ({percentage:>5.1f}%)")
    
    # 最小、最大、平均间隔
    min_interval = min(intervals, key=lambda x: x['interval_minutes'])
    max_interval = max(intervals, key=lambda x: x['interval_minutes'])
    avg_interval = sum(x['interval_minutes'] for x in intervals) / len(intervals)
    
    print(f"\n4. 间隔统计指标:")
    print(f"   最小间隔: {min_interval['interval_minutes']:.2f}分钟")
    print(f"            ({min_interval['prev_time'].strftime('%H:%M:%S')} -> {min_interval['curr_time'].strftime('%H:%M:%S')})")
    print(f"   最大间隔: {max_interval['interval_minutes']:.2f}分钟")
    print(f"            ({max_interval['prev_time'].strftime('%H:%M:%S')} -> {max_interval['curr_time'].strftime('%H:%M:%S')})")
    print(f"   平均间隔: {avg_interval:.2f}分钟")

else:
    print("robot文件数少于2个，无法计算间隔！")

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
robot_status = {}

for filename, timestamp in file_data:
    filepath = os.path.join(folder_path, filename)
    has_robot = False
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            if 'robot' in content.lower():
                has_robot = True
    except:
        pass
    robot_status[(filename, timestamp)] = has_robot

print(f"总文件数: {len(file_data)}")
print("\n" + "="*80)
print("按2分钟时间段统计robot出现次数")
print("="*80)

# 获取时间范围
if file_data:
    start_time = file_data[0][1]
    end_time = file_data[-1][1]
    
    print(f"\n时间范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} 到 {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总时间跨度: {(end_time - start_time).total_seconds() / 60:.1f} 分钟\n")
    
    # 按2分钟为单位分组
    current_time = start_time.replace(second=0, microsecond=0)
    # 向下对齐到2分钟边界
    current_time = current_time.replace(minute=(current_time.minute // 2) * 2)
    
    time_segments = []
    
    while current_time <= end_time:
        segment_start = current_time
        segment_end = current_time + timedelta(minutes=2)
        
        # 统计这个时间段内的文件和robot数
        files_in_segment = []
        robot_count = 0
        total_count = 0
        
        for filename, timestamp in file_data:
            if segment_start <= timestamp < segment_end:
                files_in_segment.append((filename, timestamp))
                total_count += 1
                if robot_status[(filename, timestamp)]:
                    robot_count += 1
        
        if total_count > 0 or len(time_segments) == 0:  # 记录有文件的段，或第一段
            time_segments.append({
                'start': segment_start,
                'end': segment_end,
                'robot_count': robot_count,
                'total_count': total_count,
                'files': files_in_segment
            })
        
        current_time += timedelta(minutes=2)
    
    # 显示统计结果
    print(f"{'时间段':<40} {'总文件':<8} {'Robot文件':<12} {'占比':<10}")
    print("-" * 75)
    
    for segment in time_segments:
        if segment['total_count'] > 0:
            start_str = segment['start'].strftime('%Y-%m-%d %H:%M:%S')
            end_str = segment['end'].strftime('%H:%M:%S')
            robot_count = segment['robot_count']
            total_count = segment['total_count']
            ratio = robot_count / total_count * 100 if total_count > 0 else 0
            
            print(f"{start_str} - {end_str:<10} {total_count:<8} {robot_count:<12} {ratio:>6.1f}%")
    
    # 统计汇总
    print("\n" + "="*80)
    print("汇总统计:")
    print("="*80)
    
    total_robot = sum(s['robot_count'] for s in time_segments)
    total_files = sum(s['total_count'] for s in time_segments)
    robot_segments = sum(1 for s in time_segments if s['robot_count'] > 0)
    
    print(f"总共 {len(time_segments)} 个2分钟时间段")
    print(f"包含robot的时间段: {robot_segments} 个")
    print(f"总robot数: {total_robot} 个")
    print(f"总文件数: {total_files} 个")
    print(f"整体robot占比: {total_robot/total_files*100:.1f}%")
    
    # 显示robot最多的时间段
    print("\n最多robot出现的时间段 (Top 10):")
    sorted_segments = sorted(time_segments, key=lambda x: x['robot_count'], reverse=True)[:10]
    for idx, segment in enumerate(sorted_segments, 1):
        start_str = segment['start'].strftime('%Y-%m-%d %H:%M:%S')
        end_str = segment['end'].strftime('%H:%M:%S')
        ratio = segment['robot_count'] / segment['total_count'] * 100 if segment['total_count'] > 0 else 0
        print(f"{idx:2d}. {start_str} - {end_str:<10}: {segment['robot_count']:>3}/{segment['total_count']:<3} ({ratio:>6.1f}%)")

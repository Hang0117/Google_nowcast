import json
from pathlib import Path
from collections import Counter

# folder = Path('2026010502')
base_dir = Path(__file__).parent
output_dir = base_dir / "Crawled"
date_folders = sorted([d for d in output_dir.glob('[0-9]*') if d.is_dir()])
folder = date_folders[-1]
type_counts = Counter()
robot_files = []
null_files = []
other_files = []

for json_file in sorted(folder.glob('*.json')):
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            type_val = data.get('type')
            type_counts[str(type_val)] += 1
            
            if type_val == 'robot':
                robot_files.append(json_file.name)
            elif type_val is None:
                null_files.append(json_file.name)
            else:
                other_files.append((json_file.name, type_val))
    except:
        pass

print('=== Type 分布统计 ===')
for type_val, count in sorted(type_counts.items(), key=lambda x: -x[1]):
    print(f'{type_val}: {count}')

print(f'\n=== type: robot 的文件 ({len(robot_files)}) ===')
for fname in robot_files[:10]:
    print(fname)
if len(robot_files) > 10:
    print(f'... 还有 {len(robot_files)-10} 个')

print(f'\n=== type: null 的文件 ({len(null_files)}) ===')
for fname in null_files[:10]:
    print(fname)
if len(null_files) > 10:
    print(f'... 还有 {len(null_files)-10} 个')

print(f'\n=== 其他类型 ({len(other_files)}) ===')
for fname, t in other_files[:10]:
    print(f'{fname}: {t}')
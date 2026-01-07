import json
from pathlib import Path

# folder = Path('2026010502')
base_dir = Path(__file__).parent
output_dir = base_dir / "Crawled"
date_folders = sorted([d for d in output_dir.glob('[0-9]*') if d.is_dir()])
folder = date_folders[-1]
no_data_files = []
total_files = 0

for json_file in sorted(folder.glob('*.json')):
    total_files += 1
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if data.get('message') == 'no nowcast data now.':
                no_data_files.append(json_file.name)
    except:
        pass

print(f'总文件数: {total_files}')
print(f'无数据文件数: {len(no_data_files)}')
print(f'占比: {len(no_data_files)/total_files*100:.1f}%')
print(f'\n前10个无数据文件:')
for fname in no_data_files[:10]:
    print(f'  {fname}')
if len(no_data_files) > 10:
    print(f'  ... 还有 {len(no_data_files)-10} 个')
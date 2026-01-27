import re

file_path = r'q:\Code\Google_nowcast\GoogleNowcastHTML\2026012602\02462_2026012602.html'

with open(file_path, 'r', encoding='utf-8') as f:
    html = f.read()

# 找到所有container
container_pattern = r'jsname="s2gQvd"[^>]*class="EDblX HG5ZQb"'
containers = list(re.finditer(container_pattern, html))

print(f"找到 {len(containers)} 個containers\n")
print("=" * 100)

# 對每個container檢查其後面5000字符內的hourly items
for i, container in enumerate(containers, 1):
    pos = container.start()
    
    # 向后搜索5000字符
    search_end = min(len(html), pos + 5000)
    search_text = html[pos:search_end]
    
    # 計算hourly items
    hourly_items = re.findall(r'role="listitem"[^>]*aria-label="[^"]*\d{1,2}\s*[AP]M', search_text)
    time_markers = re.findall(r'\d{1,2}\s*[AP]M', ' '.join(hourly_items))
    
    print(f"容器 {i}:")
    print(f"  位置: {pos}")
    print(f"  後續5000字符內的listitem數: {len(hourly_items)}")
    print(f"  時間標記數: {len(time_markers)}")
    
    if len(hourly_items) >= 20:
        print(f"  ✅ 這是真正的24小時hourly容器!")
        print(f"  前5個小時: {time_markers[:5]}")
        print(f"  後5個小時: {time_markers[-5:]}")
    else:
        print(f"  ❌ 這不是hourly容器 (可能是UI元素)")
    
    print("-" * 100)

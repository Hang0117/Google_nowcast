#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Google天气未来24小时预报数据爬取 - 单个城市版本 (移动端)
爬取内容：
- jsname="s2gQvd" class="EDblX HG5ZQb" 下的 aria-label 信息
"""

import sys
import io
import os

# 设置UTF-8编码
os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from datetime import datetime
import time
import pandas as pd
import re
import json
from pathlib import Path

print("\n" + "="*60)
print("爬取Google未来24小时天气预报数据 - 单个城市 (移动端)")
print("="*60)

# 检查Selenium安装
selenium_installed = False
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    selenium_installed = True
    print("\n✓ Selenium已安装")
except ImportError as import_err:
    print(f"\n❌ Selenium导入失败: {import_err}")
    print("请运行: pip install selenium webdriver-manager")
    exit(1)


def scrape_24h_forecast(city_name, headless=True, save_json=False, save_csv=True, output_dir=None):
    """
    爬取Google搜索中的未来24小时天气预报数据
    """
    if not selenium_installed:
        print("❌ Selenium未安装，无法继续")
        return None
    
    # 设置输出目录
    if output_dir is None:
        output_dir = Path(__file__).parent
    else:
        output_dir = Path(output_dir)
    
    try:
        print(f"\n正在爬取 {city_name} 的未来24小时预报...")
        print(f"  浏览器模式: {'无头' if headless else '显示'}")
        print(f"  设备类型: 移动端")

        def save_debug_html(tag: str):
            """Save current page HTML for troubleshooting."""
            try:
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                fname = output_dir / f"debug_mobile_{tag}_{ts}.html"
                fname.write_text(driver.page_source, encoding="utf-8")
                print(f"    ⚠ 已保存调试页面: {fname}")
            except Exception as e:
                print(f"    ⚠ 保存调试页面失败: {e}")
        
        # 配置Chrome选项
        print("  正在配置Chrome浏览器...")
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--lang=en-US')
        
        # 移动设备模拟
        mobile_emulation = {"deviceName": "Nexus 5"}
        options.add_experimental_option("mobileEmulation", mobile_emulation)
        
        # 移动设备User-Agent
        options.add_argument("user-agent=Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36")
        
        # 初始化WebDriver
        print("  正在初始化WebDriver...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        print("  ✓ WebDriver已初始化")
        
        try:
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
            })
        except Exception:
            pass
        
        # 访问Google搜索
        print("  [1/3] 打开Google搜索...")
        driver.get('https://www.google.com/ncr?hl=en&gl=us')
        
        # 处理同意弹窗
        def click_consent():
            try:
                btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, 'L2AGLb'))
                )
                btn.click()
                time.sleep(0.5)
                return True
            except Exception:
                pass
            try:
                candidates = driver.find_elements(By.XPATH, "//button//*[text()='Accept all']/..|//button//*[text()='I agree']/..")
                if candidates:
                    candidates[0].click()
                    time.sleep(0.5)
                    return True
            except Exception:
                pass
            return False
        
        click_consent()

        def click_tab(tab_id: str):
            """点击顶部的温度/降水/风速tab，确保页面切换可见。"""
            try:
                tab = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, tab_id))
                )
                tab.click()
                time.sleep(0.6)
                return True
            except Exception:
                return False
        
        # 搜索天气
        print("  [2/3] 搜索天气...")
        try:
            search_box = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.NAME, 'q'))
            )
            search_box.clear()
            search_box.send_keys(f'weather in {city_name}')
            search_box.send_keys(Keys.RETURN)
        except Exception:
            q = re.sub(r"\s+", "+", f"weather in {city_name}")
            driver.get(f'https://www.google.com/search?q={q}&hl=en&gl=us')
        
        time.sleep(3)
        wait = WebDriverWait(driver, 20)
        
        # 等待天气widget加载
        try:
            widget = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#wob_wc")))
            print("  ✓ 天气widget已加载")
        except Exception:
            print("  ⚠ 天气widget加载超时")
        
        print("  [3/3] 爬取未来24小时预报数据...")

        # 等待渲染稳定
        time.sleep(2)

        # 轻微滚动以触发展示
        try:
            driver.execute_script("window.scrollBy(0, 300);")
            time.sleep(0.5)
            driver.execute_script("window.scrollBy(0, -200);")
        except Exception:
            pass
        
        # 初始化预报数据字典
        forecast_data = {
            'city': city_name,
            'timestamp': datetime.now().isoformat(),
            'time': [],
            'aria_labels': []
        }
        
        # === 爬取时间 ===
        print("\n  [时间] 正在爬取...")
        try:
            time_result = driver.execute_script(
                """
                const container = document.getElementById('wob_sd');
                if (!container) return [];
                const spans = container.querySelectorAll('span');
                const times = [];
                const seen = new Set();
                for (const s of spans) {
                  const styleAttr = (s.getAttribute('style') || '').toLowerCase();
                  if (styleAttr.includes('display:none') || styleAttr.includes('display: none')) continue;
                  const cs = window.getComputedStyle(s);
                  if (cs.display === 'none' || cs.visibility === 'hidden' || cs.opacity === '0') continue;
                  const rect = s.getBoundingClientRect();
                  if (rect.width === 0 || rect.height === 0) continue;
                  const txt = (s.textContent || '').trim().toUpperCase();
                  if (txt && /^\d{1,2}\s*[AP]M$/.test(txt) && !seen.has(txt)) {
                    const normalized = txt.replace(/\s+/g, ' ');
                    if (!seen.has(normalized)) {
                      times.push(normalized);
                      seen.add(normalized);
                    }
                  }
                }
                return times;
                """
            ) or []
            
            forecast_data['time'] = time_result
            print(f"    ✓ 获取 {len(time_result)} 个时间点")
            if time_result:
                print(f"      样本: {time_result[:3]}")
        except Exception as e:
            print(f"    ✗ 无法获取时间: {str(e)[:80]}")
        
        # === 爬取 jsname="s2gQvd" class="EDblX HG5ZQb" 下的 aria-label ===
        print("\n  [aria-label] 正在爬取...")
        try:
            aria_result = None
            for attempt in range(1, 4):
                aria_result = driver.execute_script(
                    """
                    const container = document.querySelector('[jsname="s2gQvd"].EDblX.HG5ZQb');
                    if (!container) return { count: 0, labels: [] };
                    const items = container.querySelectorAll('[role="listitem"][aria-label]');
                    const labels = [];
                    for (const item of items) {
                        const ariaLabel = item.getAttribute('aria-label');
                        if (ariaLabel) {
                            labels.push(ariaLabel);
                        }
                    }
                    return { count: items.length, labels: labels };
                    """
                ) or {}

                if isinstance(aria_result, dict) and aria_result.get('labels'):
                    break

                # 若未抓到，尝试滚动并等待
                driver.execute_script("window.scrollBy(0, 400);")
                time.sleep(1)
                driver.execute_script("window.scrollBy(0, -300);")
                time.sleep(0.5)

            if isinstance(aria_result, dict):
                forecast_data['aria_labels'] = aria_result.get('labels', [])
                print(f"    ✓ 获取 {aria_result.get('count', 0)} 个节点，aria-label 数量 {len(forecast_data['aria_labels'])}")
                if forecast_data['aria_labels']:
                    print(f"      样本: {forecast_data['aria_labels'][:3]}")
            else:
                # 兼容老返回格式
                forecast_data['aria_labels'] = aria_result if isinstance(aria_result, list) else []
                print(f"    ✓ 获取 {len(forecast_data['aria_labels'])} 个 aria-label")
                if forecast_data['aria_labels']:
                    print(f"      样本: {forecast_data['aria_labels'][:3]}")
        except Exception as e:
            print(f"    ✗ 无法获取 aria-label: {str(e)[:80]}")

        # 若未抓到核心数据，保存调试页面
        if not forecast_data['time'] or not forecast_data['aria_labels']:
            print("    ⚠ 未获取到完整数据，保存调试页面以便排查")
            save_debug_html("nodata")
        
        driver.quit()
        
        # 保存结果
        if save_json or save_csv:
            output_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            city_short = city_name.split(',')[0].replace(' ', '_')

            if save_json:
                json_file = output_dir / f"{city_short}_24h_forecast_mobile_{ts}.json"
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(forecast_data, f, ensure_ascii=False, indent=2)
                print(f"\n✓ 已保存JSON到 {json_file}")

            if save_csv:
                df = create_forecast_dataframe(forecast_data)
                if df is not None:
                    csv_file = output_dir / f"{city_short}_24h_forecast_mobile_{ts}.csv"
                    df.to_csv(csv_file, index=False, encoding='utf-8')
                    print(f"✓ 已保存CSV到 {csv_file}")
        
        # 显示摘要
        print(f"\n{'='*60}")
        print(f"【{city_name}】未来24小时预报数据摘要")
        print(f"{'='*60}")
        print(f"时间戳: {forecast_data['timestamp']}")
        print(f"\n数据点数量:")
        print(f"  - 时间: {len(forecast_data['time'])} 个")
        print(f"  - aria-label: {len(forecast_data['aria_labels'])} 个")
        
        return forecast_data
        
    except Exception as e:
        print(f"\n❌ 爬取失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def create_forecast_dataframe(forecast_data):
    """
    将预报数据转换为DataFrame
    """
    if not forecast_data:
        return None
    
    times = forecast_data.get('time', [])
    aria_labels = forecast_data.get('aria_labels', [])
    
    if not aria_labels:
        return None
    
    # 对齐长度
    max_len = max(len(times), len(aria_labels))
    times = times + [''] * (max_len - len(times))
    aria_labels = aria_labels + [''] * (max_len - len(aria_labels))
    
    df = pd.DataFrame({
        'time': times[:max_len],
        'aria_label': aria_labels[:max_len]
    })
    
    return df


if __name__ == "__main__":
    if not selenium_installed:
        print("\n❌ Selenium未安装，无法运行爬虫")
        print("请先运行: pip install selenium webdriver-manager")
        exit(1)
    
    # 默认城市
    city_name = "美国, 纽约, 伊什威"
    
    # 支持命令行参数指定城市
    if len(sys.argv) > 1:
        city_name = sys.argv[1]
    
    print(f"\n{'='*60}")
    print(f"开始爬取城市: {city_name}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # 爬取单个城市（headless=False 可以看到浏览器操作）
    result = scrape_24h_forecast(
        city_name, 
        headless=False, 
        save_json=True, 
        save_csv=True
    )
    
    if result:
        print(f"\n✓ 爬取成功!")
    else:
        print(f"\n✗ 爬取失败")
    
    print(f"\n{'='*60}\n")

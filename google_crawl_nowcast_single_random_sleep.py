
#!/usr/bin/env python3
"""Concurrent Google nowcast scraper with multi-threading.

Uses ThreadPoolExecutor to scrape multiple cities in parallel.
"""
import os
import sys
import subprocess
import platform
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def _install_dependencies():
    """Auto-install system and Python dependencies on Linux."""
    system = platform.system()
    
    if system != "Linux":
        # Skip on non-Linux systems (Windows, macOS)
        try:
            import selenium
            import pandas
            from apscheduler.schedulers.background import BackgroundScheduler
        except ImportError:
            print("⚠️  Warning: Some Python packages not installed. Please install manually:")
            print("   pip install selenium webdriver-manager pandas apscheduler pytz")
        return
    
    # Linux: Install Chrome and Python dependencies
    print("🔧 Checking and installing dependencies on Linux...")
    
    # Check if Chrome is installed
    chrome_check = subprocess.run(
        ["which", "google-chrome"], 
        capture_output=True
    )
    
    if chrome_check.returncode != 0:
        print("📦 Installing Google Chrome...")
        try:
            subprocess.run(["sudo", "apt-get", "update"], check=True, capture_output=True)
            subprocess.run(
                ["sudo", "bash", "-c", 
                 "wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && "
                 "echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' > /etc/apt/sources.list.d/google-chrome.list"],
                check=True, capture_output=True
            )
            subprocess.run(["sudo", "apt-get", "update"], check=True, capture_output=True)
            subprocess.run(
                ["sudo", "apt-get", "install", "-y", "google-chrome-stable"],
                check=True, capture_output=True
            )
            print("✅ Google Chrome installed")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Could not install Chrome: {e}")
            print("   Please install manually: sudo apt-get install -y google-chrome-stable")
    else:
        print("✅ Google Chrome already installed")
    
    # Install Python packages
    print("📦 Installing Python packages...")
    packages = ["selenium", "webdriver-manager", "pandas", "apscheduler", "pytz"]
    try:
        subprocess.run(
            ["pip", "install", "-q"] + packages,
            check=True
        )
        print("✅ Python packages installed")
    except subprocess.CalledProcessError as e:
        print(f"⚠️  Could not install Python packages: {e}")
        raise


# Auto-install dependencies when imported
_install_dependencies()

# Now import the required packages
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd


def _chrome_driver(headless: bool = True):
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"]) 
    options.add_experimental_option("useAutomationExtension", False)
    mobile_emulation = {"deviceName": "Nexus 5"}
    options.add_experimental_option("mobileEmulation", mobile_emulation)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def _accept_consent(driver):
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.ID, "L2AGLb")))
        btn.click()
        time.sleep(0.3)
        return
    except Exception:
        pass
    try:
        from selenium.webdriver.common.by import By
        candidates = driver.find_elements(By.XPATH, "//button//*[text()='Accept all']/..|//button//*[text()='I agree']/..")
        if candidates:
            candidates[0].click()
            time.sleep(0.3)
    except Exception:
        pass


def scrape_nowcast_svg(
    city: str = "Fairfax, California, United States",
    city_id: str = "",
    headless: bool = True,
    save_json: bool = True,
    output_dir: str | Path | None = None,
    first_scrape_date: str | None = None,
):
    """Scrape rect heights from the SVG whose viewBox includes 1440 and 48."""
    start_time = time.time()
    
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as e:
        print(f"ERR [{city}]: selenium not available:", e)
        return None

    base_dir = Path(output_dir) if output_dir else Path(__file__).parent

    out = {
        "city": city,
        "city_id": city_id,
        "scrape_time": datetime.now(timezone.utc).isoformat(),
        "type": None,
        "viewBox": None,
        "points": []
    }

    driver = _chrome_driver(headless=headless)
    try:
        # start crawling timer
        crawl_start = time.time()
        driver.get("https://www.google.com/ncr?hl=en&gl=us")
        _accept_consent(driver)

        from urllib.parse import quote_plus
        q = quote_plus(f"weather {city}")
        driver.get(f"https://www.google.com/search?q={q}&hl=en&gl=us")

        time.sleep(3)
        
        # Save HTML page
        try:
            html_content = driver.page_source
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            html_dir = base_dir / "GoogleNowcastHTML" / folder_date
            html_dir.mkdir(parents=True, exist_ok=True)
            html_filename = f"{city_id}_{folder_date}.html"
            html_path = html_dir / html_filename
            html_path.write_text(html_content, encoding="utf-8")
            print(f"[{city}] Saved HTML: {html_filename}")
        except Exception as e:
            print(f"[{city}] Warning: Could not save HTML: {e}")
        
        crawl_time = time.time() - crawl_start
        print(f"[{city_id}] 爬取耗时（含保存HTML）: {crawl_time:.2f}秒")
        
        # start parsing timer
        parse_start = time.time()
        
        # Check for reCAPTCHA robot verification
        check_robot_js = """
        const pageText = document.body.innerText;
        const hasRobotCheck = pageText.includes("I'm not a robot") || pageText.includes("unusual traffic");
        return hasRobotCheck;
        """
        is_robot_check = driver.execute_script(check_robot_js)
        if is_robot_check:
            print("⚠ reCAPTCHA verification detected: 'I'm not a robot'")
            out["type"] = "robot"
            if save_json:
                folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                print("Saved:", fname)
            return out

        js = """
        const all = document.querySelectorAll('svg');
        const withRects = [];
        for (const svg of all) {
            const rects = svg.querySelectorAll('rect');
            if (rects.length) {
                withRects.push({svg, viewBox: svg.getAttribute('viewBox'), rectCount: rects.length});
            }
        }
        let target = null;
        for (const info of withRects) {
            const vb = (info.viewBox || "");
            if (vb.includes('1440') && vb.includes('48')) { target = info; break; }
        }
        if (!target) {
            return {found:false, sample: withRects.slice(0, 10)};
        }
        const rects = target.svg.querySelectorAll('rect');
        const rows = [];
        for (let i=0;i<rects.length;i++){
            const r = rects[i];
            rows.push({
                idx:i,
                height: r.getAttribute('height')||'',
                fill: r.getAttribute('fill')||'',
                x: r.getAttribute('x')||'',
                y: r.getAttribute('y')||'',
                width: r.getAttribute('width')||''
            });
        }
        return {found:true, viewBox: target.viewBox, rects: rows};
        """

        result = driver.execute_script(js)
        if not result or not result.get("found"):
            print(f"[{city}] No target SVG found. Trying fallback div...")
            fallback_js = """
            const div = document.querySelector('div[jsname="Kt2ahd"].XhUg9e');
            if (!div) return {found: false, reason: 'no_kt2ahd_div'};
            const div1 = div.querySelector('.SnOHQb.tNxQIb');
            const div2 = div.querySelector('.jz8NAf.ApHyTb');
            if (!div1 && !div2) return {found: false, reason: 'no_target_divs'};
            const data = {
                div1_text: div1 ? div1.textContent.trim() : null,
                div2_text: div2 ? div2.textContent.trim() : null
            };
            return {found: true, source: 'fallback_div', data: data};
            """
            
            fallback_result = driver.execute_script(fallback_js)
            if fallback_result and fallback_result.get("found"):
                print(f"[{city}] Fallback OK: found divs")
                out["fallback_data"] = fallback_result.get("data")
                out["source"] = "fallback_div"
                out["type"] = "nowcast"
                result = {"viewBox": None, "rects": []}
            else:
                print(f"[{city}] Fallback div not found. Trying hourly forecast...")
                hourly_js = """
                const container = document.querySelector('[jsname="s2gQvd"].EDblX.HG5ZQb');
                if (!container) return { found: false, reason: 'no_hourly_container' };
                const items = container.querySelectorAll('[role="listitem"][aria-label]');
                if (!items || items.length === 0) {
                    return { found: false, reason: 'no_hourly_items' };
                }
                const labels = [];
                for (let i = 0; i < Math.min(6, items.length); i++) {
                    const ariaLabel = items[i].getAttribute('aria-label');
                    if (ariaLabel) labels.push(ariaLabel);
                }
                return { found: labels.length > 0, count: labels.length, labels: labels };
                """
                
                hourly_result = driver.execute_script(hourly_js)
                if hourly_result and hourly_result.get("found"):
                    print(f"[{city}] Hourly forecast OK: {hourly_result.get('count', 0)} items")
                    out["hourly_data"] = hourly_result.get("labels", [])
                    out["source"] = "hourly_aria_label"
                    out["type"] = "hourly"
                    result = {"viewBox": None, "rects": []}
                else:
                    html = driver.page_source
                    dbg = base_dir / f"debug_nowcast_{city.split(',')[0].replace(' ', '_')}.html"
                    dbg.write_text(html, encoding="utf-8")
                    reason = hourly_result.get('reason') if hourly_result else 'unknown'
                    print(f"[{city}] No data found (reason: {reason}). Wrote {dbg.name}")
                    # Delete debug file after saving
                    try:
                        import time as time_module
                        time_module.sleep(0.5)  # Brief delay to ensure file is written
                        dbg.unlink()  # Delete the file
                        print(f"[{city}] Debug file deleted: {dbg.name}")
                    except Exception as del_err:
                        print(f"[{city}] Could not delete debug file: {del_err}")
                    out["message"] = "no nowcast data now."
                    if save_json:
                        folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                        file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                        outdir = base_dir / "Crawled" / folder_date
                        outdir.mkdir(parents=True, exist_ok=True)
                        fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                        fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                    return out

        out["viewBox"] = result.get("viewBox")
        if result.get("source"):
            out["source"] = result.get("source")
        if not out["type"]:
            out["type"] = "nowcast"
        rows = result.get("rects") or []
        start = datetime.fromisoformat(out["scrape_time"])
        # If minute is odd, subtract 1 minute to make it even
        if start.minute % 2 == 1:
            start = start - timedelta(minutes=1)
        for row in rows:
            t = start + timedelta(minutes=int(row.get("idx", 0)) * 2)
            out["points"].append({
                "minute_index": int(row.get("idx", 0)),
                "time": t.strftime("%Y-%m-%d %H:%M"),
                "height": row.get("height"),
                "fill": row.get("fill"),
                "x": row.get("x"),
                "y": row.get("y"),
                "width": row.get("width")
            })

        if save_json and out["points"]:
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[{city}] Saved: {fname.name}")
        elif save_json and out.get("fallback_data"):
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[{city}] Saved: {fname.name}")
        elif save_json and out.get("hourly_data"):
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[{city}] Saved: {fname.name}")

        parse_time = time.time() - parse_start
        total_time = time.time() - start_time
        print(f"[{city_id}] ⏱️ 解析耗时: {parse_time:.2f}秒")
        print(f"[{city_id}] ⏱️ 总耗时: {total_time:.2f}秒")
        
        return out

    except Exception as e:
        total_time = time.time() - start_time
        print(f"ERR [{city}] (总耗时 {total_time:.2f}秒):", e)
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# Thread-safe counter for progress tracking
class ProgressTracker:
    def __init__(self, total):
        self.total = total
        self.completed = 0
        self.lock = threading.Lock()
    
    def increment(self):
        with self.lock:
            self.completed += 1
            return self.completed


def scrape_city_wrapper(city, city_id, headless, output_root, tracker, first_scrape_date):
    """Wrapper function for concurrent scraping."""
    result = scrape_nowcast_svg(city, city_id=city_id, headless=headless, save_json=True, output_dir=output_root, first_scrape_date=first_scrape_date)
    completed = tracker.increment()
    
    if result and result.get("points"):
        print(f"[{completed}/{tracker.total}] ✓ {city_id}: {len(result['points'])} points")
    elif result and result.get("hourly_data"):
        print(f"[{completed}/{tracker.total}] ✓ {city_id}: {len(result['hourly_data'])} hourly items")
    elif result and result.get("fallback_data"):
        print(f"[{completed}/{tracker.total}] ✓ {city_id}: fallback data")
    else:
        print(f"[{completed}/{tracker.total}] ✗ {city_id}: No data")
    
    return city, result


def scrape_single_city(city, city_id, base_dir):
    """单个城市的爬取任务（用于定时调度）"""
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 开始爬取 {city_id}")
    result = scrape_nowcast_svg(
        city=city,
        city_id=city_id,
        headless=False,  # 显示浏览器窗口
        save_json=True,
        output_dir=base_dir
    )
    if result:
        if result.get("points"):
            print(f"✓ {city_id} 完成: {len(result['points'])} 个数据点")
        elif result.get("type") == "robot":
            print(f"⚠️ {city_id} reCAPTCHA")
        else:
            print(f"✓ {city_id} 完成")
    else:
        print(f"✗ {city_id} 失败")
    return result


def generate_random_schedule(total_stations, duration_hours=12, avg_scrape_time=15):
    """生成随机化的调度时间表（考虑实际爬取耗时）
    
    Args:
        total_stations: 总站点数
        duration_hours: 持续时间（小时）
        avg_scrape_time: 平均每个站点的爬取时间（秒），默认15秒
        
    Returns:
        list: 每个站点的调度秒数列表（相对于开始时间）
    """
    import random
    
    total_seconds = duration_hours * 3600
    # 总执行时间 = 所有站点的爬取时间总和
    total_scrape_time = total_stations * avg_scrape_time
    # 可用于间隔的时间 = 总时间 - 总执行时间
    available_interval_time = total_seconds - total_scrape_time
    
    if available_interval_time < 0:
        print(f"⚠️ 警告: {duration_hours}小时不足以完成{total_stations}个站点（需要{total_scrape_time/3600:.1f}小时）")
        print(f"   建议增加持续时间或减少站点数")
        # 紧密调度，间隔最小化
        avg_interval = 1
    else:
        # 平均间隔 = 可用间隔时间 / 站点数
        avg_interval = available_interval_time / total_stations
    
    print(f"📊 调度参数:")
    print(f"   总时间: {total_seconds}秒 ({duration_hours}小时)")
    print(f"   预计爬取时间: {total_scrape_time}秒 ({total_scrape_time/3600:.2f}小时)")
    print(f"   可用间隔时间: {available_interval_time}秒 ({available_interval_time/3600:.2f}小时)")
    print(f"   平均间隔: {avg_interval:.1f}秒\n")
    
    # 为每个站点分配一个时间段，然后在时间段内随机化
    schedule = []
    for i in range(total_stations):
        # 计算该站点的启动时间段范围（不包括执行时间）
        segment_start = int(i * avg_interval)
        segment_end = int((i + 1) * avg_interval)
        
        # 在时间段内随机选择一个启动时间点
        random_time = random.randint(segment_start, min(segment_end, int(available_interval_time)))
        schedule.append(random_time)
    
    # 打乱顺序使其更随机
    random.shuffle(schedule)
    
    return schedule


def scrape_all_cities_concurrent(base_dir, csv_file='nowcast_crawl_list_v3.csv', max_workers=5):
    """并发爬取所有城市的气象数据
    
    Args:
        base_dir: 输出目录的基础路径
        csv_file: CSV 文件路径，默认为 'nowcast_crawl_list_v3.csv'
        max_workers: 最大并发线程数，默认5个
    """
    import random
    
    df = pd.read_csv(csv_file)
    name_list = df['name'].tolist()
    id_list = df['id'].tolist()
    
    # 随机打乱站点顺序
    combined = list(zip(name_list, id_list))
    random.shuffle(combined)
    name_list, id_list = zip(*combined)
    name_list = list(name_list)
    id_list = list(id_list)
    
    output_root = Path(base_dir)
    first_scrape_date = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    
    print(f"\n{'='*60}")
    print(f"开始并发爬取任务 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"输出文件夹: {first_scrape_date}")
    print(f"总城市数: {len(name_list)}, 并发线程数: {max_workers}")
    print(f"{'='*60}\n")
    
    tracker = ProgressTracker(len(name_list))
    results = {}
    
    # Use ThreadPoolExecutor for concurrent scraping
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_city = {
            executor.submit(scrape_city_wrapper, city, city_id, False, output_root, tracker, first_scrape_date): (city, city_id)
            for city, city_id in zip(name_list, id_list)
        }
        
        # Process completed tasks
        for future in as_completed(future_to_city):
            city, city_id = future_to_city[future]
            try:
                city_name, result = future.result()
                results[city_name] = result
            except Exception as e:
                print(f"✗ Exception for {city_id}: {e}")
                results[city] = None
    
    print(f"\n{'='*60}")
    print(f"爬取任务完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"成功: {sum(1 for r in results.values() if r)}/{len(name_list)}")
    print(f"{'='*60}\n")
    
    return results


if __name__ == "__main__":
    import pytz
    import random
    
    # 定义配置参数
    CSV_FILE = 'nowcast_crawl_list_v3.csv'
    BASE_DIR = Path(__file__).parent
    DURATION_HOURS = 12  # 12小时内完成所有站点
    AVG_SCRAPE_TIME = 15  # 每个站点平均爬取时间（秒）
    
    # 设置调度器
    scheduler = BackgroundScheduler(timezone='UTC')
    
    def scheduled_crawl_task():
        """定时执行的爬取任务"""
        # 读取站点列表
        df = pd.read_csv(CSV_FILE)
        name_list = df['name'].tolist()
        id_list = df['id'].tolist()
        total_stations = len(name_list)
        
        print("\n" + "="*60)
        print(f"定时任务触发 - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("="*60)
        print(f"总站点数: {total_stations}")
        print(f"调度周期: {DURATION_HOURS} 小时")
        print(f"预计单站耗时: {AVG_SCRAPE_TIME} 秒")
        print("="*60 + "\n")
        
        # 生成随机调度时间表
        schedule = generate_random_schedule(total_stations, DURATION_HOURS, AVG_SCRAPE_TIME)
        
        # 获取当前时间作为起始时间
        start_time = datetime.now(timezone.utc)
        
        # 为每个站点添加调度任务
        scheduled_count = 0
        for i, (city, city_id, delay_seconds) in enumerate(zip(name_list, id_list, schedule)):
            # 计算执行时间
            run_time = start_time + timedelta(seconds=delay_seconds)
            
            # 添加一次性任务
            scheduler.add_job(
                scrape_single_city,
                'date',
                run_date=run_time,
                args=[city, city_id, BASE_DIR],
                id=f'scrape_{city_id}_{int(start_time.timestamp())}_{i}'
            )
            scheduled_count += 1
            
            # 每100个站点输出一次进度
            if (i + 1) % 100 == 0:
                print(f"已调度 {i + 1}/{total_stations} 个站点...")
        
        print(f"\n✓ 成功调度 {scheduled_count} 个站点")
        print(f"✓ 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        # 计算实际预计结束时间（最后一个任务的启动时间 + 爬取时间）
        actual_end_time = start_time + timedelta(seconds=max(schedule) + AVG_SCRAPE_TIME)
        print(f"✓ 预计结束: {actual_end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        actual_duration = (actual_end_time - start_time).total_seconds() / 3600
        print(f"✓ 实际持续时间: {actual_duration:.2f} 小时")
        print(f"✓ 首个任务将在 {min(schedule)} 秒后执行")
        print(f"✓ 最后任务将在 {max(schedule)} 秒后启动\n")
    
    # 添加定时任务：每天 UTC 0点和12点触发
    scheduler.add_job(scheduled_crawl_task, 'cron', hour='0,12', minute='0')
    
    print("="*60)
    print("定时爬虫已启动（随机调度模式）")
    print("="*60)
    print(f"✓ 输出目录: {BASE_DIR}")
    print(f"✓ CSV 文件: {CSV_FILE}")
    print(f"✓ 触发时间: 每天 UTC 00:00 和 12:00")
    print(f"✓ 当前 UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ 按 Ctrl+C 停止程序\n")
    
    # 启动调度器
    scheduler.start()
    
    # 立即执行一次（可选，用于测试）
    # scheduled_crawl_task()
    
    # 持续运行
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n\n程序已停止")
        scheduler.shutdown()


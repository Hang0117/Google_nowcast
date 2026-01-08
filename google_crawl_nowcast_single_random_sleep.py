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
import random


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

        return out

    except Exception as e:
        print(f"ERR [{city}]:", e)
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


def scrape_all_cities_concurrent(base_dir, csv_file='nowcast_crawl_list_v3.csv', max_workers=5, total_duration_hours=12, avg_scrape_time=15):
    """并发爬取所有城市的气象数据，在指定时间内分散执行
    
    Args:
        base_dir: 输出目录的基础路径
        csv_file: CSV 文件路径，默认为 'nowcast_crawl_list_v3.csv'
        max_workers: 最大并发线程数，默认5个
        total_duration_hours: 总执行时长（小时），默认12小时
        avg_scrape_time: 每个站点平均爬取时间（秒），默认15秒
    """
    # 确保必要的目录存在
    output_root = Path(base_dir)
    crawled_dir = output_root / "Crawled"
    html_dir = output_root / "GoogleNowcastHTML"
    
    if not crawled_dir.exists():
        crawled_dir.mkdir(parents=True, exist_ok=True)
        print(f"✓ 创建目录: {crawled_dir}")
    
    if not html_dir.exists():
        html_dir.mkdir(parents=True, exist_ok=True)
        print(f"✓ 创建目录: {html_dir}")
    
    df = pd.read_csv(csv_file)
    
    # randomly shuffle the DataFrame
    df = df.sample(frac=1, random_state=None).reset_index(drop=True)
    
    name_list = df['name'].tolist()
    id_list = df['id'].tolist()
    total_cities = len(name_list)
    
    # calculate total available time in seconds
    total_seconds = total_duration_hours * 3600
    total_scrape_time = total_cities * avg_scrape_time
    total_interval_time = total_seconds - total_scrape_time
    
    if total_interval_time < 0:
        print(f"⚠️  警告: {total_cities} 个站点需要约 {total_scrape_time/3600:.2f} 小时，超过设定的 {total_duration_hours} 小时")
        total_interval_time = 0
    
    # calculate average interval time per city
    avg_interval = total_interval_time / total_cities if total_cities > 0 else 0
    
    # generate random intervals for each city (between 50% and 150% of the average)
    intervals = []
    for i in range(total_cities):
        if avg_interval > 0:
            # Random offset ±50%
            random_interval = avg_interval * random.uniform(0.5, 1.5)
            intervals.append(random_interval)
        else:
            intervals.append(0)
    
    # Adjust intervals to ensure total duration is close to target
    if sum(intervals) > 0:
        scale_factor = total_interval_time / sum(intervals)
        intervals = [interval * scale_factor for interval in intervals]
    
    first_scrape_date = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    
    print(f"\n{'='*60}")
    print(f"开始分散爬取任务 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"输出文件夹: {first_scrape_date}")
    print(f"总城市数: {total_cities}, 并发线程数: {max_workers}")
    print(f"预计总时长: {total_duration_hours} 小时 ({total_seconds/3600:.2f}h)")
    print(f"预计爬取时间: {total_scrape_time/3600:.2f} 小时")
    print(f"预计间隔时间: {total_interval_time/3600:.2f} 小时")
    print(f"平均站点间隔: {avg_interval:.1f} 秒 (随机偏移 ±50%)")
    print(f"✓ 城市列表已随机打乱")
    print(f"{'='*60}\n")
    
    tracker = ProgressTracker(total_cities)
    results = {}
    start_time = time.time()
    
    # Use ThreadPoolExecutor for concurrent scraping
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 逐个提交任务，等待完成后再等待随机间隔
        for idx, (city, city_id) in enumerate(zip(name_list, id_list)):
            # 提交任务并立即等待完成
            future = executor.submit(scrape_city_wrapper, city, city_id, False, output_root, tracker, first_scrape_date)
            
            try:
                city_name, result = future.result()  # 等待任务完成（浏览器关闭）
                results[city_name] = result
            except Exception as e:
                print(f"✗ Exception for {city_id}: {e}")
                results[city] = None
            
            # 任务完成后，在提交下一个任务前等待随机间隔（最后一个任务不需要等待）
            if idx < total_cities - 1:
                sleep_time = intervals[idx]
                if sleep_time > 0:
                    elapsed = time.time() - start_time
                    expected_elapsed = sum(intervals[:idx+1]) + (idx + 1) * avg_scrape_time
                    # 调整sleep时间以保持整体节奏
                    adjusted_sleep = max(0, sleep_time - max(0, elapsed - expected_elapsed))
                    if adjusted_sleep > 0:
                        print(f"⏳ 等待 {adjusted_sleep:.1f} 秒后继续...")
                        time.sleep(adjusted_sleep)
    
    elapsed_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"爬取任务完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"实际用时: {elapsed_time/3600:.2f} 小时 ({elapsed_time:.0f} 秒)")
    print(f"成功: {sum(1 for r in results.values() if r)}/{total_cities}")
    print(f"{'='*60}\n")
    
    return results


if __name__ == "__main__":
    import pytz
    
    # settings parameters
    CSV_FILE = 'nowcast_crawl_list_v3.csv'
    MAX_WORKERS = 1
    BASE_DIR = Path(__file__).parent
    TOTAL_DURATION_HOURS = 12  # 每次爬取任务在12小时内完成
    AVG_SCRAPE_TIME = 15  # 每个站点平均爬取时间（秒）
    
    # 设置北京时区
    beijing_tz = pytz.timezone('Asia/Shanghai')
    
    # 使用 APScheduler 配置 UTC 时区的定时任务
    scheduler = BackgroundScheduler(timezone='UTC')
    
    # 每天 UTC 时间 0点、6点、12点、18点各执行一次
    # scheduler.add_job(lambda: scrape_all_cities_concurrent(base_dir=BASE_DIR, csv_file=CSV_FILE, max_workers=MAX_WORKERS, total_duration_hours=TOTAL_DURATION_HOURS, avg_scrape_time=AVG_SCRAPE_TIME), 'cron', hour='0,6,12,18')
    scheduler.add_job(lambda: scrape_all_cities_concurrent(base_dir=BASE_DIR, csv_file=CSV_FILE, max_workers=MAX_WORKERS, total_duration_hours=TOTAL_DURATION_HOURS, avg_scrape_time=AVG_SCRAPE_TIME), 'cron', hour='0')
    scheduler.start()
    
    print("✓ 定时爬虫已启动（分散爬取模式）")
    print(f"✓ 输出目录: {BASE_DIR}")
    print(f"✓ CSV 文件: {CSV_FILE}")
    print(f"✓ 将在每天 UTC 时间 00:00 执行爬取任务")
    print(f"✓ 并发线程数: {MAX_WORKERS}")
    print(f"✓ 每次任务时长: {TOTAL_DURATION_HOURS} 小时")
    print(f"✓ 平均爬取时间: {AVG_SCRAPE_TIME} 秒/站点")
    print(f"✓ 当前北京时间: {datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✓ 当前 UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ 按 Ctrl+C 停止程序\n")
    
    # 立即执行一次（可选）
    scrape_all_cities_concurrent(base_dir=BASE_DIR, csv_file=CSV_FILE, max_workers=MAX_WORKERS, total_duration_hours=TOTAL_DURATION_HOURS, avg_scrape_time=AVG_SCRAPE_TIME)
    
    # 持续运行调度器
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n\n程序已停止")
        scheduler.shutdown()

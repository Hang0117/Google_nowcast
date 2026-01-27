#!/usr/bin/env python3
"""Concurrent Google nowcast scraper with Playwright and multi-threading.

Uses Playwright for faster and more reliable browser automation.
"""
import os
import sys
import subprocess
import platform
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random


def _install_dependencies():
    """Auto-install Playwright and Python dependencies."""
    system = platform.system()
    
    # Install Python packages
    print("📦 Installing Python packages...")
    packages = ["playwright", "pandas", "apscheduler", "pytz"]
    
    try:
        import playwright
        import pandas
        from apscheduler.schedulers.background import BackgroundScheduler
        print("✅ Python packages already installed")
    except ImportError:
        print("📦 Installing required packages...")
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-q"] + packages,
                check=True
            )
            print("✅ Python packages installed")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Could not install Python packages: {e}")
            print("   Please install manually: pip install playwright pandas apscheduler pytz")
            raise
    
    # Install Playwright browsers
    try:
        from playwright.sync_api import sync_playwright
        print("✅ Playwright module available")
        
        # Check if browsers are installed by trying to launch
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()
            print("✅ Playwright browsers already installed")
        except Exception:
            print("📦 Installing Playwright browsers...")
            try:
                subprocess.run(
                    [sys.executable, "-m", "playwright", "install", "chromium"],
                    check=True
                )
                print("✅ Playwright browsers installed")
            except subprocess.CalledProcessError as e:
                print(f"⚠️  Could not install Playwright browsers: {e}")
                print("   Please install manually: playwright install chromium")
                raise
    except ImportError:
        print("⚠️  Playwright not available after installation")
        raise


# Auto-install dependencies when imported
_install_dependencies()

# Now import the required packages
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd


def scrape_nowcast_svg(
    city: str = "Fairfax, California, United States",
    city_id: str = "",
    headless: bool = True,
    save_json: bool = True,
    output_dir: str | Path | None = None,
    first_scrape_date: str | None = None,
):
    """Scrape rect heights from the SVG using Playwright."""
    base_dir = Path(output_dir) if output_dir else Path(__file__).parent

    out = {
        "city": city,
        "city_id": city_id,
        "scrape_time": datetime.now(timezone.utc).isoformat(),
        "type": None,
        "viewBox": None,
        "points": []
    }

    with sync_playwright() as p:
        try:
            # Launch browser with mobile emulation
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ]
            )
            
            # Create context with mobile device emulation
            context = browser.new_context(
                viewport={'width': 412, 'height': 915},
                device_scale_factor=2.625,
                is_mobile=True,
                has_touch=True,
                user_agent='Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
                locale='en-US',
                timezone_id='America/Los_Angeles'
            )
            
            page = context.new_page()
            
            # Navigate to Google
            page.goto("https://www.google.com/ncr?hl=en&gl=us", wait_until="domcontentloaded")
            
            # Handle consent dialog
            try:
                page.wait_for_selector('#L2AGLb', timeout=3000)
                page.click('#L2AGLb')
                page.wait_for_timeout(300)
            except PlaywrightTimeout:
                # Try alternative consent button
                try:
                    page.locator('button:has-text("Accept all"), button:has-text("I agree")').first.click(timeout=1000)
                    page.wait_for_timeout(300)
                except Exception:
                    pass
            
            # Search for weather
            from urllib.parse import quote_plus
            q = quote_plus(f"weather {city}")
            page.goto(f"https://www.google.com/search?q={q}&hl=en&gl=us", wait_until="domcontentloaded")
            
            page.wait_for_timeout(3000)
            
            # Save HTML page
            try:
                html_content = page.content()
                folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                html_dir = base_dir / "GoogleNowcastHTML" / folder_date
                html_dir.mkdir(parents=True, exist_ok=True)
                html_filename = f"{city_id}_{file_timestamp}.html"
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
            is_robot_check = page.evaluate(check_robot_js)
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
                browser.close()
                return out

            # Extract SVG rect data
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

            result = page.evaluate(js)
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
                
                fallback_result = page.evaluate(fallback_js)
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
                    
                    hourly_result = page.evaluate(hourly_js)
                    if hourly_result and hourly_result.get("found"):
                        print(f"[{city}] Hourly forecast OK: {hourly_result.get('count', 0)} items")
                        out["hourly_data"] = hourly_result.get("labels", [])
                        out["source"] = "hourly_aria_label"
                        out["type"] = "hourly"
                        result = {"viewBox": None, "rects": []}
                    else:
                        html = page.content()
                        dbg = base_dir / f"debug_nowcast_{city.split(',')[0].replace(' ', '_')}.html"
                        dbg.write_text(html, encoding="utf-8")
                        reason = hourly_result.get('reason') if hourly_result else 'unknown'
                        print(f"[{city}] No data found (reason: {reason}). Wrote {dbg.name}")
                        # Delete debug file after saving
                        try:
                            import time as time_module
                            time_module.sleep(0.5)
                            dbg.unlink()
                            print(f"[{city}] Debug file deleted: {dbg.name}")
                        except Exception as del_err:
                            print(f"[{city}] Could not delete debug file: {del_err}")
                        out["message"] = "no nowcast data now."
                        if save_json:
                            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                            outdir = base_dir / "Crawled" / folder_date
                            outdir.mkdir(parents=True, exist_ok=True)
                            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                        browser.close()
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
                file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"[{city}] Saved: {fname.name}")
            elif save_json and out.get("fallback_data"):
                folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"[{city}] Saved: {fname.name}")
            elif save_json and out.get("hourly_data"):
                folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"[{city}] Saved: {fname.name}")

            browser.close()
            return out

        except Exception as e:
            print(f"ERR [{city}]:", e)
            try:
                browser.close()
            except Exception:
                pass
            return None


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
    print(f"开始分散爬取任务 (Playwright) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    
    print("✓ 定时爬虫已启动（Playwright 版本 - 分散爬取模式）")
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

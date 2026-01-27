#!/usr/bin/env python3
"""Concurrent Google nowcast scraper with 20min work / 1hour rest cycle.

Uses ThreadPoolExecutor to scrape multiple cities in parallel.
Works in cycles: 20 minutes scraping, then 1 hour rest, until all cities done.
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


def scrape_all_cities_concurrent(base_dir, csv_file='nowcast_crawl_list_v3.csv', max_workers=5, work_duration_minutes=20, rest_duration_minutes=60):
    """并发爬取所有城市的气象数据，工作/休息循环模式
    
    Args:
        base_dir: 输出目录的基础路径
        csv_file: CSV 文件路径，默认为 'nowcast_crawl_list_v3.csv'
        max_workers: 最大并发线程数，默认5个
        work_duration_minutes: 工作时长（分钟），默认20分钟
        rest_duration_minutes: 休息时长（分钟），默认60分钟
    """
    df = pd.read_csv(csv_file)
    name_list = df['name'].tolist()
    id_list = df['id'].tolist()
    output_root = Path(base_dir)
    first_scrape_date = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    
    total = len(name_list)
    work_duration = timedelta(minutes=work_duration_minutes)
    rest_duration = timedelta(minutes=rest_duration_minutes)
    
    print(f"\n{'='*60}")
    print(f"开始并发爬取任务（循环模式）- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"输出文件夹: {first_scrape_date}")
    print(f"总城市数: {total}, 并发线程数: {max_workers}")
    print(f"工作时长: {work_duration_minutes}分钟, 休息时长: {rest_duration_minutes}分钟")
    print(f"{'='*60}\n")
    
    tracker = ProgressTracker(total)
    results = {}
    pending_cities = list(zip(name_list, id_list))  # 待爬取的城市队列
    cycle_num = 1
    
    while pending_cities:
        cycle_start = datetime.now(timezone.utc)
        window_end = cycle_start + work_duration
        
        print(f"\n{'='*60}")
        print(f"【第 {cycle_num} 轮工作周期开始】")
        print(f"开始时间: {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"预计结束: {window_end.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"剩余城市: {len(pending_cities)}/{total}")
        print(f"{'='*60}\n")
        
        # 在本轮工作窗口内并发爬取
        cycle_results = []
        executor = ThreadPoolExecutor(max_workers=max_workers)
        futures_dict = {}
        completed_in_cycle = []
        
        # 动态提交任务，在工作窗口内控制提交数量
        city_index = 0
        
        # 先提交第一批任务（max_workers个）
        while city_index < len(pending_cities) and city_index < max_workers:
            city, city_id = pending_cities[city_index]
            future = executor.submit(
                scrape_city_wrapper,
                city,
                city_id,
                False,
                output_root,
                tracker,
                first_scrape_date
            )
            futures_dict[future] = (city, city_id)
            city_index += 1
        
        # 收集完成的任务，在工作窗口内继续提交新任务
        # 使用while循环动态处理futures，而不是for循环
        while futures_dict:
            # 等待任意一个任务完成
            done_futures = []
            try:
                for future in as_completed(futures_dict, timeout=1):
                    done_futures.append(future)
                    break  # 只处理一个，然后检查时间和提交新任务
            except Exception:
                # timeout或其他异常，继续等待
                pass
            
            if not done_futures:
                continue
            
            future = done_futures[0]
            city, city_id = futures_dict.pop(future)  # 从字典中移除已完成的
            
            try:
                city_name, result = future.result()
                results[city_name] = result
                completed_in_cycle.append((city, city_id))
            except Exception as e:
                print(f"✗ Exception for {city_id}: {e}")
                results[city] = None
                completed_in_cycle.append((city, city_id))
            
            # 检查是否超过工作窗口
            if datetime.now(timezone.utc) >= window_end:
                print(f"\n⏰ 工作窗口已到 {work_duration_minutes} 分钟，停止提交新任务...")
                break
            
            # 如果还有待处理城市，继续提交新任务
            if city_index < len(pending_cities):
                city, city_id = pending_cities[city_index]
                new_future = executor.submit(
                    scrape_city_wrapper,
                    city,
                    city_id,
                    False,
                    output_root,
                    tracker,
                    first_scrape_date
                )
                futures_dict[new_future] = (city, city_id)
                city_index += 1
        
        # 关闭线程池，不再接受新任务，但等待已提交的任务完成
        print(f"\n等待本轮剩余任务完成...")
        executor.shutdown(wait=True)
        
        # 收集剩余未处理的结果（如果有的话）
        for future, (city, city_id) in list(futures_dict.items()):
            if (city, city_id) not in completed_in_cycle:
                try:
                    city_name, result = future.result(timeout=0)
                    results[city_name] = result
                    completed_in_cycle.append((city, city_id))
                except Exception as e:
                    print(f"✗ Exception for {city_id}: {e}")
                    results[city] = None
                    completed_in_cycle.append((city, city_id))
        
        # 从待处理队列中移除已完成的城市
        pending_cities = [(c, cid) for c, cid in pending_cities if (c, cid) not in completed_in_cycle]
        
        cycle_end = datetime.now(timezone.utc)
        cycle_duration = (cycle_end - cycle_start).total_seconds() / 60
        
        print(f"\n{'='*60}")
        print(f"【第 {cycle_num} 轮工作周期结束】")
        print(f"实际运行: {cycle_duration:.1f} 分钟")
        print(f"本轮完成: {len(completed_in_cycle)} 个城市")
        print(f"总进度: {tracker.completed}/{total} ({tracker.completed/total*100:.1f}%)")
        print(f"{'='*60}\n")
        
        # 如果还有待处理城市，休息后继续
        if pending_cities:
            print(f"💤 休息 {rest_duration_minutes} 分钟后继续下一轮...")
            print(f"下轮预计开始时间: {(datetime.now(timezone.utc) + rest_duration).strftime('%Y-%m-%d %H:%M:%S')}\n")
            time.sleep(rest_duration.total_seconds())
            cycle_num += 1
    
    print(f"\n{'='*60}")
    print(f"爬取任务完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总周期数: {cycle_num}")
    print(f"成功: {sum(1 for r in results.values() if r)}/{total}")
    print(f"{'='*60}\n")
    
    return results


if __name__ == "__main__":
    import pytz
    
    # 定义配置参数
    CSV_FILE = 'nowcast_crawl_list_v3.csv'
    MAX_WORKERS = 3
    BASE_DIR = Path(__file__).parent
    WORK_MINUTES = 20  # 工作时长：20分钟
    REST_MINUTES = 60  # 休息时长：60分钟
    
    # 设置北京时区
    beijing_tz = pytz.timezone('Asia/Shanghai')
    
    # 使用 APScheduler 配置 UTC 时区的定时任务
    scheduler = BackgroundScheduler(timezone='UTC')
    
    # 每天 UTC 时间 0、6、12、18点各执行一次
    scheduler.add_job(
        lambda: scrape_all_cities_concurrent(
            base_dir=BASE_DIR, 
            csv_file=CSV_FILE, 
            max_workers=MAX_WORKERS,
            work_duration_minutes=WORK_MINUTES,
            rest_duration_minutes=REST_MINUTES
        ), 
        'cron', 
        hour='18'
    )
    scheduler.start()
    
    print("✓ 定时爬虫已启动（循环工作/休息模式）")
    print(f"✓ 输出目录: {BASE_DIR}")
    print(f"✓ CSV 文件: {CSV_FILE}")
    print(f"✓ 工作模式: {WORK_MINUTES}分钟工作 / {REST_MINUTES}分钟休息")
    print(f"✓ 将在每天 UTC 时间 00:00, 06:00, 12:00, 18:00 执行爬取任务")
    print(f"✓ 并发线程数: {MAX_WORKERS}") 
    print(f"✓ 当前北京时间: {datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✓ 当前 UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ 按 Ctrl+C 停止程序\n")
    
    # 立即执行一次（可选）
    scrape_all_cities_concurrent(
        base_dir=BASE_DIR, 
        csv_file=CSV_FILE, 
        max_workers=MAX_WORKERS,
        work_duration_minutes=WORK_MINUTES,
        rest_duration_minutes=REST_MINUTES
    )
    
    # 持续运行调度器
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n\n程序已停止")
        scheduler.shutdown()

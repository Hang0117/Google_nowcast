#!/usr/bin/env python3
"""Concurrent Google nowcast scraper with multi-threading.

Uses ThreadPoolExecutor to scrape multiple cities in parallel.
"""
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import time
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def _chrome_driver(headless: bool = True):
    from selenium import webdriver
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

    # Selenium 4.6+ automatically manages ChromeDriver - no webdriver-manager needed
    return webdriver.Chrome(options=options)


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


def scrape_all_cities_concurrent(max_workers=5, batch_size=250, sleep_between_batches=600):
    """并发爬取所有城市的气象数据，按批次运行
    
    Args:
        max_workers: 最大并发线程数，默认5个
        batch_size: 每批城市数量，默认80；None/0 表示不分批
        sleep_between_batches: 批次间休眠秒数，默认0秒（连续运行）
    """
    df = pd.read_csv('nowcast_crawl_list_v3.csv')
    name_list = df['name'].tolist()
    id_list = df['id'].tolist()
    output_root = Path(__file__).parent
    first_scrape_date = datetime.now(timezone.utc).strftime("%Y%m%d%H")

    total = len(name_list)
    tracker = ProgressTracker(total)
    results = {}

    if not batch_size or batch_size <= 0:
        batch_size = total

    total_batches = (total + batch_size - 1) // batch_size

    print(f"\n{'='*60}")
    print(f"开始并发爬取任务 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"输出文件夹: {first_scrape_date}")
    print(f"总城市数: {total}, 并发线程数: {max_workers}, 批大小: {batch_size}, 批次数: {total_batches}")
    print(f"{'='*60}\n")

    for batch_idx, start_idx in enumerate(range(0, total, batch_size), start=1):
        end_idx = min(start_idx + batch_size, total)
        batch_cities = name_list[start_idx:end_idx]
        batch_ids = id_list[start_idx:end_idx]

        print(f"-- 开始第 {batch_idx}/{total_batches} 批，城市 {start_idx+1}-{end_idx} --")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_city = {
                executor.submit(
                    scrape_city_wrapper,
                    city,
                    city_id,
                    False,
                    output_root,
                    tracker,
                    first_scrape_date,
                ): (city, city_id)
                for city, city_id in zip(batch_cities, batch_ids)
            }

            for future in as_completed(future_to_city):
                city, city_id = future_to_city[future]
                try:
                    city_name, result = future.result()
                    results[city_name] = result
                except Exception as e:
                    print(f"✗ Exception for {city_id}: {e}")
                    results[city] = None

        print(f"-- 第 {batch_idx}/{total_batches} 批完成 --\n")

        if batch_idx < total_batches:
            minutes = sleep_between_batches / 60.0
            print(f"批次间休眠 {minutes:.1f} 分钟...")
            time.sleep(sleep_between_batches)

    print(f"\n{'='*60}")
    print(f"爬取任务完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"成功: {sum(1 for r in results.values() if r)}/{total}")
    print(f"{'='*60}\n")

    return results


if __name__ == "__main__":
    import pytz
    
    # 设置北京时区
    beijing_tz = pytz.timezone('Asia/Shanghai')
    
    # 使用 APScheduler 配置 UTC 时区的定时任务
    scheduler = BackgroundScheduler(timezone='UTC')
    
    # 每天 UTC 时间 0点、6点、12点、18点各执行一次
    # scheduler.add_job(lambda: scrape_all_cities_concurrent(max_workers=3), 'cron', hour='0,6,12,18')
    scheduler.add_job(lambda: scrape_all_cities_concurrent(max_workers=3), 'cron', hour='18')
    scheduler.start()
    
    print("✓ 定时爬虫已启动（并发模式）")
    print(f"✓ 将在每天 UTC 时间 00:00, 06:00, 12:00, 18:00 执行爬取任务")
    print(f"✓ 并发线程数: 3")
    print(f"✓ 当前北京时间: {datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✓ 当前 UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ 按 Ctrl+C 停止程序\n")
    
    # 立即执行一次（可选）
    # scrape_all_cities_concurrent(max_workers=3)
    
    # 持续运行调度器
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n\n程序已停止")
        scheduler.shutdown()

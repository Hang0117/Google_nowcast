#!/usr/bin/env python3
# Minimal-output, mobile-emulated Google Nowcast SVG scraper
# City: Fairfax, California, United States

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import time
import schedule
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
    # Mobile emulation keeps layout consistent with the provided screenshot
    mobile_emulation = {"deviceName": "Nexus 5"}
    options.add_experimental_option("mobileEmulation", mobile_emulation)
    # Explicit UA (mobile)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def _accept_consent(driver):
    # Best-effort acceptance of Google consent dialogs
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        # Common button id
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
    """Scrape rect heights from the SVG whose viewBox includes 1440 and 48.

    Returns a dict with fields: city, city_id, scrape_time, viewBox, points (list of {time,height,fill,x,y,width}).
    """
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as e:
        print("ERR: selenium not available:", e)
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
        # Open Google with US locale, then search
        driver.get("https://www.google.com/ncr?hl=en&gl=us")
        _accept_consent(driver)

        # Direct search URL keeps things simple
        from urllib.parse import quote_plus
        q = quote_plus(f"weather {city}")
        driver.get(f"https://www.google.com/search?q={q}&hl=en&gl=us")

        # Give time to render
        time.sleep(3)
        
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

        # Run JS to find the target SVG and collect rect heights
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
            # Fallback: try div[jsname="Kt2ahd"][class="XhUg9e"]
            print("No target SVG found. Trying fallback div...")
            fallback_js = """
            const div = document.querySelector('div[jsname="Kt2ahd"].XhUg9e');
            if (!div) return {found: false, reason: 'no_kt2ahd_div'};
            
            // Find the two target divs
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
                print(f"Fallback OK: found divs with classes SnOHQb/jz8NAf")
                print(f"  div1: {fallback_result.get('data', {}).get('div1_text', 'N/A')[:50]}")
                print(f"  div2: {fallback_result.get('data', {}).get('div2_text', 'N/A')[:50]}")
                # Store fallback data in output
                out["fallback_data"] = fallback_result.get("data")
                out["source"] = "fallback_div"
                out["type"] = "nowcast"
                # Ensure downstream logic has a result object
                result = {"viewBox": None, "rects": []}
            else:
                # Third fallback: try hourly forecast data (aria-label)
                print("Fallback div not found. Trying hourly forecast (aria-label)...")
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
                    print(f"Hourly forecast OK: found {hourly_result.get('count', 0)} items")
                    print(f"  Sample: {hourly_result.get('labels', [])[:3]}")
                    # Store hourly data in output
                    out["hourly_data"] = hourly_result.get("labels", [])
                    out["source"] = "hourly_aria_label"
                    out["type"] = "hourly"
                    # Ensure downstream logic has a result object
                    result = {"viewBox": None, "rects": []}
                else:
                    # All methods failed - save HTML for inspection
                    html = driver.page_source
                    dbg = base_dir / "debug_nowcast_fairfax.html"
                    dbg.write_text(html, encoding="utf-8")
                    reason = hourly_result.get('reason') if hourly_result else 'unknown'
                    print(f"No data found (reason: {reason}). Wrote {dbg.name}")
                    # Delete debug file after saving
                    try:
                        import time as time_module
                        time_module.sleep(0.5)  # Brief delay to ensure file is written
                        dbg.unlink()  # Delete the file
                        print(f"Debug file deleted: {dbg.name}")
                    except Exception as del_err:
                        print(f"Could not delete debug file: {del_err}")
                    # Persist a minimal JSON to indicate no data
                    out["message"] = "no nowcast data now."
                    if save_json:
                        folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                        file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        outdir = base_dir / "Crawled" / folder_date
                        outdir.mkdir(parents=True, exist_ok=True)
                        fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                        fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                        print("Saved:", fname)
                    return out

        out["viewBox"] = result.get("viewBox")
        if result.get("source"):
            out["source"] = result.get("source")
        if not out["type"]:  # Only set if not already set (e.g., by hourly fallback)
            out["type"] = "nowcast"
        rows = result.get("rects") or []
        start = datetime.fromisoformat(out["scrape_time"])  # base time
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
            file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            print("Saved:", fname)
        elif save_json and out.get("fallback_data"):
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            print("Saved:", fname)
        elif save_json and out.get("hourly_data"):
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            outdir = base_dir / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            print("Saved:", fname)

        return out

    except Exception as e:
        print("ERR:", e)
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def scrape_all_cities():
    """爬取所有城市的气象数据"""
    df = pd.read_csv('nowcast_crawl_list_v2.csv')
    name_list = df['name'].tolist()
    id_list = df['id'].tolist()
    output_root = Path(__file__).parent
    first_scrape_date = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    
    print(f"\n{'='*60}")
    print(f"开始爬取任务 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"输出文件夹: {first_scrape_date}")
    print(f"{'='*60}")
    
    for idx, (city, city_id) in enumerate(zip(name_list, id_list), 1):
        print(f"\n[{idx}/{len(name_list)}] Running nowcast SVG scraper for: {city} (ID: {city_id})")
        data = scrape_nowcast_svg(city, city_id=city_id, headless=False, save_json=True, output_dir=output_root, first_scrape_date=first_scrape_date)
        if data and data.get("points"):
            print(f"  ✓ OK points: {len(data['points'])}, viewBox: {data.get('viewBox')}")
        else:
            print(f"  ✗ No data scraped.")
        time.sleep(2)  # 爬取间隔，避免请求过快
    
    print(f"\n{'='*60}")
    print(f"爬取任务完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Keep output ASCII-only to avoid Windows codepage issues
    import pytz
    
    # 设置北京时区
    beijing_tz = pytz.timezone('Asia/Shanghai')
    
    # 每天北京时间 0点、6点、12点、18点各执行一次
    schedule.every().day.at("00:00").do(scrape_all_cities)
    schedule.every().day.at("06:00").do(scrape_all_cities)
    schedule.every().day.at("12:00").do(scrape_all_cities)
    schedule.every().day.at("18:00").do(scrape_all_cities)
    
    print("✓ 定时爬虫已启动")
    print(f"✓ 将在每天北京时间 00:00, 06:00, 12:00, 18:00 执行爬取任务")
    print(f"✓ 当前北京时间: {datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ 按 Ctrl+C 停止程序\n")
    
    # 立即执行一次（可选，注释掉则只在指定时间执行）
    scrape_all_cities()
    
    # 持续运行调度器
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次是否需要执行
    except KeyboardInterrupt:
        print("\n\n程序已停止")

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
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import threading
import random
import uuid
import socket
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def _get_local_ip():
    """获取本机 IP 地址"""
    try:
        # 创建一个 UDP socket，不实际发送数据
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            # 备用方案：获取主机名对应的 IP
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "无法获取"


def _get_public_ip():
    """获取公网 IP 地址（Google 看到的 IP）"""
    try:
        import requests
        # 从多个服务轮询，增加可靠性
        services = [
            "https://api.ipify.org?format=json",
            "https://checkip.amazonaws.com",
            "https://icanhazip.com",
        ]
        for service in services:
            try:
                if "ipify" in service:
                    response = requests.get(service, timeout=3)
                    return response.json().get("ip", "查询失败")
                else:
                    response = requests.get(service, timeout=3)
                    return response.text.strip()
            except Exception:
                continue
        return "无网络连接"
    except ImportError:
        return "requests 模块未安装"
    except Exception:
        return "查询失败"


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
            logging.info("⚠️  Warning: Some Python packages not installed. Please install manually:")
            logging.info("   pip install selenium webdriver-manager pandas apscheduler pytz")
        return
    
    # Linux: Install Chrome and Python dependencies
    logging.info("🔧 Checking and installing dependencies on Linux...")
    
    # Check if Chrome is installed
    chrome_check = subprocess.run(
        ["which", "google-chrome"], 
        capture_output=True
    )
    
    if chrome_check.returncode != 0:
        logging.info("📦 Installing Google Chrome...")
        try:
            # Try to use apt-get without sudo (works in containers running as root)
            subprocess.run(["apt-get", "update"], check=True, capture_output=True, timeout=60)
            subprocess.run(
                ["bash", "-c", 
                 "wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && "
                 "echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' > /etc/apt/sources.list.d/google-chrome.list"],
                check=True, capture_output=True, timeout=30
            )
            subprocess.run(["apt-get", "update"], check=True, capture_output=True, timeout=60)
            subprocess.run(
                ["apt-get", "install", "-y", "google-chrome-stable"],
                check=True, capture_output=True, timeout=120
            )
            logging.info("✅ Google Chrome installed successfully")
        except subprocess.CalledProcessError as e:
            logging.info(f"⚠️  Could not install Chrome: {e}")
            logging.info("   Continuing... Chrome may already be partially installed")
        except subprocess.TimeoutExpired:
            logging.info(f"⚠️  Chrome installation timed out")
            logging.info("   Continuing... Chrome may already be installed")
    else:
        logging.info("✅ Google Chrome already installed")
    
    # Install Python packages
    logging.info("📦 Installing Python packages...")
    packages = ["selenium", "webdriver-manager", "pandas", "apscheduler", "pytz", "requests"]
    try:
        subprocess.run(
            ["pip", "install", "-q"] + packages,
            check=True
        )
        logging.info("✅ Python packages installed")
    except subprocess.CalledProcessError as e:
        logging.info(f"⚠️  Could not install Python packages: {e}")
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
    # options.add_argument(
    #     "user-agent=Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    # )
    # Random User-Agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ]
    ua = random.choice(user_agents)
    options.add_argument(f"user-agent={ua}")

    # Isolated Chrome profile (cross-platform)
    import tempfile
    import os
    tmp_dir = tempfile.gettempdir()
    profile_dir = os.path.join(tmp_dir, f"chrome_profile_{uuid.uuid4()}")
    options.add_argument(f"--user-data-dir={profile_dir}")
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

    # 合并：只用一个driver实例，先抓取天气页面，再获取公网IP
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as e:
        logging.info(f"ERR [{city}]: selenium not available: {e}")
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
        # 1. 访问 Google 天气页面
        driver.get("https://www.google.com/ncr?hl=en&gl=us")
        _accept_consent(driver)

        from urllib.parse import quote_plus
        q = quote_plus(f"weather {city}")
        driver.get(f"https://www.google.com/search?q={q}&hl=en&gl=us")

        time.sleep(3)

        # 2. 获取公网IP（同一个driver实例，直接访问api.ipify.org）
        try:
            driver.execute_script("window.open('https://api.ipify.org?format=json', '_blank');")
            driver.switch_to.window(driver.window_handles[-1])
            time.sleep(1.5)
            ip_text = driver.find_element(By.TAG_NAME, "body").text
            import json as _json
            ip_info = _json.loads(ip_text)
            logging.info(f"[Browser Public IP] {ip_info.get('ip')}")
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        except Exception as e:
            logging.info(f"[Browser Public IP] 获取失败: {e}")

        # Save HTML page
        try:
            html_content = driver.page_source
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            html_dir = base_dir / "GoogleNowcastHTML" / folder_date
            html_dir.mkdir(parents=True, exist_ok=True)
            html_filename = f"{city_id}_{folder_date}.html"
            html_path = html_dir / html_filename
            html_path.write_text(html_content, encoding="utf-8")
            logging.info(f"[{city}] Saved HTML: {html_filename}")
        except Exception as e:
            logging.info(f"[{city}] Warning: Could not save HTML: {e}")

        # Check for reCAPTCHA robot verification
        check_robot_js = """
        const pageText = document.body.innerText;
        const hasRobotCheck = pageText.includes("I'm not a robot") || pageText.includes("unusual traffic");
        return hasRobotCheck;
        """
        is_robot_check = driver.execute_script(check_robot_js)
        if is_robot_check:
            logging.info("⚠ reCAPTCHA verification detected: 'I'm not a robot'")
            out["type"] = "robot"
            if save_json:
                folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                logging.info(f"Saved: {fname}")
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
            logging.info(f"[{city}] No target SVG found. Trying fallback div...")
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
                logging.info(f"[{city}] Fallback OK: found divs")
                out["fallback_data"] = fallback_result.get("data")
                out["source"] = "fallback_div"
                out["type"] = "nowcast"
                result = {"viewBox": None, "rects": []}
            else:
                logging.info(f"[{city}] Fallback div not found. Trying hourly forecast...")
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
                    logging.info(f"[{city}] Hourly forecast OK: {hourly_result.get('count', 0)} items")
                    out["hourly_data"] = hourly_result.get("labels", [])
                    out["source"] = "hourly_aria_label"
                    out["type"] = "hourly"
                    result = {"viewBox": None, "rects": []}
                else:
                    html = driver.page_source
                    dbg = base_dir / f"debug_nowcast_{city.split(',')[0].replace(' ', '_')}.html"
                    dbg.write_text(html, encoding="utf-8")
                    reason = hourly_result.get('reason') if hourly_result else 'unknown'
                    logging.info(f"[{city}] No data found (reason: {reason}). Wrote {dbg.name}")
                    # Delete debug file after saving
                    try:
                        import time as time_module
                        time_module.sleep(0.5)  # Brief delay to ensure file is written
                        dbg.unlink()  # Delete the file
                        logging.info(f"[{city}] Debug file deleted: {dbg.name}")
                    except Exception as del_err:
                        logging.info(f"[{city}] Could not delete debug file: {del_err}")
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
            logging.info(f"[{city}] Saved: {fname.name}")
        elif save_json and out.get("fallback_data"):
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            logging.info(f"[{city}] Saved: {fname.name}")
        elif save_json and out.get("hourly_data"):
            folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            logging.info(f"[{city}] Saved: {fname.name}")

        return out

    except Exception as e:
        logging.info(f"ERR [{city}]: {e}")
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# Thread-safe counter for progress tracking



def scrape_city_wrapper(city, city_id, headless, output_root, first_scrape_date):
    """Wrapper function for concurrent scraping (no tracker, for process pool)."""
    result = scrape_nowcast_svg(city, city_id=city_id, headless=headless, save_json=True, output_dir=output_root, first_scrape_date=first_scrape_date)
    return city, result


def scrape_all_cities_concurrent(base_dir, csv_file='nowcast_crawl_list_v4.csv', max_workers=5, random_group=None):
    """并发爬取所有城市的气象数据，在指定时间内分散执行
    
    Args:
        base_dir: 输出目录的基础路径
        csv_file: CSV 文件路径，默认为 'nowcast_crawl_list_v4.csv'
        max_workers: 最大并发线程数，默认5个
    """
    # 确保必要的目录存在
    output_root = Path(base_dir)
    crawled_dir = output_root / "Crawled"
    html_dir = output_root / "GoogleNowcastHTML"
    
    if not crawled_dir.exists():
        crawled_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"✓ 创建目录: {crawled_dir}")
    
    if not html_dir.exists():
        html_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"✓ 创建目录: {html_dir}")
    
    # 检查 CSV 文件是否存在
    csv_path = Path(csv_file)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 文件不存在: {csv_file}")
    
    df = pd.read_csv(csv_file)
    # 如果指定了random_group，只保留该组
    if random_group is not None:
        if 'random_group' not in df.columns:
            raise ValueError('CSV文件缺少random_group列')
        df = df[df['random_group'] == int(random_group)]
        if df.empty:
            raise ValueError(f'random_group={random_group} 没有任何station')
    # 打乱顺序
    df = df.sample(frac=1, random_state=None).reset_index(drop=True)
    name_list = df['search_name'].tolist()
    id_list = df['id'].tolist()
    total_cities = len(name_list)
    
    # 直接并发执行所有任务，无等待间隔
    first_scrape_date = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    logging.info(f"\n{'='*60}")
    logging.info(f"开始并发爬取任务 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"输出文件夹: {first_scrape_date}")
    logging.info(f"总城市数: {total_cities}, 并发进程数: {max_workers}")
    logging.info(f"✓ 城市列表已随机打乱")
    logging.info(f"{'='*60}\n")

    results = {}
    failed_stations = []  # 记录失败的station名
    start_time = time.time()
    completed = 0
    # 用 ProcessPoolExecutor 并发提交所有任务（无等待间隔, 3进程）
    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = []
        for city, city_id in zip(name_list, id_list):
            futures.append(executor.submit(scrape_city_wrapper, city, city_id, True, output_root, first_scrape_date))
        # 用 zip 绑定 future 和城市名/id，保证异常分支也能拿到 city_name
        for future, city_name, city_id in zip(futures, name_list, id_list):
            try:
                city_name_result, result = future.result()
                completed += 1
                if result and result.get("points"):
                    logging.info(f"[{completed}/{total_cities}] ✓ {city_name_result}: {len(result['points'])} points")
                elif result and result.get("hourly_data"):
                    logging.info(f"[{completed}/{total_cities}] ✓ {city_name_result}: {len(result['hourly_data'])} hourly items")
                elif result and result.get("fallback_data"):
                    logging.info(f"[{completed}/{total_cities}] ✓ {city_name_result}: fallback data")
                else:
                    logging.info(f"[{completed}/{total_cities}] ✗ {city_name_result}: No data")
                    failed_stations.append((city_name_result, city_id))
                results[city_name_result] = result
            except Exception as e:
                completed += 1
                logging.info(f"[{completed}/{total_cities}] ✗ Exception: {e}")
                failed_stations.append((city_name, city_id))

    elapsed_time = time.time() - start_time
    logging.info(f"\n{'='*60}")
    logging.info(f"爬取任务完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"实际用时: {elapsed_time/3600:.2f} 小时 ({elapsed_time:.0f} 秒)")
    logging.info(f"成功: {sum(1 for r in results.values() if r)}/{total_cities}")
    logging.info(f"{'='*60}\n")

    # 失败重试逻辑
    max_retry = 5
    retry_count = 0
    while failed_stations and retry_count < max_retry:
        retry_count += 1
        logging.info(f"等待10分钟后重试失败的站点，第{retry_count}轮，共{len(failed_stations)}个...")
        time.sleep(600)  # 10分钟
        # 在重试阶段打乱 failed_stations 顺序
        random.shuffle(failed_stations)
        retry_failed = []
        for city_name, city_id in failed_stations:
            # 删除旧json/html文件
            try:
                folder_date = first_scrape_date
                # 删除Crawled下的json
                for f in (output_root / "Crawled" / folder_date).glob(f"nowcast_{city_id}_*.json"):
                    f.unlink()
                # 删除GoogleNowcastHTML下的html
                for f in (output_root / "GoogleNowcastHTML" / folder_date).glob(f"{city_id}_{folder_date}.html"):
                    f.unlink()
            except Exception as del_err:
                logging.info(f"[重试] 删除旧文件失败: {del_err}")
            # 重新爬取
            try:
                _, result = scrape_city_wrapper(city_name, city_id, True, output_root, first_scrape_date)
                if result and result.get("points"):
                    logging.info(f"[重试{retry_count}] ✓ {city_name}: {len(result['points'])} points")
                elif result and result.get("hourly_data"):
                    logging.info(f"[重试{retry_count}] ✓ {city_name}: {len(result['hourly_data'])} hourly items")
                elif result and result.get("fallback_data"):
                    logging.info(f"[重试{retry_count}] ✓ {city_name}: fallback data")
                else:
                    logging.info(f"[重试{retry_count}] ✗ {city_name}: No data")
                    retry_failed.append((city_name, city_id))
                results[city_name] = result
            except Exception as e:
                logging.info(f"[重试{retry_count}] ✗ Exception: {e}")
                retry_failed.append((city_name, city_id))
        failed_stations = retry_failed
    if failed_stations:
        logging.info(f"最终仍有{len(failed_stations)}个站点爬取失败: {[c for c, _ in failed_stations]}")
    return results


if __name__ == "__main__":
    import pytz

    # settings parameters
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--group', type=str, default=None, help='只爬取指定random_group编号的station（1~7）')
    args = parser.parse_args()

    CSV_FILE = 'nowcast_crawl_list_v4_with_group.csv'
    MAX_WORKERS = 3
    BASE_DIR = Path(__file__).parent
    # 已移除 TOTAL_DURATION_HOURS 和 AVG_SCRAPE_TIME

    # 设置北京时区
    beijing_tz = pytz.timezone('Asia/Shanghai')

    local_ip = _get_local_ip()
    public_ip = _get_public_ip()
    logging.info("✓ 定时爬虫已启动（分组爬取模式）")
    logging.info(f"✓ 本机 IP (内网): {local_ip}")
    logging.info(f"✓ 公网 IP (Google看到): {public_ip}")
    logging.info(f"✓ 输出目录: {BASE_DIR}")
    logging.info(f"✓ CSV 文件: {CSV_FILE}")
    logging.info(f"✓ 并发线程数: {MAX_WORKERS}")
    # logging.info(f"✓ 每次任务时长: {TOTAL_DURATION_HOURS} 小时")
    # logging.info(f"✓ 平均爬取时间: {AVG_SCRAPE_TIME} 秒/站点")
    logging.info(f"✓ 当前北京时间: {datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"✓ 当前 UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"✓ 只爬取 random_group = {args.group}" if args.group else "✓ 爬取全部分组")
    logging.info("✓ 按 Ctrl+C 停止程序\n")

    # 立即执行一次
    try:
        scrape_all_cities_concurrent(base_dir=BASE_DIR, csv_file=CSV_FILE, max_workers=MAX_WORKERS, random_group=args.group)
        logging.info("\n✓ 爬虫任务完成")
    except Exception as e:
        logging.info(f"\n✗ 爬虫任务失败: {e}")
        raise


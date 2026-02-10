#!/usr/bin/env python3
"""Concurrent Google nowcast scraper with multi-threading and scheduled execution.

Uses ThreadPoolExecutor to scrape multiple cities in parallel.

Two execution modes:
  1. devbox mode: Scheduled to run daily at UTC 00:00, uploads to Azure with _devbox suffix
  2. main_sub_group mode: One-time execution for specific main_group/sub_group segments

Usage:
  # DevBox mode (scheduled with Azure upload)
  python google_crawl_nowcast_scheduled_azap_devbox.py --mode devbox

  # Main/Sub group mode (one-time execution)
  python google_crawl_nowcast_scheduled_azap_devbox.py --mode main_sub_group \
    --main_group 1 --sub_group 2 --out_path ./output --station_list ./stations.csv
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random
import uuid
import socket
import argparse

# Configure logging (both console and file)
import sys
from datetime import datetime, timezone
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Log')
os.makedirs(log_dir, exist_ok=True)
log_filename = datetime.now(timezone.utc).strftime('%Y%m%d%H') + '.log'
log_file = os.path.join(log_dir, log_filename)

# 设置控制台输出为UTF-8编码
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Reduce noisy Azure HTTP request logs
for _logger_name in [
    "azure.core.pipeline.policies.http_logging_policy",
    "azure.storage",
    "azure"
]:
    logging.getLogger(_logger_name).setLevel(logging.WARNING)

# 统一日志时间为 UTC，并在前缀标注 UTC
import time as _time
for _h in logging.getLogger().handlers:
    _fmt = logging.Formatter('%(asctime)s UTC - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    _fmt.converter = _time.gmtime
    _h.setFormatter(_fmt)


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
            # Add Google Chrome repository and install (auto-resolves dependencies)
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


def _chrome_driver(headless: bool = True):
    """Create Chrome WebDriver instance."""
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager
    
    # 检测操作系统
    system = platform.system()
    
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Nexus 5 mobile emulation
    mobile_emulation = {
        "deviceMetrics": {"width": 360, "height": 640, "pixelRatio": 3.0},
        "userAgent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36"
    }
    chrome_options.add_experimental_option("mobileEmulation", mobile_emulation)
    
    # Random User-Agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ]
    ua = random.choice(user_agents)
    chrome_options.add_argument(f"user-agent={ua}")

    # Isolated Chrome profile (cross-platform)
    import tempfile
    import os
    tmp_dir = tempfile.gettempdir()
    profile_dir = os.path.join(tmp_dir, f"chrome_profile_{uuid.uuid4()}")
    chrome_options.add_argument(f"--user-data-dir={profile_dir}")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


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
        "scrape_time": None,  # 将在保存 HTML 时设置
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
            # 切回后等待页面完全加载
            time.sleep(2)
        except Exception as e:
            logging.info(f"[Browser Public IP] 获取失败: {e}")
            # 确保切回主窗口
            try:
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(2)
            except:
                pass

        # Save HTML page - 在此时获取统一时间戳
        try:
            html_content = driver.page_source
            # 统一时间戳：在保存 HTML 时获取，用于 scrape_time、HTML 文件名、JSON 文件名
            scrape_time_obj = datetime.now(timezone.utc)
            out["scrape_time"] = scrape_time_obj.isoformat()  # 更新 scrape_time
            folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
            file_timestamp = scrape_time_obj.strftime("%Y%m%d%H%M%S")
            html_dir = base_dir / "GoogleNowcastHTML" / folder_date
            html_dir.mkdir(parents=True, exist_ok=True)
            html_filename = f"{city_id}_{file_timestamp}.html"
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
                folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
                file_timestamp = scrape_time_obj.strftime("%Y%m%d_%H%M%S")
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
        else:
            # SVG found successfully - process with time alignment
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
                idx = row.get("idx", 0)
                offset_min = idx * 10
                ts = start + timedelta(minutes=offset_min)
                row["time"] = ts.isoformat()
            out["points"] = rows
            logging.info(f"✓ SVG extraction successful for {city}")
            if save_json:
                folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
                file_timestamp = scrape_time_obj.strftime("%Y%m%d_%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                logging.info(f"Saved: {fname}")
            return out

        # Fallback 1: Try enhanced jsname="Kt2ahd" div with specific classes
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
            if save_json:
                folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
                file_timestamp = scrape_time_obj.strftime("%Y%m%d%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                logging.info(f"[{city}] Saved: {fname.name}")
            return out
        else:
            logging.info(f"[{city}] Fallback div not found. Trying hourly forecast...")

        # Fallback 2: Enhanced hourly forecast with strict selector (limit to 6 items)
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
            if save_json:
                folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
                file_timestamp = scrape_time_obj.strftime("%Y%m%d%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                logging.info(f"[{city}] Saved: {fname.name}")
            return out
        else:
            # No data found - save debug HTML and delete
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
                folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
                file_timestamp = scrape_time_obj.strftime("%Y%m%d%H%M%S")
                outdir = base_dir / "Crawled" / folder_date
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            return out

        # Fallback 3: Try alternative hourly selectors
        alternative_selectors = [
            'div[data-attrid="HourlyWeatherContent"]',
            'div.wob_df',
            'div[id="wob_dp"]',
        ]
        for selector in alternative_selectors:
            try:
                js_alt = f"""
                const container = document.querySelector('{selector}');
                if (!container) return null;
                const items = container.querySelectorAll('[aria-label]');
                if (items.length === 0) return null;
                return Array.from(items).map(el => el.getAttribute('aria-label')).filter(Boolean);
                """
                result_alt = driver.execute_script(js_alt)
                if result_alt and len(result_alt) > 0:
                    out["type"] = "hourly"
                    out["hourly_data"] = result_alt
                    out["source"] = f"alternative_{selector}"
                    logging.info(f"✓ Alternative hourly extraction successful for {city} using {selector}")
                    if save_json:
                        folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
                        file_timestamp = scrape_time_obj.strftime("%Y%m%d%H%M%S")
                        outdir = base_dir / "Crawled" / folder_date
                        outdir.mkdir(parents=True, exist_ok=True)
                        fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                        fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                        logging.info(f"Saved: {fname}")
                    return out
            except Exception as e:
                logging.info(f"Alternative selector {selector} failed: {e}")
                continue

        logging.info(f"✗ No nowcast data found for {city}")
        out["message"] = "no nowcast data now."
        if save_json:
            folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
            file_timestamp = scrape_time_obj.strftime("%Y%m%d%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            logging.info(f"Saved: {fname}")

        return out

    except Exception as e:
        logging.info(f"ERR [{city}]: {e}")
        out["error"] = str(e)
        if save_json:
            folder_date = first_scrape_date if first_scrape_date else scrape_time_obj.strftime("%Y%m%d%H")
            file_timestamp = scrape_time_obj.strftime("%Y%m%d_%H%M%S")
            outdir = base_dir / "Crawled" / folder_date
            outdir.mkdir(parents=True, exist_ok=True)
            fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
            fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return None
    finally:
        try:
            driver.quit()
        except:
            pass


class ProgressTracker:
    """线程安全的进度追踪器"""
    def __init__(self, total):
        self.total = total
        self.completed = 0
        self.lock = threading.Lock()
    
    def increment(self):
        with self.lock:
            self.completed += 1
            return self.completed


def scrape_city_wrapper(city, city_id, headless, output_root, tracker, first_scrape_date):
    completed = tracker.increment()
    result = scrape_nowcast_svg(city, city_id=city_id, headless=headless, save_json=True, output_dir=output_root, first_scrape_date=first_scrape_date)
    if result:
        if result.get("points"):
            logging.info(f"[{completed}/{tracker.total}] ✓ {city_id}: {len(result['points'])} points")
        elif result.get("hourly_data"):
            logging.info(f"[{completed}/{tracker.total}] ✓ {city_id}: {len(result['hourly_data'])} hourly items")
        elif result.get("type") == "div_fallback":
            logging.info(f"[{completed}/{tracker.total}] ✓ {city_id}: fallback data")
        else:
            logging.info(f"[{completed}/{tracker.total}] ✗ {city_id}: No data")
    else:
        logging.info(f"[{completed}/{tracker.total}] ✗ {city_id}: Error")
    return result


def scrape_all_cities_concurrent(
    base_dir: Path,
    csv_file: str,
    max_workers: int = 1,
    total_duration_hours: float = 12,
    avg_scrape_time: int = 15,
    main_group: str = None,
    sub_group: str = None,
    stagger_submissions: bool = False,
):
    """Concurrent scraping with ThreadPoolExecutor and optional staggered scheduling
    
    Args:
        stagger_submissions: If True, stagger task submissions over time (DevBox mode).
                           If False, submit all tasks immediately (Main/Sub Group mode).
    """
    import pandas as pd
    
    df = pd.read_csv(csv_file)

    # 按日期种子打乱 + 分主分组 + 子分组（与 legacy main_sub_group 脚本一致，无需 random_group 列）
    date_seed = int(datetime.now(timezone.utc).strftime('%Y%m%d'))
    df = df.sample(frac=1, random_state=date_seed).reset_index(drop=True)

    if main_group is not None:
        main_group_num = int(main_group)
        if main_group_num < 1 or main_group_num > 7:
            raise ValueError(f"main_group 参数必须在 1-7 之间，当前值: {main_group_num}")

        total_stations = len(df)
        main_size = total_stations // 7
        remainder = total_stations % 7
        start_idx = (main_group_num - 1) * main_size + min(main_group_num - 1, remainder)
        end_idx = start_idx + main_size + (1 if main_group_num <= remainder else 0)
        df = df.iloc[start_idx:end_idx].reset_index(drop=True)

        main_seed = int(f"{date_seed}{main_group_num}")
        df = df.sample(frac=1, random_state=main_seed).reset_index(drop=True)
        logging.info(f"✓ 选择主分组 {main_group_num} [{start_idx}:{end_idx}]，共 {len(df)} 个站点，seed={main_seed}")

    if sub_group is not None:
        sub_group_num = int(sub_group)
        if sub_group_num < 1 or sub_group_num > 4:
            raise ValueError(f"sub_group 参数必须在 1-4 之间，当前值: {sub_group_num}")

        total_stations = len(df)
        sub_size = total_stations // 4
        remainder = total_stations % 4
        start_idx = (sub_group_num - 1) * sub_size + min(sub_group_num - 1, remainder)
        end_idx = start_idx + sub_size + (1 if sub_group_num <= remainder else 0)
        df = df.iloc[start_idx:end_idx].reset_index(drop=True)

        if main_group is not None:
            sub_seed = int(f"{date_seed}{int(main_group)}{sub_group_num}")
        else:
            sub_seed = int(f"{date_seed}{sub_group_num}")
        df = df.sample(frac=1, random_state=sub_seed).reset_index(drop=True)
        logging.info(f"✓ 选择子分组 {sub_group_num} [{start_idx}:{end_idx}]，共 {len(df)} 个站点，seed={sub_seed}")
    
    # 如果既不指定主分组也不指定子分组，用当日种子打乱（同一天一致，每天不同）
    if main_group is None and sub_group is None:
        df = df.sample(frac=1, random_state=date_seed).reset_index(drop=True)
    
    name_list = df["search_name"].tolist()
    id_list = df["id"].tolist()
    
    first_scrape_date = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    logging.info(f"Total cities to scrape: {len(name_list)}")
    logging.info(f"First scrape date: {first_scrape_date}")
    
    output_root = base_dir
    tracker = ProgressTracker(total=len(name_list))
    
    results = []
    failed = []
    
    # generate random intervals for each city (between 50% and 150% of the average)
    total_duration_sec = total_duration_hours * 3600
    avg_interval = total_duration_sec / len(name_list) if len(name_list) > 0 else avg_scrape_time
    intervals = []
    for i in range(len(name_list)):
        if i < len(name_list) - 1:
            # Random offset ±50%
            random_interval = avg_interval * random.uniform(0.5, 1.5)
            intervals.append(random_interval)
        else:
            intervals.append(0)  # Last city doesn't need to wait
    
    logging.info(f"开始爬取 {len(name_list)} 个城市...")
    if stagger_submissions:
        logging.info("任务提交模式: 时间分布（DevBox模式）")
    else:
        logging.info("任务提交模式: 快速提交（Main/Sub Group模式）")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, (city, city_id) in enumerate(zip(name_list, id_list)):
            # Calculate expected submission time (only used if stagger_submissions=True)
            if stagger_submissions:
                expected_submit_time = start_time + sum(intervals[:idx])
                current_time = time.time()
                wait_time = expected_submit_time - current_time
                
                if wait_time > 0:
                    logging.info(f"等待 {wait_time:.1f} 秒后爬取下一个城市...")
                    time.sleep(wait_time)
            
            # Submit the task
            future = executor.submit(scrape_city_wrapper, city, city_id, True, output_root, tracker, first_scrape_date)
            futures[future] = (city, city_id)
            
            if stagger_submissions and idx < len(name_list) - 1:
                logging.info(f"[{idx+1}/{len(name_list)}] 已提交: {city_id}")
        
        # Collect results
        for future in as_completed(futures):
            city, city_id = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                else:
                    failed.append((city, city_id))
            except Exception as e:
                logging.info(f"✗ Exception for {city_id}: {e}")
                failed.append((city, city_id))
    
    elapsed = time.time() - start_time
    logging.info(f"\n爬取完成！耗时: {elapsed/60:.1f} 分钟")
    logging.info(f"成功: {len(results)}/{len(name_list)}, 失败: {len(failed)}")
    
    # Retry failed cities once
    if failed:
        logging.info(f"重试失败的 {len(failed)} 个城市...")
        retry_failed = []
        for city, city_id in failed:
            try:
                result = scrape_city_wrapper(city, city_id, True, output_root, tracker, first_scrape_date)
                if result:
                    results.append(result)
                    logging.info(f"重试成功: {city_id}")
                else:
                    retry_failed.append((city, city_id))
            except Exception as e:
                logging.info(f"重试失败: {city_id} ({e})")
                retry_failed.append((city, city_id))
        
        if retry_failed:
            logging.info(f"重试后仍失败 {len(retry_failed)} 个: {[c for c, _ in retry_failed]}")

    return results, first_scrape_date


def upload_to_azure(base_dir, folder_date, container_prefix="GoogleNowcast", folder_suffix="_devbox"):
    """上传爬取的数据到 Azure Blob 存储

    Args:
        base_dir: 本地数据根目录
        folder_date: 爬取日期文件夹（如 2026012700）
        container_prefix: Azure Blob 容器中的路径前缀，默认为 "GoogleNowcast"
        folder_suffix: 上传文件夹后缀，默认为 "_devbox"
    """
    try:
        from azure_wrapper import get_wxforecasting_azure_wrapper
        
        base_dir = Path(base_dir)
        logging.info("开始上传数据到 Azure Blob 存储...")
        logging.info(f"Azure 路径前缀: {container_prefix}")
        
        # 获取 Azure Wrapper
        wrapper = get_wxforecasting_azure_wrapper()
        
        # 上传 Crawled 数据
        crawled_dir = base_dir / "Crawled" / folder_date
        if crawled_dir.exists():
            azure_path = f"{container_prefix}/Crawled/{folder_date}{folder_suffix}"
            logging.info(f"📤 上传 Crawled 数据:")
            logging.info(f"   目标路径: {azure_path}")
            success, total = wrapper.upload_folder(
                local_folder=str(crawled_dir),
                container_prefix=azure_path,
                filename_suffix=folder_suffix
            )
            logging.info(f"   ✓ 上传完成: {success}/{total} 文件")
        else:
            logging.info(f"⚠️  跳过 Crawled 数据上传: 目录不存在 {crawled_dir}")
        
        # 上传 HTML 数据
        html_dir = base_dir / "GoogleNowcastHTML" / folder_date
        if html_dir.exists():
            azure_path = f"{container_prefix}/GoogleNowcastHTML/{folder_date}{folder_suffix}"
            logging.info(f"📤 上传 HTML 数据:")
            logging.info(f"   目标路径: {azure_path}")
            success, total = wrapper.upload_folder(
                local_folder=str(html_dir),
                container_prefix=azure_path,
                filename_suffix=folder_suffix
            )
            logging.info(f"   ✓ 上传完成: {success}/{total} 文件")
        else:
            logging.info(f"⚠️  跳过 HTML 数据上传: 目录不存在 {html_dir}")
        
        logging.info("✅ Azure 上传全部完成")
        
    except ImportError as e:
        logging.info(f"⚠️  Azure 上传跳过: 缺少依赖库 ({e})")
    except ValueError as e:
        logging.info(f"⚠️  Azure 上传跳过: {e}")
    except Exception as e:
        logging.info(f"❌ Azure 上传失败: {e}")


def run_devbox_mode(csv_file=None):
    """DevBox mode: Execute once and exit"""
    import pytz

    # settings parameters
    CSV_FILE = csv_file or 'Q:\\Code\\Google_nowcast\\nowcast_crawl_list_v7_devbox.csv'
    MAX_WORKERS = 1
    BASE_DIR = Path(__file__).parent
    TOTAL_DURATION_HOURS = 12  # 每次爬取任务在12小时内完成
    AVG_SCRAPE_TIME = 15  # 每个站点平均爬取时间（秒）
    AZURE_CONTAINER_PREFIX = "GoogleNowcast"  # Azure Blob 容器中的路径前缀

    # 设置时区
    beijing_tz = pytz.timezone('Asia/Shanghai')
    start_time = datetime.now(timezone.utc)

    local_ip = _get_local_ip()
    public_ip = _get_public_ip()
    logging.info("✓ 爬虫已启动（DevBox 模式 - 执行一次后退出）")
    logging.info(f"✓ 本机 IP (内网): {local_ip}")
    logging.info(f"✓ 公网 IP (Google看到): {public_ip}")
    logging.info(f"✓ 输出目录: {BASE_DIR}")
    logging.info(f"✓ CSV 文件: {CSV_FILE}")
    logging.info(f"✓ 并发线程数: {MAX_WORKERS}")
    logging.info(f"✓ 每次任务时长: {TOTAL_DURATION_HOURS} 小时")
    logging.info(f"✓ 平均爬取时间: {AVG_SCRAPE_TIME} 秒/站点")
    logging.info(f"✓ Azure 上传路径: {AZURE_CONTAINER_PREFIX}")
    logging.info(f"✓ 程序启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logging.info(f"✓ 当前北京时间: {datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"✓ 当前 UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        results, folder_date = scrape_all_cities_concurrent(
            base_dir=BASE_DIR,
            csv_file=CSV_FILE,
            max_workers=MAX_WORKERS,
            total_duration_hours=TOTAL_DURATION_HOURS,
            avg_scrape_time=AVG_SCRAPE_TIME,
            stagger_submissions=True,
        )
        logging.info("\n✓ 爬虫任务完成")
        
        # 爬取完成后上传到 Azure
        upload_to_azure(BASE_DIR, folder_date, AZURE_CONTAINER_PREFIX)
        logging.info("✓ 程序结束")
    except KeyboardInterrupt:
        logging.info("\n收到停止信号，程序即将退出")
    except Exception as e:
        logging.info(f"\n✗ 爬虫任务失败: {e}")
        raise


def run_main_sub_group_mode(args):
    """Main/Sub group mode: One-time execution for specific segment"""
    import pytz

    CSV_FILE = args.station_list
    BASE_DIR = Path(args.out_path)
    MAX_WORKERS = 1
    TOTAL_DURATION_HOURS = 20 / 60  # 每次爬取任务在20分钟内完成
    AVG_SCRAPE_TIME = 15  # 每个站点平均爬取时间（秒）

    # 设置时区
    beijing_tz = pytz.timezone('Asia/Shanghai')

    local_ip = _get_local_ip()
    public_ip = _get_public_ip()
    logging.info("✓ 定时爬虫已启动（Main/Sub Group 模式）")
    logging.info(f"✓ 本机 IP (内网): {local_ip}")
    logging.info(f"✓ 公网 IP (Google看到): {public_ip}")
    logging.info(f"✓ 主分组: {args.main_group}, 子分组: {args.sub_group}")
    logging.info(f"✓ 输出目录: {BASE_DIR}")
    logging.info(f"✓ CSV 文件: {CSV_FILE}")
    logging.info(f"✓ 并发线程数: {MAX_WORKERS}")
    logging.info(f"✓ 每次任务时长: {TOTAL_DURATION_HOURS} 小时")
    logging.info(f"✓ 平均爬取时间: {AVG_SCRAPE_TIME} 秒/站点")
    logging.info(f"✓ 当前北京时间: {datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"✓ 当前 UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("✓ 按 Ctrl+C 停止程序\n")

    # 立即执行一次（无定时调度，无循环重试），结束后直接退出
    try:
        scrape_all_cities_concurrent(
            base_dir=BASE_DIR,
            csv_file=CSV_FILE,
            max_workers=MAX_WORKERS,
            total_duration_hours=TOTAL_DURATION_HOURS,
            avg_scrape_time=AVG_SCRAPE_TIME,
            main_group=args.main_group,
            sub_group=args.sub_group,
            stagger_submissions=False,
        )
        logging.info("\n✓ 爬虫任务完成，程序结束")
    except KeyboardInterrupt:
        logging.info("\n收到停止信号，程序即将退出")
    except Exception as e:
        logging.info(f"\n✗ 爬虫任务失败: {e}")
        raise
    logging.info("程序已停止")


if __name__ == "__main__":
    # Auto-install dependencies on Linux
    _install_dependencies()

    parser = argparse.ArgumentParser(
        description="Google Nowcast Scraper - Two execution modes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
    # DevBox mode (scheduled with Azure upload, use default CSV)
    python %(prog)s --mode devbox
    
    # DevBox mode (with custom CSV file)
    python %(prog)s --mode devbox --csv_file Q:\\Code\\path\\to\\stations.csv

    # Main/Sub group mode (one-time execution)
    python %(prog)s --mode main_sub_group --main_group 1 --sub_group 2 \\
        --out_path ./output --station_list ./stations.csv
            """
        )
    
    parser.add_argument('--mode', type=str, required=True, 
                       choices=['devbox', 'main_sub_group'],
                       help='Execution mode: devbox (scheduled) or main_sub_group (one-time)')
    
    # Common arguments
    parser.add_argument('--csv_file', type=str,
                       help='Path to the station list CSV file [devbox and main_sub_group mode]')
    
    # Main/Sub group mode arguments
    parser.add_argument('--main_group', type=str,
                       help='主分组编号（1~7，基于CSV中的random_group列）[main_sub_group mode only]')
    parser.add_argument('--sub_group', type=str,
                       help='子分组编号（1~4），表示在主分组内的位置 [main_sub_group mode only]')
    parser.add_argument("--out_path", type=str,
                       help="Root output directory [main_sub_group mode only]")
    parser.add_argument("--station_list", type=str,
                       help="Path to the station list CSV file [main_sub_group mode only]")
    
    args = parser.parse_args()
    
    if args.mode == 'devbox':
        run_devbox_mode(csv_file=args.csv_file)
    elif args.mode == 'main_sub_group':
        # Validate required arguments for main_sub_group mode
        if not all([args.main_group, args.sub_group, args.out_path, args.station_list]):
            parser.error("--mode main_sub_group requires: --main_group, --sub_group, --out_path, --station_list")
        run_main_sub_group_mode(args)

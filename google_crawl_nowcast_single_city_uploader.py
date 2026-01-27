#!/usr/bin/env python3
"""Single-city Google nowcast scraper with immediate execution and Azure upload.

Run once for a single city, no scheduling. Scrapes data and uploads to Azure immediately.
Usage: python google_crawl_nowcast_single_city_uploader.py "City Name" CITY_ID
Example: python google_crawl_nowcast_single_city_uploader.py "Fairfax, California, United States" KFAX
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
import threading
import random
import uuid
import socket

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
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
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "无法获取"


def _get_public_ip():
    """获取公网 IP 地址（Google 看到的 IP）"""
    try:
        import requests
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
        try:
            import selenium
            import pandas
            from apscheduler.schedulers.background import BackgroundScheduler
        except ImportError:
            logging.info("⚠️  Warning: Some Python packages not installed. Please install manually:")
            logging.info("   pip install selenium webdriver-manager pandas apscheduler pytz")
        return
    
    logging.info("🔧 Checking and installing dependencies on Linux...")
    
    chrome_check = subprocess.run(
        ["which", "google-chrome"], 
        capture_output=True
    )
    
    if chrome_check.returncode != 0:
        logging.info("📦 Installing Google Chrome...")
        try:
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
        except subprocess.TimeoutExpired:
            logging.info(f"⚠️  Chrome installation timed out")
    else:
        logging.info("✅ Google Chrome already installed")
    
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


_install_dependencies()

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
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ]
    ua = random.choice(user_agents)
    options.add_argument(f"user-agent={ua}")

    import tempfile
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
    """爬取单个城市的气象数据"""
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
            file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
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
                if save_json:
                    folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                    file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                    outdir = base_dir / "Crawled" / folder_date
                    outdir.mkdir(parents=True, exist_ok=True)
                    fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                    fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                    logging.info(f"[{city}] Saved: {fname.name}")
                return out
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
                    if save_json:
                        folder_date = first_scrape_date if first_scrape_date else datetime.now(timezone.utc).strftime("%Y%m%d%H")
                        file_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                        outdir = base_dir / "Crawled" / folder_date
                        outdir.mkdir(parents=True, exist_ok=True)
                        fname = outdir / f"nowcast_{city_id}_{file_timestamp}.json"
                        fname.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                        logging.info(f"[{city}] Saved: {fname.name}")
                    return out
                else:
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


def upload_to_azure(base_dir, folder_date, container_prefix="GoogleNowcast", folder_suffix="_blob"):
    """上传爬取的数据到 Azure Blob 存储"""
    try:
        from azure_wrapper import get_wxforecasting_azure_wrapper
        
        logging.info(f"\n{'='*60}")
        logging.info("开始上传数据到 Azure Blob 存储...")
        logging.info(f"Azure 路径前缀: {container_prefix}")
        logging.info(f"{'='*60}\n")
        
        wrapper = get_wxforecasting_azure_wrapper()
        base_path = Path(base_dir)
        
        # 上传 Crawled 文件夹
        crawled_folder = base_path / "Crawled" / folder_date
        if crawled_folder.exists():
            azure_path = f"{container_prefix}/Crawled/{folder_date}{folder_suffix}"
            logging.info(f"📤 上传 Crawled 文件夹: {crawled_folder}")
            logging.info(f"   目标路径: {azure_path}")
            success, total = wrapper.upload_folder(
                local_folder=str(crawled_folder),
                container_prefix=azure_path,
                show_progress=True,
                filename_suffix=""
            )
            logging.info(f"✅ Crawled 上传完成: {success}/{total} 文件")
        else:
            logging.info(f"⚠️  Crawled 文件夹不存在: {crawled_folder}")
        
        # 上传 GoogleNowcastHTML 文件夹
        html_folder = base_path / "GoogleNowcastHTML" / folder_date
        if html_folder.exists():
            azure_path = f"{container_prefix}/GoogleNowcastHTML/{folder_date}{folder_suffix}"
            logging.info(f"\n📤 上传 GoogleNowcastHTML 文件夹: {html_folder}")
            logging.info(f"   目标路径: {azure_path}")
            success, total = wrapper.upload_folder(
                local_folder=str(html_folder),
                container_prefix=azure_path,
                show_progress=True,
                filename_suffix=""
            )
            logging.info(f"✅ GoogleNowcastHTML 上传完成: {success}/{total} 文件")
        else:
            logging.info(f"⚠️  GoogleNowcastHTML 文件夹不存在: {html_folder}")
        
        logging.info(f"\n{'='*60}")
        logging.info("✅ Azure 上传任务完成")
        logging.info(f"{'='*60}\n")
        
    except ImportError as e:
        logging.info(f"⚠️  Azure 上传跳过: 缺少依赖库 ({e})")
    except ValueError as e:
        logging.info(f"⚠️  Azure 上传跳过: {e}")
    except Exception as e:
        logging.info(f"❌ Azure 上传失败: {e}")


if __name__ == "__main__":
    import pytz

    # 解析命令行参数
    if len(sys.argv) < 3:
        print("Usage: python google_crawl_nowcast_single_city_uploader.py \"City Name\" CITY_ID [upload_to_azure]")
        print("Example: python google_crawl_nowcast_single_city_uploader.py \"Fairfax, California, United States\" KFAX")
        print("Example with Azure upload: python google_crawl_nowcast_single_city_uploader.py \"Fairfax, California, United States\" KFAX yes")
        sys.exit(1)
    
    CITY = sys.argv[1]
    CITY_ID = sys.argv[2]
    UPLOAD_TO_AZURE = len(sys.argv) > 3 and sys.argv[3].lower() in ['yes', 'true', '1']
    
    BASE_DIR = Path(__file__).parent
    AZURE_CONTAINER_PREFIX = "users/v-zhanghang/lib/data/GoogleNowcast"
    
    beijing_tz = pytz.timezone('Asia/Shanghai')
    start_time = datetime.now(timezone.utc)
    
    logging.info(f"\n{'='*60}")
    logging.info("✓ 单城市爬虫已启动（立即执行模式）")
    logging.info(f"✓ 城市: {CITY}")
    logging.info(f"✓ 城市代码: {CITY_ID}")
    logging.info(f"✓ 输出目录: {BASE_DIR}")
    logging.info(f"✓ 程序启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logging.info(f"✓ 当前北京时间: {datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"✓ 当前 UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"✓ 上传到 Azure: {'是' if UPLOAD_TO_AZURE else '否'}")
    logging.info(f"{'='*60}\n")
    
    # 立即执行爬取
    first_scrape_date = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    
    logging.info(f"开始爬取 {CITY} ({CITY_ID})...")
    result = scrape_nowcast_svg(CITY, city_id=CITY_ID, headless=True, save_json=True, output_dir=BASE_DIR, first_scrape_date=first_scrape_date)
    
    if result:
        if result.get("points"):
            logging.info(f"✓ 成功: 获取 {len(result['points'])} 个数据点")
        elif result.get("hourly_data"):
            logging.info(f"✓ 成功（小时预报）: 获取 {len(result['hourly_data'])} 项")
        elif result.get("fallback_data"):
            logging.info(f"✓ 成功（后备数据）")
        else:
            logging.info(f"⚠️  部分成功: {result.get('message', '未知')}")
        
        # 上传到 Azure（如果指定）
        if UPLOAD_TO_AZURE:
            logging.info("\n开始上传到 Azure...")
            upload_to_azure(BASE_DIR, first_scrape_date, AZURE_CONTAINER_PREFIX)
    else:
        logging.info(f"✗ 失败: 无法获取数据")
    
    logging.info(f"\n{'='*60}")
    logging.info("✓ 爬虫任务完成")
    logging.info(f"{'='*60}\n")

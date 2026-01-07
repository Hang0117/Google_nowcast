import json
import csv
from pathlib import Path
from datetime import datetime, timedelta
import re
import pytz

def load_timezone_mapping(csv_file='nowcast_crawl_list_v3.csv'):
    """Load city_id to timezone mapping from CSV"""
    tz_map = {}
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                city_id = row.get('id', '')
                tz_str = row.get('tz', '')
                if city_id and tz_str:
                    tz_map[city_id] = tz_str
    except Exception as e:
        print(f"Warning: Could not load timezone mapping: {e}")
    return tz_map

def parse_nowcast_data(json_file, tz_map=None):
    """Parse nowcast JSON file and extract data"""
    results = []
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    city = data.get('city', '')
    city_id = data.get('city_id', '')
    data_type = data.get('type')
    
    # Skip if type is null
    if data_type is None:
        return results
    
    # Get timezone for this city
    local_tz = None
    if tz_map and city_id in tz_map:
        try:
            local_tz = pytz.timezone(tz_map[city_id])
        except:
            pass
    
    # Extract and format scrape_time
    scrape_time_iso = data.get('scrape_time', '')
    try:
        scrape_dt = datetime.fromisoformat(scrape_time_iso.replace('Z', '+00:00'))
        # Convert to local time if timezone available
        if local_tz:
            scrape_dt_local = scrape_dt.astimezone(local_tz)
            scrape_time_formatted = scrape_dt_local.strftime('%Y-%m-%d %H:%M')
        else:
            scrape_time_formatted = scrape_dt.strftime('%Y-%m-%d %H:%M')
    except:
        scrape_time_formatted = ''
        scrape_dt = None
    
    # Extract date from scrape_time (yyyy-mm-dd)
    date_str = scrape_time_iso.split('T')[0] if scrape_time_iso else ''
    
    # Handle nowcast data
    if data_type == 'nowcast' and data.get('points'):
        for point in data['points']:
            minute_index = point.get('minute_index', 0)
            leadtime = minute_index * 2  # 0->0, 1->2, 2->4, 3->6, 4->8
            
            # Calculate valid_time from scrape_time + leadtime
            if scrape_dt:
                valid_dt = scrape_dt + timedelta(minutes=leadtime)
                # Align to even minute if odd
                if valid_dt.minute % 2 == 1:
                    valid_dt = valid_dt - timedelta(minutes=1)
                # Convert to local time if timezone available
                if local_tz:
                    valid_dt_local = valid_dt.astimezone(local_tz)
                    valid_time = valid_dt_local.strftime('%Y-%m-%d %H:%M')
                else:
                    valid_time = valid_dt.strftime('%Y-%m-%d %H:%M')
            else:
                # Fallback: try to get time from point data
                valid_time = point.get('time', '')
            
            # Parse height to determine precipitation
            height_str = point.get('height', '0')
            try:
                height_val = float(height_str)
                precip = 1 if height_val > 0 else 0
            except:
                precip = 0
            
            results.append({
                'city': city,
                'city_id': city_id,
                'type': 'nowcast',
                'scrape_time': scrape_time_formatted,
                'valid_time': valid_time,
                'leadtime': leadtime,
                'precip': precip
            })

    # Handle fallback_data (text-based nowcast)
    elif data_type == 'nowcast' and data.get('fallback_data'):
        fallback = data.get('fallback_data', {})
        div1_text = fallback.get('div1_text', '')
        div2_text = fallback.get('div2_text', '')
        combined_text = f"{div1_text} {div2_text}".lower()

        # Only process if text hints precipitation
        precip_keywords = ['rain', 'shower', 'thunderstorm', 'drizzle', 'precipitation', 'wet', 'sleet', 'snow']
        has_precip = any(keyword in combined_text for keyword in precip_keywords)

        if has_precip and scrape_dt:
            # Parse all time ranges like "from 7:00 AM to 9:30 AM" or "from 10:48 AM continuing beyond 2:00 PM"
            time_range_pattern = r'from\s+(\d{1,2}):(\d{2})\s*(AM|PM)(?:\s+(?:to|continuing beyond)\s+(\d{1,2}):(\d{2})\s*(AM|PM))?'
            ranges = list(re.finditer(time_range_pattern, div2_text, re.IGNORECASE))

            precip_periods = []
            for m in ranges:
                groups = m.groups()
                sh, sm, sap = int(groups[0]), int(groups[1]), groups[2]
                
                # Handle optional end time (may be None if only "continuing beyond" without time)
                if groups[3] is not None:
                    eh, em, eap = int(groups[3]), int(groups[4]), groups[5]
                else:
                    # If no end time specified, use scrape_time as end (meaning it continues beyond known time)
                    eh, em, eap = None, None, None

                # 12 AM/PM handling
                if sap.upper() == 'PM' and sh != 12:
                    sh += 12
                if sap.upper() == 'AM' and sh == 12:
                    sh = 0
                if eap and eap.upper() == 'PM' and eh != 12:
                    eh += 12
                if eap and eap.upper() == 'AM' and eh == 12:
                    eh = 0

                # Build start/end in local tz if available; otherwise UTC
                if local_tz:
                    scrape_local = scrape_dt.astimezone(local_tz)
                    start_local = scrape_local.replace(hour=sh, minute=sm, second=0, microsecond=0)
                    
                    # If no end time, extend to 6 hours after start (reasonable forecast range)
                    if eh is None:
                        end_local = start_local + timedelta(hours=6)
                    else:
                        end_local = scrape_local.replace(hour=eh, minute=em, second=0, microsecond=0)

                    if start_local < scrape_local:
                        start_local += timedelta(days=1)
                    if end_local < scrape_local:
                        end_local += timedelta(days=1)
                    if end_local < start_local:
                        end_local += timedelta(days=1)

                    start_dt = start_local.astimezone(pytz.UTC)
                    end_dt = end_local.astimezone(pytz.UTC)
                else:
                    start_dt = scrape_dt.replace(hour=sh, minute=sm, second=0, microsecond=0)
                    
                    # If no end time, extend to 6 hours after start
                    if eh is None:
                        end_dt = start_dt + timedelta(hours=6)
                    else:
                        end_dt = scrape_dt.replace(hour=eh, minute=em, second=0, microsecond=0)
                    if start_dt < scrape_dt:
                        start_dt += timedelta(days=1)
                    if end_dt < scrape_dt:
                        end_dt += timedelta(days=1)
                    if end_dt < start_dt:
                        end_dt += timedelta(days=1)

                precip_periods.append((start_dt, end_dt))

            if precip_periods:
                max_end = max(p[1] for p in precip_periods)
                current_dt = scrape_dt
                while current_dt <= max_end:
                    leadtime = int((current_dt - scrape_dt).total_seconds() / 60)
                    precip_val = 0
                    for s, e in precip_periods:
                        if s <= current_dt <= e:
                            precip_val = 1
                            break

                    if local_tz:
                        valid_dt_local = current_dt.astimezone(local_tz)
                        # Align to even minute if odd
                        if valid_dt_local.minute % 2 == 1:
                            valid_dt_local = valid_dt_local - timedelta(minutes=1)
                        valid_time = valid_dt_local.strftime('%Y-%m-%d %H:%M')
                    else:
                        valid_dt = current_dt
                        if valid_dt.minute % 2 == 1:
                            valid_dt = valid_dt - timedelta(minutes=1)
                        valid_time = valid_dt.strftime('%Y-%m-%d %H:%M')

                    results.append({
                        'city': city,
                        'city_id': city_id,
                        'type': 'nowcast',
                        'scrape_time': scrape_time_formatted,
                        'valid_time': valid_time,
                        'leadtime': leadtime,
                        'precip': precip_val
                    })

                    current_dt += timedelta(minutes=2)
            else:
                # No explicit time range, record a single precipitation flag
                results.append({
                    'city': city,
                    'city_id': city_id,
                    'type': 'nowcast',
                    'scrape_time': scrape_time_formatted,
                    'valid_time': scrape_time_formatted,
                    'leadtime': 0,
                    'precip': 1
                })
    
    # Handle hourly data
    elif data_type == 'hourly' and data.get('hourly_data'):
        # Parse hourly_data to extract leadtime
        # Format: "Now,64°F,Cloudy" or "6 PM,63°F,Cloudy"
        
        for idx, hourly_str in enumerate(data['hourly_data']):
            # Parse time from hourly string
            parts = hourly_str.split(',')
            if not parts:
                continue
            
            weather_desc = parts[2].strip() if len(parts) > 2 else ''
            
            # Calculate leadtime from index
            # First item (idx=0) is "Now" = 0 hours lead
            # Each subsequent item is +1 hour
            leadtime_hours = idx
            
            # Calculate valid_time from scrape_time + leadtime
            # Align to the top of the hour
            if scrape_dt:
                valid_dt = scrape_dt + timedelta(hours=leadtime_hours)
                # Align to the top of the hour (minute=0, second=0)
                valid_dt = valid_dt.replace(minute=0, second=0, microsecond=0)
                # Convert to local time if timezone available
                if local_tz:
                    valid_dt_local = valid_dt.astimezone(local_tz)
                    # Re-align to hour after timezone conversion
                    valid_dt_local = valid_dt_local.replace(minute=0, second=0, microsecond=0)
                    valid_time = valid_dt_local.strftime('%Y-%m-%d %H:%M')
                else:
                    valid_time = valid_dt.strftime('%Y-%m-%d %H:%M')
            else:
                valid_time = ''
            
            # Determine precipitation from weather description
            precip_keywords = ['rain', 'shower', 'thunderstorm', 'drizzle', 'precipitation', 'wet', 'sleet', 'snow']
            precip = 1 if any(keyword in weather_desc.lower() for keyword in precip_keywords) else 0
            
            results.append({
                'city': city,
                'city_id': city_id,
                'type': 'hourly',
                'scrape_time': scrape_time_formatted,
                'valid_time': valid_time,
                'leadtime': leadtime_hours,  # Keep in hours
                'precip': precip
            })
    
    # Handle no data case
    elif data.get('message') == 'no nowcast data now.':
        # Skip records with no data
        pass
    
    return results

def parse_all_jsons_to_csv(input_dir, output_file):
    """Parse all JSON files in a directory and save to CSV"""
    input_path = Path(input_dir)
    all_results = []
    
    # Load timezone mapping
    base_dir = Path(__file__).parent
    tz_csv = base_dir / 'nowcast_crawl_list_v3.csv'
    tz_map = load_timezone_mapping(tz_csv)
    print(f"Loaded timezone mapping for {len(tz_map)} cities")
    
    # Find all nowcast JSON files
    json_files = sorted(input_path.glob('nowcast_*.json'))
    
    print(f"Found {len(json_files)} JSON files in {input_dir}")
    
    for json_file in json_files:
        try:
            results = parse_nowcast_data(json_file, tz_map)
            all_results.extend(results)
            print(f"✓ Parsed {json_file.name} - {len(results)} records")
        except Exception as e:
            print(f"✗ Error parsing {json_file.name}: {e}")
    
    # Write to CSV
    if all_results:
        csv_path = Path(output_file)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        fieldnames = ['city', 'city_id', 'type', 'scrape_time', 'valid_time', 'leadtime', 'precip']
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)
        
        print(f"\n✓ Saved {len(all_results)} records to {output_file}")
    else:
        print("No data found to save")

if __name__ == "__main__":
    # Parse the latest date folder (20251231)
    import glob
    import os
    
    # Find the latest date folder
    base_dir = Path(__file__).parent
    input_dir= base_dir/"Crawled"
    output_dir = base_dir / "GoogleNowcastParsed"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    date_folders = sorted([d for d in input_dir.glob('[0-9]*') if d.is_dir()])
    # date_folders = [Path('2026010316')]
    if date_folders:
        latest_folder = date_folders[-1]
        print(f"Processing latest folder: {latest_folder.name}")
        
        output_csv = output_dir / f"nowcast_data_{latest_folder.name}.csv"
        parse_all_jsons_to_csv(latest_folder, output_csv)
    else:
        print("No date folders found")

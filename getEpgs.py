import os
import gzip
import re
import xml.etree.ElementTree as ET
import requests
from datetime import datetime, timedelta
import time
from io import BytesIO

days_future = 0
days_past = 1
skip_dummy_programs = False

max_retries = 5
retry_delay = 5

tvg_ids_file = os.path.join(os.path.dirname(__file__), 'tvg-ids.txt')
output_file_gz = os.path.join(os.path.dirname(__file__), 'epg.xml.gz')

def fix_xml_issues(xml_content):
    """Fix common XML encoding and formatting issues for player compatibility"""
    xml_content = xml_content.replace('&amp;amp;', '&amp;')
    xml_content = re.sub(r'</programme>\s*<programme', '</programme>\n<programme', xml_content)
    xml_content = re.sub(r'[^\x20-\x7E\n\r\t]', '', xml_content)
    return xml_content

def parse_xmltv_time(time_str):
    """Parse XMLTV time format (YYYYMMDDHHmmss +TZOFFSET) to datetime"""
    if not time_str:
        return None
    try:
        date_part = time_str[:14]
        return datetime.strptime(date_part, '%Y%m%d%H%M%S')
    except:
        return None

def is_programme_too_far_future(start_time_str, days_limit):
    """Check if a programme starts more than X days in the future"""
    if not days_limit or days_limit <= 0:
        return False
    
    start_time = parse_xmltv_time(start_time_str)
    if not start_time:
        return False
    
    future_cutoff = datetime.now() + timedelta(days=days_limit)
    return start_time > future_cutoff

def is_programme_too_far_past(stop_time_str, days_limit):
    """Check if a programme ended more than X days ago"""
    if not days_limit or days_limit <= 0:
        return False
    
    stop_time = parse_xmltv_time(stop_time_str)
    if not stop_time:
        return False
    
    past_cutoff = datetime.now() - timedelta(days=days_limit)
    return stop_time < past_cutoff

def is_dummy_programme(tvg_id):
    """Check if programme has 'dummy' in its tvg-id (case insensitive)"""
    if not skip_dummy_programs:
        return False
    
    if not tvg_id:
        return False
    
    return 'dummy' in tvg_id.lower()

def fetch_with_retry(url, timeout=30):
    """Fetch URL with retry logic"""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"  Fetching {url}... (attempt {attempt}/{max_retries})")
            response = requests.get(url, timeout=timeout, stream=True)
            response.raw.decode_content = True  # Handle gzip content-encoding
            
            if response.status_code == 200:
                return response
            else:
                print(f"  Failed with status code {response.status_code}")
                response.close()
                
        except requests.exceptions.Timeout:
            print(f"  Timeout error on attempt {attempt}")
        except requests.exceptions.ConnectionError:
            print(f"  Connection error on attempt {attempt}")
        except requests.exceptions.RequestException as e:
            print(f"  Request error on attempt {attempt}: {e}")
        
        if attempt < max_retries:
            print(f"  Waiting {retry_delay} seconds before retry...")
            time.sleep(retry_delay)
    
    print(f"  Failed to fetch after {max_retries} attempts")
    return None

def extract_tvg_ids_from_playlist(url):
    """Extract all tvg-id values from an M3U playlist"""
    tvg_ids = set()
    
    response = fetch_with_retry(url)
    if response is None:
        print(f"Failed to fetch playlist {url}")
        return tvg_ids
    
    try:
        for line in response.iter_lines(decode_unicode=True):
            if line:
                matches = re.findall(r'tvg-id="([^"]+)"', line, re.IGNORECASE)
                tvg_ids.update(matches)
        
        print(f"Extracted {len(tvg_ids)} tvg-ids from {url}")
        
    except Exception as e:
        print(f"Error processing playlist {url}: {e}")
    
    return tvg_ids

def get_valid_tvg_ids(playlist_urls):
    """Get tvg-ids from both file AND playlist URLs"""
    valid_tvg_ids = set()
    
    if os.path.exists(tvg_ids_file):
        try:
            with open(tvg_ids_file, 'r', encoding='utf-8') as file:
                file_ids = set(line.strip() for line in file if line.strip())
                valid_tvg_ids.update(file_ids)
                if file_ids:
                    print(f"Loaded {len(file_ids)} tvg-ids from {tvg_ids_file}")
        except Exception as e:
            print(f"Error reading {tvg_ids_file}: {e}")
    
    if playlist_urls:
        print("Extracting tvg-ids from playlists...")
        for playlist_url in playlist_urls:
            playlist_ids = extract_tvg_ids_from_playlist(playlist_url)
            valid_tvg_ids.update(playlist_ids)
    
    if valid_tvg_ids:
        print(f"Total unique tvg-ids: {len(valid_tvg_ids)}")
    else:
        print("Warning: No tvg-ids found from file or playlists. EPG will contain all channels.")
    
    return valid_tvg_ids

def write_xml_header(f):
    """Write XML header to file"""
    f.write('<?xml version="1.0" encoding="utf-8"?>\n')
    f.write('<tv>\n')

def write_xml_footer(f):
    """Write XML footer to file"""
    f.write('</tv>\n')

def stream_parse_epg(file_obj, valid_tvg_ids, output_handle, seen_channels, seen_programmes, stats):
    """Memory-efficient streaming XML parser"""
    
    channels_added = 0
    channels_filtered = 0
    programmes_added = 0
    programmes_filtered_tvg = 0
    programmes_skipped_future = 0
    programmes_skipped_past = 0
    programmes_skipped_dummy = 0
    
    prog_count = 0
    
    try:
        # Use iterparse for streaming - only keeps current element in memory
        for event, elem in ET.iterparse(file_obj, events=('end',)):
            
            if elem.tag == 'channel':
                tvg_id = elem.get('id')
                stats['total_channels_in_sources'] += 1
                
                if valid_tvg_ids and tvg_id not in valid_tvg_ids:
                    channels_filtered += 1
                elif tvg_id not in seen_channels:
                    seen_channels.add(tvg_id)
                    channel_str = ET.tostring(elem, encoding='unicode')
                    output_handle.write('  ' + channel_str + '\n')
                    channels_added += 1
                
                # Clear element from memory immediately
                elem.clear()
                
            elif elem.tag == 'programme':
                prog_count += 1
                if prog_count % 50000 == 0:
                    print(f"    Processed {prog_count} programmes...")
                
                tvg_id = elem.get('channel')
                start_time = elem.get('start')
                stop_time = elem.get('stop')
                
                stats['total_programmes_in_sources'] += 1
                
                # Apply filters
                if valid_tvg_ids and tvg_id not in valid_tvg_ids:
                    programmes_filtered_tvg += 1
                elif is_dummy_programme(tvg_id):
                    programmes_skipped_dummy += 1
                elif is_programme_too_far_future(start_time, days_future):
                    programmes_skipped_future += 1
                elif is_programme_too_far_past(stop_time, days_past):
                    programmes_skipped_past += 1
                else:
                    prog_key = f"{tvg_id}_{start_time}_{stop_time}"
                    if prog_key not in seen_programmes:
                        seen_programmes.add(prog_key)
                        programme_str = ET.tostring(elem, encoding='unicode')
                        output_handle.write('  ' + programme_str + '\n')
                        programmes_added += 1
                
                # Clear element from memory immediately
                elem.clear()
    
    except ET.ParseError as e:
        print(f"  XML parsing error (continuing): {e}")
    
    # Update stats
    stats['channels_filtered_by_tvg_id'] += channels_filtered
    stats['programmes_filtered_by_tvg_id'] += programmes_filtered_tvg
    stats['programmes_filtered_by_future'] += programmes_skipped_future
    stats['programmes_filtered_by_past'] += programmes_skipped_past
    stats['programmes_filtered_by_dummy'] += programmes_skipped_dummy
    
    print(f"  Added {channels_added} channels and {programmes_added} programmes")
    if channels_filtered > 0:
        print(f"  Filtered {channels_filtered} channels not in tvg-id list")
    if programmes_filtered_tvg > 0:
        print(f"  Filtered {programmes_filtered_tvg} programmes (channel not in tvg-id list)")
    if programmes_skipped_dummy > 0:
        print(f"  Skipped {programmes_skipped_dummy} dummy programmes")
    if programmes_skipped_future > 0:
        print(f"  Skipped {programmes_skipped_future} programmes beyond {days_future} days in the future")
    if programmes_skipped_past > 0:
        print(f"  Skipped {programmes_skipped_past} programmes older than {days_past} days")

def process_epg_source(url, valid_tvg_ids, output_handle, seen_channels, seen_programmes, stats):
    """Process a single EPG source with streaming"""
    print(f"Processing {url}...")
    
    response = fetch_with_retry(url, timeout=60)
    if response is None:
        print(f"Skipping {url} due to fetch failure")
        return
    
    try:
        if url.endswith('.gz'):
            print(f"  Streaming and decompressing...")
            # Stream decompress directly from response - NEVER loads full file into memory
            decompressor = gzip.GzipFile(fileobj=response.raw)
            stream_parse_epg(decompressor, valid_tvg_ids, output_handle, seen_channels, seen_programmes, stats)
        else:
            print(f"  Streaming XML...")
            # Stream directly from response
            stream_parse_epg(response.raw, valid_tvg_ids, output_handle, seen_channels, seen_programmes, stats)
        
    except Exception as e:
        print(f"Failed to process XML from {url}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Always close the response to free up the connection
        response.close()

def filter_and_build_epg(epg_urls, playlist_urls=None):
    valid_tvg_ids = get_valid_tvg_ids(playlist_urls or [])
    
    seen_channels = set()
    seen_programmes = set()
    
    stats = {
        'total_channels_in_sources': 0,
        'total_programmes_in_sources': 0,
        'channels_filtered_by_tvg_id': 0,
        'programmes_filtered_by_tvg_id': 0,
        'programmes_filtered_by_future': 0,
        'programmes_filtered_by_past': 0,
        'programmes_filtered_by_dummy': 0
    }
    
    print("\nBuilding compressed EPG file...")
    filters = []
    if days_future and days_future > 0:
        filters.append(f"programmes up to {days_future} days in the future")
    if days_past and days_past > 0:
        filters.append(f"programmes from last {days_past} days")
    if skip_dummy_programs:
        filters.append("excluding dummy programmes")
    
    if filters:
        print(f"Filtering: {', '.join(filters)}\n")
    
    with gzip.open(output_file_gz, 'wt', encoding='utf-8') as f:
        write_xml_header(f)
        
        for url in epg_urls:
            process_epg_source(url, valid_tvg_ids, f, seen_channels, seen_programmes, stats)
        
        write_xml_footer(f)
    
    print(f"\nCompressed EPG saved to {output_file_gz}")
    print(f"Total: {len(seen_channels)} unique channels, {len(seen_programmes)} unique programmes")
    
    print("\n" + "="*70)
    print("FILTERING STATISTICS")
    print("="*70)
    print(f"Total channels in sources:          {stats['total_channels_in_sources']}")
    print(f"Channels in output:                 {len(seen_channels)}")
    print(f"Channels filtered (not in tvg-id):  {stats['channels_filtered_by_tvg_id']}")
    print()
    print(f"Total programmes in sources:        {stats['total_programmes_in_sources']}")
    print(f"Programmes in output:               {len(seen_programmes)}")
    
    total_filtered = (stats['programmes_filtered_by_tvg_id'] + 
                     stats['programmes_filtered_by_future'] + 
                     stats['programmes_filtered_by_past'] + 
                     stats['programmes_filtered_by_dummy'])
    
    print(f"Programmes filtered (total):        {total_filtered}")
    if stats['programmes_filtered_by_tvg_id'] > 0:
        print(f"  - Not in tvg-id list:             {stats['programmes_filtered_by_tvg_id']}")
    if stats['programmes_filtered_by_future'] > 0:
        print(f"  - Too far in future:              {stats['programmes_filtered_by_future']}")
    if stats['programmes_filtered_by_past'] > 0:
        print(f"  - Too old:                        {stats['programmes_filtered_by_past']}")
    if stats['programmes_filtered_by_dummy'] > 0:
        print(f"  - Dummy programmes:               {stats['programmes_filtered_by_dummy']}")
    
    if stats['total_programmes_in_sources'] > 0:
        percentage_filtered = (total_filtered / stats['total_programmes_in_sources']) * 100
        print(f"\nFiltering reduced EPG size by:      {percentage_filtered:.1f}%")
    
    print("="*70)

# EPG source URLs
epg_urls = [           
	'https://raw.githubusercontent.com/matthuisman/i.mjh.nz/refs/heads/master/Plex/all.xml.gz',
    'https://raw.githubusercontent.com/matthuisman/i.mjh.nz/refs/heads/master/PlutoTV/all.xml.gz',
    'https://raw.githubusercontent.com/matthuisman/i.mjh.nz/refs/heads/master/SamsungTVPlus/all.xml.gz',
    'https://raw.githubusercontent.com/BuddyChewChew/localnow-playlist-generator/refs/heads/main/epg.xml',
    'https://raw.githubusercontent.com/matthuisman/i.mjh.nz/refs/heads/master/Roku/all.xml.gz',
    'https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/refs/heads/main/playlists/xumo_epg.xml.gz',
    'https://animenosekai.github.io/japanterebi-xmltv/guide.xml',
    'https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz',
	'https://tvpass.org/epg.xml',
    'http://drewlive24.duckdns.org:8081/DrewLive.xml.gz'
]

playlist_urls = [
    'https://github.com/Drewski2423/DrewLive/raw/refs/heads/main/MergedPlaylist.m3u8'
]

if __name__ == "__main__":
    filter_and_build_epg(epg_urls, playlist_urls)






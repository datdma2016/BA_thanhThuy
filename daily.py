import gspread
import requests
import json
import traceback
import shlex
import time
import random
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime, timedelta

app = Flask(__name__)

# ======================================================
# 1. CẤU HÌNH
# ======================================================

FB_ACCESS_TOKEN = "EAANPvsZANh38BQt8Bcqztr63LDZBieQxO2h5TnOIGpHQtlOnV85cwg7I2ZCVf8vFTccpbB7hX97HYOsGFEKLD3fSZC2BCyKWeZA0vsUJZCXBZAMVZARMwZCvTuPGTsIStG5ro10ltZBXs3yTOzBLjZAjfL8TAeXwgKC73ZBZA3aQD6eludndMkOYFrVCFv2CrIrNe5nX82FScL0TzIXjA7qUl9HZAz" 
FILE_SHEET_GOC = "BA_ads_daily_20260120" 
BATCH_SIZE = 50 
SLEEP_TIME = 10 # <--- NGỦ 10 GIÂY SAU MỖI LẦN GHI (CHÌA KHÓA THÀNH CÔNG)

DANH_SACH_TKQC = [
    {"id": "581662847745376", "name": "tick_xanh_001"}, 
    {"id": "1934563933738877", "name": "Juul_001"},
    {"id": "995686602001085", "name": "Juul_004"},
    {"id": "689891917184988", "name": "116_1"},
    {"id": "2228203290991345", "name": "116_2"},
    {"id": "1369968844500935", "name": "116_3"},
    {"id": "828619376785021", "name": "116_4"},
    {"id": "757870177275480", "name": "116_5"}
]

# ======================================================
# CSS GIAO DIỆN
# ======================================================
CSS_STYLE = """
<style>
    body { background-color: #0d1117; color: #c9d1d9; font-family: 'Consolas', monospace; padding: 20px; font-size: 13px; }
    .log { margin-bottom: 4px; border-bottom: 1px dashed #21262d; padding: 2px 0; }
    .success { color: #3fb950; }
    .error { color: #f85149; }
    .warning { color: #d29922; }
    .info { color: #8b949e; }
    .debug { color: #79c0ff; font-weight: bold; }
    .progress { color: #e3b341; font-weight: bold; }
    .sleep { color: #d2a8ff; font-style: italic; }
    .highlight { color: #58a6ff; font-weight: bold; }
    .heartbeat { color: #8b949e; font-size: 10px; }
    table { width: 100%; border-collapse: collapse; margin-top: 15px; background: #161b22; font-size: 12px; }
    th, td { border: 1px solid #30363d; padding: 8px; text-align: right; }
    th { background-color: #21262d; text-align: center; color: #f0f6fc; }
    td:first-child { text-align: left; }
    h2 { color: #f0f6fc; margin-top: 30px; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
</style>
"""

# ======================================================
# HÀM BỔ TRỢ
# ======================================================
def fmt_vn(value):
    if not value: return "0"
    try:
        return "{:,.0f}".format(value).replace(",", ".")
    except:
        return str(value)

def get_fb_value(data_list, keys_target, value_key='value'):
    if not data_list: return 0
    for k in keys_target:
        for item in data_list:
            if item.get('action_type') == k:
                return float(item.get(value_key, 0))
    return 0

def check_keyword_v12(ten_camp, keyword_string):
    if not keyword_string: return True
    ten_camp_lower = ten_camp.lower()
    or_groups = keyword_string.split(',') 
    for group in or_groups:
        try:
            terms = shlex.split(group)
        except:
            terms = group.split()
        match_group = True
        for term in terms:
            is_negative = term.startswith('-')
            tu_khoa_chuan = term[1:] if is_negative else term
            tu_khoa_chuan = tu_khoa_chuan.lower()
            tim_thay = tu_khoa_chuan in ten_camp_lower
            if is_negative:
                if tim_thay:
                    match_group = False
                    break
            else:
                if not tim_thay:
                    match_group = False
                    break
        if match_group: return True 
    return False 

# --- HÀM GHI SHEET "CON RÙA" (SIÊU CHẬM) ---
def safe_write_sheet(worksheet, rows):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            worksheet.append_rows(rows)
            return True, None
        except Exception as e:
            time.sleep(10) # Lỗi thì nghỉ 10s rồi thử lại
            if attempt == max_retries - 1:
                return False, str(e)
    return False, "Unknown Error"

@app.route('/')
def home():
    return f"""
    <h1>Bot V29: Turtle Mode (Super Stable)</h1>
    <ul>
        <li><a href='/fb-ads'>/fb-ads</a>: Báo cáo Tổng Hợp</li>
        <li><a href='/fb-daily'>/fb-daily</a>: Báo cáo Theo Ngày</li>
    </ul>
    """

@app.route('/fb-ads')
def lay_data_tong_hop():
    def generate():
        yield CSS_STYLE
        yield "<h3>> KHỞI ĐỘNG BÁO CÁO TỔNG HỢP...</h3>"
        try:
            keyword = request.args.get('keyword', '')
            ten_tab = request.args.get('sheet', 'BaoCaoTongHop')
            start_date = request.args.get('start')
            end_date = request.args.get('end')
            date_preset = request.args.get('date', 'today')
            yield f"<div class='log info'>[INIT] File='{FILE_SHEET_GOC}' | Tab='{ten_tab}' | Mode=Total</div>"
            yield from core_process(keyword, ten_tab, start_date, end_date, date_preset, mode='total')
        except Exception as e:
             yield f"<div class='log error'>[CRASH] {traceback.format_exc()}</div>"
    return Response(stream_with_context(generate()))

@app.route('/fb-daily')
def lay_data_hang_ngay():
    def generate():
        yield CSS_STYLE
        yield "<h3>> KHỞI ĐỘNG BÁO CÁO CHI TIẾT THEO NGÀY...</h3>"
        try:
            keyword = request.args.get('keyword', '')
            ten_tab = request.args.get('sheet', 'BaoCaoTungNgay')
            start_date = request.args.get('start')
            end_date = request.args.get('end')
            date_preset = request.args.get('date', 'today')
            yield f"<div class='log info'>[INIT] File='{FILE_SHEET_GOC}' | Tab='{ten_tab}' | Mode=Daily</div>"
            yield from core_process(keyword, ten_tab, start_date, end_date, date_preset, mode='daily')
        except Exception as e:
             yield f"<div class='log error'>[CRASH] {traceback.format_exc()}</div>"
    return Response(stream_with_context(generate()))

# ======================================================
# CORE PROCESS
# ======================================================
def core_process(keyword, ten_tab, start_date, end_date, date_preset, mode='total'):
    if start_date and end_date:
        range_dict = {'since': start_date, 'until': end_date}
        time_str = f'insights.time_range({json.dumps(range_dict)})'
        thoi_gian_bao_cao = f"{start_date} đến {end_date}"
    else:
        time_str = f'insights.date_preset({date_preset})'
        thoi_gian_bao_cao = date_preset

    if mode == 'daily':
        time_str += ".time_increment(1)"
    
    yield f"<div class='log info'>[SHEET] Connecting to '{FILE_SHEET_GOC}'...</div>"
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    
    try:
        sh = client.open(FILE_SHEET_GOC) 
        yield f"<div class='log success'>[SHEET] Connected successfully!</div>"
    except Exception as e:
        yield f"<div class='log error'>[SHEET ERROR] Không mở được file. Lỗi: {str(e)}</div>"
        return

    BASE_HEADERS = [
        "ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", 
        "Tiền tiêu", "Reach", "Data", "Giá Data", "Doanh Thu", "ROAS",
        "Lượt mua", "AOV", "Rev/Data", 
        "ThruPlay", "View 25%", "View 100%", "Từ khóa (Tag)"
    ]
    if mode == 'daily':
        HEADERS = ["Ngày"] + BASE_HEADERS
    else:
        HEADERS = BASE_HEADERS.copy()
        HEADERS.insert(4, "Thời gian")

    try:
        worksheet = sh.worksheet(ten_tab)
        yield f"<div class='log success'>[SHEET] Found tab '{ten_tab}'.</div>"
    except:
        yield f"<div class='log warning'>[SHEET] Creating new tab...</div>"
        worksheet = sh.add_worksheet(title=ten_tab, rows=100, cols=20)
        worksheet.append_row(HEADERS)

    KEYWORD_GROUPS = [k.strip() for k in keyword.split(',') if k.strip()]
    fields_video = "video_p25_watched_actions,video_p100_watched_actions,video_thruplay_watched_actions"
    fields_list = f'name,status,{time_str}{{date_start,spend,reach,actions,action_values,purchase_roas,{fields_video}}}'

    for i, tk_obj in enumerate(DANH_SACH_TKQC):
        BUFFER_ROWS = [] 
        if i > 0: 
            sleep_time = random.uniform(2, 3)
            yield f"<div class='log sleep'>[SLEEP] Nghỉ {sleep_time:.1f}s...</div>"
            yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"
            time.sleep(sleep_time)

        id_tk = tk_obj['id']
        ten_tk = tk_obj['name']
        yield f"<div class='log info'>[SCAN] Scanning <b>{ten_tk}</b>...</div>"

        base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
        params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 50} 
        
        all_campaigns = []
        next_url = base_url
        
        while True:
            try:
                yield "<span class='heartbeat'>.</span>" 
                yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"
                res = requests.get(next_url, params=params if next_url == base_url else None)
                data = res.json()
                if 'error' in data:
                    yield f"<div class='log error'>[ERROR] {data['error']['message']}</div>"
                    break
                all_campaigns.extend(data.get('data', []))
                if 'paging' in data and 'next' in data['paging']:
                    next_url = data['paging']['next']
                else: break
            except: break

        total_camp = len(all_campaigns)
        yield f"<div class='log debug'>[DEBUG] {ten_tk}: Found {total_camp} campaigns. Processing...</div>"

        processed_count = 0 
        
        for camp in all_campaigns:
            processed_count += 1
            if processed_count % 50 == 0:
                yield f"<div class='log progress'>[TIẾN ĐỘ] Đã xử lý {processed_count}/{total_camp} chiến dịch...</div>"
                yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"

            ten_camp = camp.get('name', 'Không tên')
            trang_thai = camp.get('status', 'UNKNOWN')
            
            if check_keyword_v12(ten_camp, keyword):
                insights_data = camp.get('insights', {}).get('data', [])
                if insights_data:
                    for stat in insights_data:
                        spend = float(stat.get('spend', 0))
                        if spend > 0:
                            reach = int(stat.get('reach', 0))
                            actions = stat.get('actions', [])
                            action_values = stat.get('action_values', [])
                            # ... (Giữ nguyên logic lấy data) ...
                            cmts = get_fb_value(actions, ['comment'])
                            msgs = get_fb_value(actions, ['onsite_conversion.messaging_conversation_started_7d', 'messaging_conversation_started_7d'])
                            total_data = cmts + msgs
                            revenue = get_fb_value(action_values, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                            orders = get_fb_value(actions, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                            thruplay = get_fb_value(actions, ['video_thruplay_watched_actions'])
                            if thruplay == 0: thruplay = get_fb_value(stat.get('video_thruplay_watched_actions', []), ['video_view', 'video_play'])
                            view25 = get_fb_value(actions, ['video_p25_watched_actions'])
                            if view25 == 0: view25 = get_fb_value(stat.get('video_p25_watched_actions', []), ['video_view', 'video_play'])
                            view100 = get_fb_value(actions, ['video_p100_watched_actions'])
                            if view100 == 0: view100 = get_fb_value(stat.get('video_p100_watched_actions', []), ['video_view', 'video_play'])
                            gia_data = round(spend / total_data) if total_data > 0 else 0
                            roas = (revenue / spend) if spend > 0 else 0
                            aov = round(revenue / orders) if orders > 0 else 0
                            rev_per_data = round(revenue / total_data) if total_data > 0 else 0
                            
                            matched_tag = "Other"
                            if len(KEYWORD_GROUPS) > 0:
                                for kw_group in KEYWORD_GROUPS:
                                    if check_keyword_v12(ten_camp, kw_group):
                                        matched_tag = kw_group
                                        break
                            else: matched_tag = "All"

                            if mode == 'daily':
                                ngay_bao_cao = stat.get('date_start', 'Unknown')
                                row = [ngay_bao_cao, id_tk, ten_tk, ten_camp, trang_thai, spend, reach, total_data, gia_data, revenue, roas, orders, aov, rev_per_data, thruplay, view25, view100, matched_tag]
                            else:
                                row = [id_tk, ten_tk, ten_camp, trang_thai, thoi_gian_bao_cao, spend, reach, total_data, gia_data, revenue, roas, orders, aov, rev_per_data, thruplay, view25, view100, matched_tag]
                            
                            BUFFER_ROWS.append(row)

                            # --- GHI SHEET & NGỦ KHÒ ---
                            if len(BUFFER_ROWS) >= BATCH_SIZE:
                                yield f"<span class='heartbeat'>Writing batch ({len(BUFFER_ROWS)})...</span> "
                                yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"
                                success, err = safe_write_sheet(worksheet, BUFFER_ROWS)
                                if success:
                                    BUFFER_ROWS = []
                                    # CHỖ NÀY QUAN TRỌNG: NGỦ 10 GIÂY
                                    yield f"<div class='log sleep'>[COOL DOWN] Sleeping {SLEEP_TIME}s to please Google...</div>"
                                    yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"
                                    time.sleep(SLEEP_TIME) 
                                else:
                                    yield f"<div class='log error'>[WRITE FAIL] {err}</div>"

        # Ghi nốt phần còn lại
        if BUFFER_ROWS:
            success, err = safe_write_sheet(worksheet, BUFFER_ROWS)
            if success:
                yield f"<div class='log success'>[SAVED] Saved final batch.</div>"
                yield f"<div class='log sleep'>[COOL DOWN] Sleeping {SLEEP_TIME}s...</div>"
                time.sleep(SLEEP_TIME)
            else:
                yield f"<div class='log error'>[WRITE FAIL] {err}</div>"
        
        yield f"<div class='log info'>[DONE] {ten_tk}: Finished.</div>"
        yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"

    yield f"<div class='log success' style='margin-top:20px; border-top:1px solid #30363d; padding-top:10px;'>✅ ĐÃ HOÀN THÀNH TẤT CẢ!</div>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

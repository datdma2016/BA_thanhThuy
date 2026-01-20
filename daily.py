import gspread
import requests
import json
import traceback
import shlex
import time
import random
from urllib.parse import urlencode
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime, timedelta

app = Flask(__name__)

# ======================================================
# 1. CẤU HÌNH
# ======================================================

FB_ACCESS_TOKEN = "EAANPvsZANh38BQt8Bcqztr63LDZBieQxO2h5TnOIGpHQtlOnV85cwg7I2ZCVf8vFTccpbB7hX97HYOsGFEKLD3fSZC2BCyKWeZA0vsUJZCXBZAMVZARMwZCvTuPGTsIStG5ro10ltZBXs3yTOzBLjZAjfL8TAeXwgKC73ZBZA3aQD6eludndMkOYFrVCFv2CrIrNe5nX82FScL0TzIXjA7qUl9HZAz" 
FILE_SHEET_GOC = "BA_ads_daily_20260120" 

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
    .date-header { color: #e3b341; font-weight: bold; font-size: 16px; border-top: 1px solid #30363d; margin-top: 10px; padding-top:10px;}
    .next-btn { background: #238636; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 20px;}
    .redirect-msg { color: #ff7b72; font-weight: bold; font-size: 14px; margin-top: 10px; }
    table { width: 100%; border-collapse: collapse; margin-top: 15px; background: #161b22; font-size: 12px; }
    th, td { border: 1px solid #30363d; padding: 8px; text-align: right; }
    th { background-color: #21262d; text-align: center; color: #f0f6fc; }
    td:first-child { text-align: left; }
</style>
"""

# ======================================================
# HÀM BỔ TRỢ
# ======================================================
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

def safe_write_sheet(worksheet, rows):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            worksheet.append_rows(rows)
            return True, None
        except Exception as e:
            time.sleep(3) 
            if attempt == max_retries - 1:
                return False, str(e)
    return False, "Unknown Error"

@app.route('/')
def home():
    return f"""
    <h1>Bot V34: Relay Race Mode (Auto-Redirect)</h1>
    <ul>
        <li><a href='/fb-daily'>/fb-daily</a>: Báo cáo Theo Ngày (Chạy tiếp sức)</li>
    </ul>
    """

@app.route('/fb-daily')
def lay_data_hang_ngay():
    def generate():
        yield CSS_STYLE
        try:
            # Lấy tham số từ URL
            args = request.args.to_dict()
            keyword = args.get('keyword', '')
            ten_tab = args.get('sheet', 'BaoCaoTungNgay')
            start_date_str = args.get('start')
            end_date_str = args.get('end')
            
            if not start_date_str or not end_date_str:
                yield "<div class='log error'>[LỖI] Phải nhập start và end (YYYY-MM-DD)!</div>"
                return

            # Xử lý ngày hiện tại (Chỉ xử lý đúng 1 ngày start)
            current_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
            
            yield f"<h3>> ĐANG XỬ LÝ NGÀY: {start_date_str} (V34)</h3>"
            
            # --- GỌI HÀM XỬ LÝ 1 NGÀY ---
            yield from process_single_day(keyword, ten_tab, start_date_str)
            
            # --- TÍNH TOÁN NGÀY TIẾP THEO ---
            next_date_obj = current_date_obj + timedelta(days=1)
            
            if next_date_obj <= end_date_obj:
                # Tạo link cho ngày tiếp theo
                next_date_str = next_date_obj.strftime("%Y-%m-%d")
                args['start'] = next_date_str # Cập nhật ngày bắt đầu mới
                next_url = request.path + '?' + urlencode(args)
                
                yield f"<div class='redirect-msg'>⏳ Đang chuyển sang ngày {next_date_str} trong 3 giây...</div>"
                yield f"""
                <script>
                    setTimeout(function() {{
                        window.location.href = "{next_url}";
                    }}, 3000);
                </script>
                """
                yield f"<br><a href='{next_url}' class='next-btn'>👉 Bấm vào đây nếu không tự chuyển</a>"
            else:
                yield "<div class='log success' style='margin-top:20px; font-size:16px;'>🎉🎉🎉 ĐÃ HOÀN THÀNH TẤT CẢ! (Finish)</div>"

        except Exception as e:
             yield f"<div class='log error'>[CRASH] {traceback.format_exc()}</div>"
    return Response(stream_with_context(generate()))

# ======================================================
# LOGIC XỬ LÝ 1 NGÀY (Gọn nhẹ)
# ======================================================
def process_single_day(keyword, ten_tab, current_date_str):
    yield f"<div class='log info'>[SHEET] Connecting...</div>"
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    
    try:
        sh = client.open(FILE_SHEET_GOC) 
        # yield f"<div class='log success'>[SHEET] Connected!</div>"
    except Exception as e:
        yield f"<div class='log error'>[SHEET ERROR] {str(e)}</div>"
        return

    HEADERS = ["Ngày", "ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", "Tiền tiêu", "Reach", "Data", "Giá Data", "Doanh Thu", "ROAS", "Lượt mua", "AOV", "Rev/Data", "ThruPlay", "View 25%", "View 100%", "Từ khóa (Tag)"]

    try:
        worksheet = sh.worksheet(ten_tab)
    except:
        yield f"<div class='log warning'>[SHEET] Creating new tab '{ten_tab}'...</div>"
        worksheet = sh.add_worksheet(title=ten_tab, rows=100, cols=20)
        worksheet.append_row(HEADERS)

    KEYWORD_GROUPS = [k.strip() for k in keyword.split(',') if k.strip()]
    fields_video = "video_p25_watched_actions,video_p100_watched_actions,video_thruplay_watched_actions"
    range_dict = {'since': current_date_str, 'until': current_date_str}
    time_str = f'insights.time_range({json.dumps(range_dict)})'
    fields_list = f'name,status,{time_str}{{date_start,spend,reach,actions,action_values,purchase_roas,{fields_video}}}'

    BUFFER_ROWS = [] 

    for tk_obj in DANH_SACH_TKQC:
        id_tk = tk_obj['id']
        ten_tk = tk_obj['name']
        
        base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
        params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 500}
        
        all_campaigns = []
        next_url = base_url
        
        while True:
            try:
                res = requests.get(next_url, params=params if next_url == base_url else None, timeout=10)
                data = res.json()
                if 'error' in data:
                    yield f"<div class='log error'>[ERROR] {ten_tk}: {data['error']['message']}</div>"
                    break
                all_campaigns.extend(data.get('data', []))
                if 'paging' in data and 'next' in data['paging']:
                    next_url = data['paging']['next']
                else: break
            except: break
        
        for camp in all_campaigns:
            ten_camp = camp.get('name', 'Không tên')
            trang_thai = camp.get('status', 'UNKNOWN')
            
            if check_keyword_v12(ten_camp, keyword):
                insights_data = camp.get('insights', {}).get('data', [])
                if insights_data:
                    stat = insights_data[0] 
                    spend = float(stat.get('spend', 0))
                    
                    if spend > 0: 
                        reach = int(stat.get('reach', 0))
                        actions = stat.get('actions', [])
                        action_values = stat.get('action_values', [])
                        cmts = get_fb_value(actions, ['comment'])
                        msgs = get_fb_value(actions, ['onsite_conversion.messaging_conversation_started_7d', 'messaging_conversation_started_7d'])
                        total_data = cmts + msgs
                        revenue = get_fb_value(action_values, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                        orders = get_fb_value(actions, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                        thruplay = get_fb_value(actions, ['video_thruplay_watched_actions'])
                        if thruplay == 0: thruplay = get_fb_value(stat.get('video_thruplay_watched_actions', []), ['video_view', 'video_play'])
                        view25 = get_fb_value(actions, ['video_p25_watched_actions'])
                        if view25 == 0: view25 = get_fb_value(stat.get('video_p25_watched_actions', []),

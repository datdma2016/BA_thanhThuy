import requests
import json
import traceback
import shlex
import time
import io
import csv
from urllib.parse import urlencode
from flask import Flask, request, jsonify, Response, stream_with_context
from datetime import datetime, timedelta

app = Flask(__name__)

# ======================================================
# 1. CẤU HÌNH
# ======================================================

FB_ACCESS_TOKEN = "EAANPvsZANh38BQt8Bcqztr63LDZBieQxO2h5TnOIGpHQtlOnV85cwg7I2ZCVf8vFTccpbB7hX97HYOsGFEKLD3fSZC2BCyKWeZA0vsUJZCXBZAMVZARMwZCvTuPGTsIStG5ro10ltZBXs3yTOzBLjZAjfL8TAeXwgKC73ZBZA3aQD6eludndMkOYFrVCFv2CrIrNe5nX82FScL0TzIXjA7qUl9HZAz" 

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
    .highlight { color: #58a6ff; font-weight: bold; }
    .download-btn { 
        background: #238636; color: white; padding: 12px 24px; 
        text-decoration: none; border-radius: 6px; display: inline-block; 
        margin-top: 15px; font-size: 14px; font-weight: bold;
    }
    .auto-msg { color: #e3b341; margin-top: 10px; font-style: italic; }
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

@app.route('/')
def home():
    return f"""
    <h1>Bot V36: Local Download (Robust Mode)</h1>
    <ul>
        <li><a href='/fb-download'>/fb-download</a>: Tải báo cáo CSV (Đã fix lỗi mất data)</li>
    </ul>
    """

@app.route('/fb-download')
def download_data_ngay():
    args = request.args.to_dict()
    keyword = args.get('keyword', '')
    start_date_str = args.get('start')
    
    if not start_date_str:
        return "<h3>LỖI: Vui lòng nhập ngày bắt đầu (?start=YYYY-MM-DD)</h3>"

    # Bước 1: Quét dữ liệu (Code V36 mạnh mẽ hơn)
    csv_content, row_count, debug_info = process_single_day_csv(keyword, start_date_str)

    # Bước 2: Chuẩn bị chuyển trang
    end_date_str = args.get('end', start_date_str)
    current_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    next_date_obj = current_date_obj + timedelta(days=1)
    next_link = ""
    status_msg = "Đã hoàn tất toàn bộ!"

    if next_date_obj <= end_date_obj:
        next_date_str = next_date_obj.strftime("%Y-%m-%d")
        args['start'] = next_date_str
        next_link = request.path + '?' + urlencode(args)
        status_msg = f"Đang chuyển sang ngày {next_date_str}..."

    import base64
    b64_csv = base64.b64encode(csv_content.encode('utf-8-sig')).decode()
    filename = f"Baocao_{start_date_str}.csv"

    html_response = f"""
    <html>
    <head>{CSS_STYLE}</head>
    <body>
        <h2>📊 Đã quét xong ngày: <span class="highlight">{start_date_str}</span></h2>
        <div class="info">Tìm thấy: {row_count} dòng khớp từ khóa.</div>
        <div class="log" style="font-size:11px; color:#888; max-height:100px; overflow:auto;">{debug_info}</div>
        
        <a id="downloadLink" class="download-btn" download="{filename}" href="data:text/csv;charset=utf-8;base64,{b64_csv}">
            📥 Tải File {filename}
        </a>

        <div class="auto-msg" id="statusMsg">{status_msg}</div>

        <script>
            document.getElementById('downloadLink').click();
            var nextLink = "{next_link}";
            if (nextLink) {{
                setTimeout(function() {{
                    window.location.href = nextLink;
                }}, 3000); 
            }}
        </script>
    </body>
    </html>
    """
    return html_response

# ======================================================
# LOGIC XỬ LÝ CSV (FIXED RETRY)
# ======================================================
def process_single_day_csv(keyword, current_date_str):
    output = io.StringIO()
    writer = csv.writer(output)
    HEADERS = ["Ngày", "ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", "Tiền tiêu", "Reach", "Data", "Giá Data", "Doanh Thu", "ROAS", "Lượt mua", "AOV", "Rev/Data", "ThruPlay", "View 25%", "View 100%", "Từ khóa (Tag)"]
    writer.writerow(HEADERS)

    KEYWORD_GROUPS = [k.strip() for k in keyword.split(',') if k.strip()]
    fields_video = "video_p25_watched_actions,video_p100_watched_actions,video_thruplay_watched_actions"
    range_dict = {'since': current_date_str, 'until': current_date_str}
    time_str = f'insights.time_range({json.dumps(range_dict)})'
    fields_list = f'name,status,{time_str}{{date_start,spend,reach,actions,action_values,purchase_roas,{fields_video}}}'

    total_rows = 0
    debug_log = ""

    for tk_obj in DANH_SACH_TKQC:
        id_tk = tk_obj['id']
        ten_tk = tk_obj['name']
        
        base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
        params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 500} # Lấy 500 thằng 1 lần
        
        all_campaigns = []
        next_url = base_url
        page_count = 0
        
        # --- CƠ CHẾ RETRY MẠNH MẼ ---
        while True:
            retries = 3
            success = False
            while retries > 0:
                try:
                    res = requests.get(next_url, params=params if next_url == base_url else None, timeout=20)
                    data = res.json()
                    
                    if 'error' in data:
                        debug_log += f"<br>[ERROR] {ten_tk}: {data['error']['message']}"
                        retries = 0 # Lỗi Token thì dừng luôn
                        break
                    
                    fetched = data.get('data', [])
                    all_campaigns.extend(fetched)
                    page_count += 1
                    
                    if 'paging' in data and 'next' in data['paging']:
                        next_url = data['paging']['next']
                        success = True # Lấy thành công trang này, chuẩn bị lấy trang sau
                        break # Thoát vòng lặp retry
                    else:
                        next_url = None # Hết trang
                        success = True
                        break
                except Exception as e:
                    retries -= 1
                    debug_log += f"<br>[RETRY] {ten_tk} Page {page_count}: {str(e)} (Còn {retries} lần)"
                    time.sleep(2)
            
            if not success or not next_url:
                break
        
        debug_log += f"<br>✅ {ten_tk}: Quét được tổng {len(all_campaigns)} campaigns gốc."

        for camp in all_campaigns:
            ten_camp = camp.get('name', 'Không tên')
            trang_thai = camp.get('status', 'UNKNOWN')
            
            if check_keyword_v12(ten_camp, keyword):
                insights_data = camp.get('insights', {}).get('data', [])
                if insights_data:
                    stat = insights_data[0] 
                    spend = float(stat.get('spend', 0))
                    
                    # --- ĐIỀU CHỈNH: Bỏ qua check > 0 nếu cần kiểm tra chiến dịch "ẩn" ---
                    # if spend >= 0: # Lấy hết kể cả 0 đồng để kiểm tra
                    
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

                    row = [current_date_str, id_tk, ten_tk, ten_camp, trang_thai, spend, reach, total_data, gia_data, revenue, roas, orders, aov, rev_per_data, thruplay, view25, view100, matched_tag]
                    writer.writerow(row)
                    total_rows += 1

    return output.getvalue(), total_rows, debug_log

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

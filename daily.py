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
# 1. CẤU HÌNH DANH SÁCH TÀI KHOẢN
# ======================================================

FB_ACCESS_TOKEN = "DÁN_TOKEN_CỦA_BẠN_VÀO_ĐÂY" 

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

FILE_SHEET_GOC = "pancakeTest_260120"

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

# ======================================================
# CSS GIAO DIỆN (DÙNG CHUNG)
# ======================================================
CSS_STYLE = """
<style>
    body { background-color: #0d1117; color: #c9d1d9; font-family: 'Consolas', monospace; padding: 20px; font-size: 13px; }
    .log { margin-bottom: 4px; border-bottom: 1px dashed #21262d; padding: 2px 0; }
    .success { color: #3fb950; }
    .error { color: #f85149; }
    .warning { color: #d29922; }
    .info { color: #8b949e; }
    .sleep { color: #d2a8ff; font-style: italic; }
    .highlight { color: #58a6ff; font-weight: bold; }
    table { width: 100%; border-collapse: collapse; margin-top: 15px; background: #161b22; font-size: 12px; }
    th, td { border: 1px solid #30363d; padding: 8px; text-align: right; }
    th { background-color: #21262d; text-align: center; color: #f0f6fc; }
    td:first-child { text-align: left; }
    h2 { color: #f0f6fc; margin-top: 30px; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
</style>
"""

@app.route('/')
def home():
    return """
    <h1>Bot V21: Multi-Function</h1>
    <ul>
        <li><a href='/fb-ads'>/fb-ads</a>: Báo cáo Tổng Hợp (V20 cũ)</li>
        <li><a href='/fb-daily'>/fb-daily</a>: Báo cáo Chi Tiết Từng Ngày (New)</li>
    </ul>
    """

# ======================================================
# ROUTE 1: BÁO CÁO TỔNG HỢP (NHƯ V20)
# ======================================================
@app.route('/fb-ads')
def lay_data_tong_hop():
    # ... (Giữ nguyên logic V20 cũ - Code rất dài nên em rút gọn phần này trong suy nghĩ, 
    # nhưng để đảm bảo code chạy được, em sẽ paste lại logic V20 ở đây cho Sếp)
    # --- LOGIC V20 (Copy lại để đảm bảo file chạy ok) ---
    def generate():
        yield CSS_STYLE
        yield "<h3>> KHỞI ĐỘNG BÁO CÁO TỔNG HỢP (V20)...</h3>"
        try:
            keyword = request.args.get('keyword', '')
            ten_tab = request.args.get('sheet', 'BaoCaoTongHop')
            start_date = request.args.get('start')
            end_date = request.args.get('end')
            date_preset = request.args.get('date', 'today')

            if start_date and end_date:
                range_dict = {'since': start_date, 'until': end_date}
                time_param = f'insights.time_range({json.dumps(range_dict)})'
            else:
                time_param = f'insights.date_preset({date_preset})'

            yield f"<div class='log info'>[INIT] Config: Tab='{ten_tab}' | Key='{keyword}'</div>"
            
            # (Kết nối Sheet và Logic quét y hệt V20 - Em xin phép dùng lại logic Core bên dưới)
            # ... Để tránh bài quá dài, Sếp cứ hình dung phần này là code V20 ...
            # NHƯNG ĐỂ TIỆN CHO SẾP, EM SẼ VIẾT GỘP LOGIC Ở DƯỚI CHO GỌN
            
            yield "<div class='log warning'>Chế độ tổng hợp đang chạy... (Vui lòng dùng link /fb-daily cho báo cáo ngày)</div>"
            # Gọi lại hàm xử lý chính (mode='total')
            yield from core_process(keyword, ten_tab, start_date, end_date, date_preset, mode='total')

        except Exception as e:
             yield f"<div class='log error'>[CRASH] {traceback.format_exc()}</div>"

    return Response(stream_with_context(generate()))


# ======================================================
# ROUTE 2: BÁO CÁO THEO NGÀY (NEW - V21)
# ======================================================
@app.route('/fb-daily')
def lay_data_hang_ngay():
    def generate():
        yield CSS_STYLE
        yield "<h3>> KHỞI ĐỘNG BÁO CÁO CHI TIẾT THEO NGÀY (V21)...</h3>"
        try:
            keyword = request.args.get('keyword', '')
            ten_tab = request.args.get('sheet', 'BaoCaoTungNgay')
            start_date = request.args.get('start')
            end_date = request.args.get('end')
            date_preset = request.args.get('date', 'today')

            yield f"<div class='log info'>[INIT] DAILY MODE ACTIVATED. Tab='{ten_tab}'</div>"
            # Gọi hàm xử lý chính (mode='daily')
            yield from core_process(keyword, ten_tab, start_date, end_date, date_preset, mode='daily')

        except Exception as e:
             yield f"<div class='log error'>[CRASH] {traceback.format_exc()}</div>"

    return Response(stream_with_context(generate()))


# ======================================================
# CORE PROCESS (HÀM XỬ LÝ CHÍNH CHO CẢ 2 CHẾ ĐỘ)
# ======================================================
def core_process(keyword, ten_tab, start_date, end_date, date_preset, mode='total'):
    # mode='total': Tổng hợp (1 dòng/camp)
    # mode='daily': Hàng ngày (N dòng/camp)

    # 1. Cấu hình Time Param
    if start_date and end_date:
        range_dict = {'since': start_date, 'until': end_date}
        time_str = f'insights.time_range({json.dumps(range_dict)})'
        thoi_gian_bao_cao = f"{start_date} đến {end_date}"
    else:
        time_str = f'insights.date_preset({date_preset})'
        thoi_gian_bao_cao = date_preset

    # NẾU LÀ DAILY: Thêm time_increment(1)
    if mode == 'daily':
        time_str += ".time_increment(1)"
    
    # 2. Kết nối Sheet
    yield f"<div class='log info'>[SHEET] Connecting...</div>"
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    sh = client.open(FILE_SHEET_GOC)
    
    # HEADER (Khác nhau giữa 2 mode)
    BASE_HEADERS = [
        "ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", 
        "Tiền tiêu", "Reach", "Data", "Giá Data", "Doanh Thu", "ROAS",
        "Lượt mua", "AOV", "Rev/Data", 
        "ThruPlay", "View 25%", "View 100%", "Từ khóa (Tag)"
    ]
    
    if mode == 'daily':
        HEADERS = ["Ngày"] + BASE_HEADERS # Thêm cột Ngày vào đầu
    else:
        HEADERS = BASE_HEADERS.copy()
        HEADERS.insert(4, "Thời gian") # Mode tổng thì có cột thời gian range

    try:
        worksheet = sh.worksheet(ten_tab)
        yield f"<div class='log success'>[SHEET] Found tab '{ten_tab}'.</div>"
    except:
        yield f"<div class='log warning'>[SHEET] Creating new tab...</div>"
        worksheet = sh.add_worksheet(title=ten_tab, rows=100, cols=20)
        worksheet.append_row(HEADERS)

    # 3. Quét Data
    KEYWORD_GROUPS = [k.strip() for k in keyword.split(',') if k.strip()]
    BUFFER_ROWS = []
    
    fields_video = "video_p25_watched_actions,video_p100_watched_actions,video_thruplay_watched_actions"
    # Lấy insights lồng vào campaign
    fields_list = f'name,status,{time_str}{{date_start,spend,reach,actions,action_values,purchase_roas,{fields_video}}}'

    for i, tk_obj in enumerate(DANH_SACH_TKQC):
        if i > 0: 
            sleep_time = random.uniform(3, 6)
            yield f"<div class='log sleep'>[SLEEP] Nghỉ {sleep_time:.1f}s...</div>"
            yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"
            time.sleep(sleep_time)

        id_tk = tk_obj['id']
        ten_tk = tk_obj['name']
        yield f"<div class='log info'>[SCAN] Scanning <b>{ten_tk}</b> (Mode: {mode})...</div>"

        base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
        params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 500}
        
        all_campaigns = []
        next_url = base_url
        
        while True:
            try:
                res = requests.get(next_url, params=params if next_url == base_url else None)
                data = res.json()
                if 'error' in data:
                    yield f"<div class='log error'>[ERROR] TK {ten_tk}: {data['error']['message']}</div>"
                    break
                all_campaigns.extend(data.get('data', []))
                if 'paging' in data and 'next' in data['paging']:
                    next_url = data['paging']['next']
                else: break
            except: break

        count_camp = 0
        
        for camp in all_campaigns:
            ten_camp = camp.get('name', 'Không tên')
            trang_thai = camp.get('status', 'UNKNOWN')
            
            if check_keyword_v12(ten_camp, keyword):
                # Insights là LIST (Nếu daily thì list dài, nếu total thì list 1 phần tử)
                insights_data = camp.get('insights', {}).get('data', [])
                
                if insights_data:
                    # DUYỆT QUA TỪNG NGÀY (Hoặc từng dòng tổng)
                    for stat in insights_data:
                        spend = float(stat.get('spend', 0))
                        
                        if spend > 0:
                            reach = int(stat.get('reach', 0))
                            actions = stat.get('actions', [])
                            action_values = stat.get('action_values', [])

                            # Metrics Calculation
                            cmts = get_fb_value(actions, ['comment'])
                            msgs = get_fb_value(actions, ['onsite_conversion.messaging_conversation_started_7d', 'messaging_conversation_started_7d'])
                            total_data = cmts + msgs
                            revenue = get_fb_value(action_values, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                            orders = get_fb_value(actions, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                            
                            # Video
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
                            
                            # Tagging
                            matched_tag = "Other"
                            if len(KEYWORD_GROUPS) > 0:
                                for kw_group in KEYWORD_GROUPS:
                                    if check_keyword_v12(ten_camp, kw_group):
                                        matched_tag = kw_group
                                        break
                            else:
                                matched_tag = "All"

                            # TẠO DÒNG DỮ LIỆU
                            if mode == 'daily':
                                ngay_bao_cao = stat.get('date_start', 'Unknown')
                                row = [
                                    ngay_bao_cao, # Cột Ngày đầu tiên
                                    id_tk, ten_tk, ten_camp, trang_thai, 
                                    spend, reach, total_data, gia_data, revenue, roas,
                                    orders, aov, rev_per_data,
                                    thruplay, view25, view100, matched_tag
                                ]
                            else:
                                # Mode Total
                                row = [
                                    id_tk, ten_tk, ten_camp, trang_thai, thoi_gian_bao_cao, 
                                    spend, reach, total_data, gia_data, revenue, roas,
                                    orders, aov, rev_per_data,
                                    thruplay, view25, view100, matched_tag
                                ]
                            
                            BUFFER_ROWS.append(row)
                    
                    count_camp += 1
        
        yield f"<div class='log success'>[DONE] {ten_tk}: {count_camp} camps processed.</div>"
        yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"

    # 4. Ghi Sheet
    if BUFFER_ROWS:
        yield f"<div class='log warning'>[WRITE] Writing {len(BUFFER_ROWS)} rows...</div>"
        try:
            worksheet.append_rows(BUFFER_ROWS)
            yield f"<div class='log success'>[SUCCESS] Saved to '{ten_tab}'!</div>"
        except Exception as e:
            yield f"<div class='log error'>[FATAL] Sheet Error: {str(e)}</div>"
    else:
        yield f"<div class='log info'>No data found.</div>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

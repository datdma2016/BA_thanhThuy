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

FILE_SHEET_GOC = "pancakeTest_260120"

# ======================================================

def fmt_vn(value):
    if not value: return "0"
    try:
        return "{:,.0f}".format(value).replace(",", ".")
    except:
        return str(value)

def get_fb_value(data_list, keys_target, value_key='value'):
    """Hàm đào dữ liệu đa năng"""
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
    return "<h1>Bot V18: Video Deep Scan - Fix Metrics 0</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    def generate():
        yield """
        <style>
            body { background-color: #0d1117; color: #c9d1d9; font-family: 'Consolas', monospace; padding: 20px; font-size: 13px; }
            .log { margin-bottom: 4px; border-bottom: 1px dashed #21262d; padding: 2px 0; }
            .success { color: #3fb950; }
            .error { color: #f85149; }
            .warning { color: #d29922; }
            .info { color: #8b949e; }
            .debug { color: #e3b341; font-weight:bold; } /* Màu vàng cho Debug */
            .sleep { color: #d2a8ff; font-style: italic; }
            .highlight { color: #58a6ff; font-weight: bold; }
            
            table { width: 100%; border-collapse: collapse; margin-top: 15px; background: #161b22; font-size: 12px; }
            th, td { border: 1px solid #30363d; padding: 8px; text-align: right; }
            th { background-color: #21262d; text-align: center; color: #f0f6fc; }
            td:first-child { text-align: left; }
            
            .kpi-box { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 15px; }
            .kpi-card { background: #161b22; border: 1px solid #30363d; padding: 10px; border-radius: 6px; }
            .kpi-title { font-size: 11px; color: #8b949e; text-transform: uppercase; }
            .kpi-value { font-size: 18px; font-weight: bold; margin-top: 5px; color: #f0f6fc; }
            
            .final-section { background: #0d1117; border-top: 2px solid #30363d; margin-top: 30px; padding-top: 20px; }
        </style>
        <h3>> KHỞI ĐỘNG V18 (VIDEO DEEP SCAN)...</h3>
        """
        
        try:
            # --- 1. LẤY THAM SỐ ---
            keyword = request.args.get('keyword', '')
            ten_tab = request.args.get('sheet', 'BaoCaoV18_VideoFix')
            start_date = request.args.get('start')
            end_date = request.args.get('end')
            date_preset = request.args.get('date', 'today')

            if start_date and end_date:
                range_dict = {'since': start_date, 'until': end_date}
                time_param = f'insights.time_range({json.dumps(range_dict)})'
                thoi_gian_bao_cao = f"{start_date} đến {end_date}"
            else:
                time_param = f'insights.date_preset({date_preset})'
                thoi_gian_bao_cao = date_preset

            yield f"<div class='log info'>[INIT] Config: Tab='{ten_tab}' | Key='{keyword}'</div>"

            # --- 2. KẾT NỐI SHEET ---
            yield f"<div class='log info'>[SHEET] Connecting...</div>"
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
            client = gspread.authorize(creds)
            sh = client.open(FILE_SHEET_GOC)
            
            HEADERS = [
                "ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", "Thời gian", 
                "Tiền tiêu", "Reach", "Data", "Giá Data", "Doanh Thu", "ROAS",
                "ThruPlay", "View 25% (Số)", "View 25% (Tỉ lệ)", "View 100% (Số)", "View 100% (Tỉ lệ)"
            ]
            
            try:
                worksheet = sh.worksheet(ten_tab)
                yield f"<div class='log success'>[SHEET] Found tab '{ten_tab}'.</div>"
            except:
                yield f"<div class='log warning'>[SHEET] Creating new tab...</div>"
                worksheet = sh.add_worksheet(title=ten_tab, rows=100, cols=20)
                worksheet.append_row(HEADERS)

            # --- 3. QUÉT DỮ LIỆU ---
            grand_total = {
                'spend': 0, 'revenue': 0, 'data': 0, 'reach': 0,
                'thruplay': 0, 'view25': 0, 'view100': 0
            }
            tong_hop_tk = {}
            BUFFER_ROWS = [] 
            
            # --- CẤU HÌNH QUAN TRỌNG: GỌI ĐÍCH DANH CHỈ SỐ VIDEO ---
            fields_video = "video_p25_watched_actions,video_p100_watched_actions,video_thruplay_watched_actions"
            fields_list = f'name,status,{time_param}{{spend,reach,actions,action_values,purchase_roas,{fields_video}}}'

            for i, tk_obj in enumerate(DANH_SACH_TKQC):
                if i > 0: 
                    sleep_time = random.uniform(3, 6) 
                    yield f"<div class='log sleep'>[SLEEP] Nghỉ {sleep_time:.1f}s...</div>"
                    yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"
                    time.sleep(sleep_time)

                id_tk = tk_obj['id']
                ten_tk = tk_obj['name']
                
                if ten_tk not in tong_hop_tk:
                    tong_hop_tk[ten_tk] = {
                        'id': id_tk, 'spend': 0, 'revenue': 0, 'data': 0, 'reach': 0, 'camp_count': 0,
                        'view25': 0, 'view100': 0
                    }

                yield f"<div class='log info'>[SCAN] Scanning <b>{ten_tk}</b>...</div>"
                
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
                debug_shown = False # Chỉ hiện debug cho camp đầu tiên để đỡ rối
                
                for camp in all_campaigns:
                    ten_camp = camp.get('name', 'Không tên')
                    trang_thai = camp.get('status', 'UNKNOWN')
                    
                    if check_keyword_v12(ten_camp, keyword):
                        insights = camp.get('insights', {}).get('data', [])
                        if insights:
                            stat = insights[0]
                            spend = float(stat.get('spend', 0))
                            
                            if spend > 0:
                                reach = int(stat.get('reach', 0))
                                actions = stat.get('actions', [])
                                action_values = stat.get('action_values', [])

                                # --- DEBUG ACTION KEYS (Để bắt bệnh nếu = 0) ---
                                if not debug_shown and actions:
                                    keys_found = [x.get('action_type') for x in actions]
                                    yield f"<div class='log debug'>[DEBUG] {ten_tk} keys: {str(keys_found)}</div>"
                                    debug_shown = True

                                # Data & Revenue
                                cmts = get_fb_value(actions, ['comment'])
                                msgs = get_fb_value(actions, ['onsite_conversion.messaging_conversation_started_7d', 'messaging_conversation_started_7d'])
                                total_data = cmts + msgs
                                revenue = get_fb_value(action_values, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                                
                                # --- LẤY CHỈ SỐ VIDEO (ƯU TIÊN LẤY TỪ ACTIONS CHUNG) ---
                                thruplay = get_fb_value(actions, ['video_thruplay_watched_actions'])
                                view25 = get_fb_value(actions, ['video_p25_watched_actions'])
                                view100 = get_fb_value(actions, ['video_p100_watched_actions'])
                                
                                # --- NẾU VẪN = 0, THỬ TÌM TRONG TRƯỜNG RIÊNG (CƠ CHẾ VÉT CẠN) ---
                                if thruplay == 0:
                                    # Khi gọi riêng, nó trả về list: [{'action_type': 'video_view', 'value': '123'}]
                                    thruplay = get_fb_value(stat.get('video_thruplay_watched_actions', []), ['video_view', 'video_play'])
                                if view25 == 0:
                                    view25 = get_fb_value(stat.get('video_p25_watched_actions', []), ['video_view', 'video_play'])
                                if view100 == 0:
                                    view100 = get_fb_value(stat.get('video_p100_watched_actions', []), ['video_view', 'video_play'])

                                # Tính toán Rate
                                rate25 = (view25 / reach) if reach > 0 else 0
                                rate100 = (view100 / reach) if reach > 0 else 0
                                
                                gia_data = round(spend / total_data) if total_data > 0 else 0
                                roas = (revenue / spend) if spend > 0 else 0
                                
                                row = [
                                    id_tk, ten_tk, ten_camp, trang_thai, thoi_gian_bao_cao, 
                                    spend, reach, total_data, gia_data, revenue, roas,
                                    thruplay, view25, rate25, view100, rate100
                                ]
                                BUFFER_ROWS.append(row)
                                
                                tong_hop_tk[ten_tk]['spend'] += spend
                                tong_hop_tk[ten_tk]['revenue'] += revenue
                                tong_hop_tk[ten_tk]['data'] += total_data
                                tong_hop_tk[ten_tk]['reach'] += reach
                                tong_hop_tk[ten_tk]['camp_count'] += 1
                                tong_hop_tk[ten_tk]['view25'] += view25
                                tong_hop_tk[ten_tk]['view100'] += view100
                                
                                grand_total['spend'] += spend
                                grand_total['revenue'] += revenue
                                grand_total['data'] += total_data
                                grand_total['reach'] += reach
                                grand_total['thruplay'] += thruplay
                                grand_total['view25'] += view25
                                grand_total['view100'] += view100
                                
                                count_camp += 1
                
                yield f"<div class='log success'>[DONE] {ten_tk}: {count_camp} camps matched.</div>"
                yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"

            if BUFFER_ROWS:
                yield f"<div class='log warning'>[WRITE] Writing {len(BUFFER_ROWS)} rows...</div>"
                try:
                    worksheet.append_rows(BUFFER_ROWS)
                    yield f"<div class='log success'>[SUCCESS] Saved!</div>"
                except Exception as e:
                    yield f"<div class='log error'>[FATAL] Sheet Error: {str(e)}</div>"
            else:
                yield f"<div class='log info'>No data found.</div>"

            g_reach = grand_total['reach']
            g_view25 = grand_total['view25']
            g_view100 = grand_total['view100']
            
            g_rate25 = (g_view25 / g_reach * 100) if g_reach > 0 else 0
            g_rate100 = (g_view100 / g_reach * 100) if g_reach > 0 else 0
            
            yield f"""
            <div class='final-section'>
                <h2 style='color:#f0f6fc; margin-bottom: 20px;'>BÁO CÁO V18 (VIDEO DEEP SCAN)</h2>
                
                <div class='kpi-box'>
                    <div class='kpi-card'><div class='kpi-title'>💰 Tổng Tiêu</div><div class='kpi-value' style='color:#ff7b72'>{fmt_vn(grand_total['spend'])}</div></div>
                    <div class='kpi-card'><div class='kpi-title'>💎 Doanh Thu</div><div class='kpi-value' style='color:#3fb950'>{fmt_vn(grand_total['revenue'])}</div></div>
                    <div class='kpi-card'><div class='kpi-title'>👀 Tổng Reach</div><div class='kpi-value'>{fmt_vn(g_reach)}</div></div>
                    <div class='kpi-card'><div class='kpi-title'>▶️ ThruPlay</div><div class='kpi-value'>{fmt_vn(grand_total['thruplay'])}</div></div>
                </div>

                <div class='kpi-box'>
                    <div class='kpi-card'>
                        <div class='kpi-title'>📉 View 25% (Rate)</div>
                        <div class='kpi-value'>{fmt_vn(g_view25)} <span style='font-size:14px; color:#a5d6ff'>({g_rate25:.2f}%)</span></div>
                    </div>
                    <div class='kpi-card'>
                        <div class='kpi-title'>🎯 View 100% (Rate)</div>
                        <div class='kpi-value'>{fmt_vn(g_view100)} <span style='font-size:14px; color:#a5d6ff'>({g_rate100:.2f}%)</span></div>
                    </div>
                </div>
                
                <table>
                    <thead><tr><th>Tên TK</th><th>Tiêu</th><th>Data</th><th>Rev</th><th>Reach</th><th>View 25% (Tỉ lệ)</th><th>View 100% (Tỉ lệ)</th></tr></thead>
                    <tbody>
            """
            for ten, val in tong_hop_tk.items():
                r_reach = val['reach']
                r_v25 = val['view25']
                r_v100 = val['view100']
                
                rate_v25 = (r_v25 / r_reach * 100) if r_reach > 0 else 0
                rate_v100 = (r_v100 / r_reach * 100) if r_reach > 0 else 0
                
                yield f"""
                    <tr>
                        <td><span class='highlight'>{ten}</span></td>
                        <td align='right'>{fmt_vn(val['spend'])}</td>
                        <td align='center'>{fmt_vn(val['data'])}</td>
                        <td align='right'>{fmt_vn(val['revenue'])}</td>
                        <td align='center'>{fmt_vn(r_reach)}</td>
                        <td align='right'>{fmt_vn(r_v25)} <small>({rate_v25:.1f}%)</small></td>
                        <td align='right'>{fmt_vn(r_v100)} <small>({rate_v100:.1f}%)</small></td>
                    </tr>
                """
            yield """</tbody></table></div><script>window.scrollTo(0, document.body.scrollHeight);</script>"""

        except Exception as e:
             yield f"<div class='log error'>[CRASH] {traceback.format_exc()}</div>"

    return Response(stream_with_context(generate()))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

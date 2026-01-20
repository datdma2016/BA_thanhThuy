import requests
import json
import traceback
import shlex
import time
import io
import csv
import random
import base64
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
# CSS GIAO DIỆN (Đen huyền bí)
# ======================================================
CSS_STYLE = """
<style>
    body { background-color: #0d1117; color: #c9d1d9; font-family: 'Consolas', monospace; padding: 20px; font-size: 13px; }
    .log-entry { border-bottom: 1px solid #21262d; padding: 2px 0; }
    .success { color: #3fb950; }
    .error { color: #f85149; }
    .warning { color: #d29922; }
    .info { color: #8b949e; }
    .highlight { color: #58a6ff; font-weight: bold; }
    .status-bar { 
        position: fixed; top: 0; left: 0; right: 0; 
        background: #161b22; border-bottom: 1px solid #30363d; 
        padding: 10px; font-weight: bold; font-size: 14px; color: #e3b341;
        text-align: center; z-index: 999;
    }
    .content { margin-top: 50px; }
    .download-msg { 
        background: #238636; color: white; padding: 15px; 
        border-radius: 6px; margin-top: 20px; text-align: center; 
        font-size: 16px; font-weight: bold; display: none;
    }
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
    
    # Xử lý parse từ khóa an toàn hơn để tránh lỗi 500
    try:
        or_groups = keyword_string.split(',') 
    except:
        return False

    for group in or_groups:
        try:
            terms = shlex.split(group)
        except:
            terms = group.split() # Fallback nếu shlex lỗi
            
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
    <h1>Bot V40: Streaming Download (Anti-Timeout)</h1>
    <ul>
        <li><a href='/fb-download'>/fb-download</a></li>
    </ul>
    """

@app.route('/fb-download')
def download_data_ngay():
    def generate():
        # Gửi Header HTML ngay lập tức để giữ kết nối
        yield f"<html><head>{CSS_STYLE}</head><body>"
        
        try:
            args = request.args.to_dict()
            keyword = args.get('keyword', '')
            start_date_str = args.get('start')
            
            if not start_date_str:
                yield "<div class='error'>❌ LỖI: Thiếu ngày bắt đầu (?start=...)</div></body></html>"
                return

            yield f"<div class='status-bar'>ĐANG XỬ LÝ NGÀY: {start_date_str}</div>"
            yield "<div class='content'>"

            # --- BẮT ĐẦU XỬ LÝ LOGIC ---
            # Chuẩn bị CSV Header
            csv_output = io.StringIO()
            writer = csv.writer(csv_output)
            HEADERS = ["Ngày", "ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", "Tiền tiêu", "Reach", "Data", "Giá Data", "Doanh Thu", "ROAS", "Lượt mua", "AOV", "Rev/Data", "ThruPlay", "View 25%", "View 100%", "Từ khóa (Tag)"]
            writer.writerow(HEADERS)
            
            KEYWORD_GROUPS = [k.strip() for k in keyword.split(',') if k.strip()]
            range_dict = {'since': start_date_str, 'until': start_date_str}
            time_str = f'insights.time_range({json.dumps(range_dict)})'
            fields_video = "video_p25_watched_actions,video_p100_watched_actions,video_thruplay_watched_actions"
            fields_list = f'name,status,{time_str}{{date_start,spend,reach,actions,action_values,purchase_roas,{fields_video}}}'

            total_rows = 0

            # Duyệt từng tài khoản
            for i, tk_obj in enumerate(DANH_SACH_TKQC):
                # Sleep nhẹ
                if i > 0: time.sleep(random.uniform(1, 2))
                
                id_tk = tk_obj['id']
                ten_tk = tk_obj['name']
                yield f"<div class='log-entry'>Scan: <span class='highlight'>{ten_tk}</span>... "
                yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>" # Cuộn xuống
                
                base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
                params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 500}
                
                all_campaigns = []
                next_url = base_url
                
                # Vòng lặp lấy dữ liệu (Retry Logic)
                while True:
                    retries = 3
                    success = False
                    while retries > 0:
                        try:
                            res = requests.get(next_url, params=params if next_url == base_url else None, timeout=30)
                            data = res.json()
                            if 'error' in data:
                                yield f"<span class='error'>[Err: {data['error']['message']}]</span>"
                                retries = 0; break
                            
                            fetched = data.get('data', [])
                            all_campaigns.extend(fetched)
                            
                            if 'paging' in data and 'next' in data['paging']:
                                next_url = data['paging']['next']
                                success = True; break
                            else:
                                next_url = None
                                success = True; break
                        except Exception as e:
                            retries -= 1
                            yield f"<span class='warning'>[Retry]</span>"
                            time.sleep(2)
                    
                    if not success or not next_url: break
                    yield "." # Dấu chấm báo hiệu đang tải trang
                    time.sleep(0.5)

                # Lọc và ghi CSV (vào bộ nhớ đệm)
                camp_count = 0
                for camp in all_campaigns:
                    ten_camp = camp.get('name', 'Không tên')
                    trang_thai = camp.get('status', 'UNKNOWN')
                    
                    if check_keyword_v12(ten_camp, keyword):
                        insights_data = camp.get('insights', {}).get('data', [])
                        if insights_data:
                            stat = insights_data[0]
                            spend = float(stat.get('spend', 0))
                            # Logic lấy chỉ số (giữ nguyên như cũ)
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

                            row = [start_date_str, id_tk, ten_tk, ten_camp, trang_thai, spend, reach, total_data, gia_data, revenue, roas, orders, aov, rev_per_data, thruplay, view25, view100, matched_tag]
                            writer.writerow(row)
                            camp_count += 1
                            total_rows += 1
                
                yield f" <span class='success'>OK ({camp_count})</span></div>"

            # --- PHẦN QUAN TRỌNG: KÍCH HOẠT TẢI FILE BẰNG JS ---
            # Chuyển CSV thành Base64 để gửi qua HTML
            csv_str = csv_output.getvalue()
            b64_csv = base64.b64encode(csv_str.encode('utf-8-sig')).decode()
            filename = f"Baocao_{start_date_str}.csv"
            
            yield f"""
            <div id='doneMsg' class='download-msg'>
                ✅ Đã xong {total_rows} dòng. Đang tải file...
            </div>
            <script>
                document.getElementById('doneMsg').style.display = 'block';
                
                // Hàm tải file từ Base64
                function downloadFile(filename, b64) {{
                    var element = document.createElement('a');
                    element.setAttribute('href', 'data:text/csv;charset=utf-8;base64,' + b64);
                    element.setAttribute('download', filename);
                    element.style.display = 'none';
                    document.body.appendChild(element);
                    element.click();
                    document.body.removeChild(element);
                }}
                
                // Kích hoạt tải ngay
                downloadFile("{filename}", "{b64_csv}");
            </script>
            """

            # --- TỰ ĐỘNG CHUYỂN NGÀY ---
            end_date_str = args.get('end', start_date_str)
            current_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
            next_date_obj = current_date_obj + timedelta(days=1)
            
            if next_date_obj <= end_date_obj:
                next_date_str = next_date_obj.strftime("%Y-%m-%d")
                args['start'] = next_date_str
                next_link = request.path + '?' + urlencode(args)
                
                # Random sleep trước khi chuyển trang
                delay = random.randint(3000, 5000)
                yield f"<div class='info' style='text-align:center; margin-top:10px;'>⏳ Chuyển sang ngày {next_date_str} sau {delay/1000}s...</div>"
                yield f"""
                <script>
                    setTimeout(function() {{
                        window.location.href = "{next_link}";
                    }}, {delay});
                </script>
                """
            else:
                yield "<div class='success' style='text-align:center; margin-top:20px; font-size:20px;'>🎉 HOÀN TẤT TOÀN BỘ!</div>"

            yield "</div></body></html>"

        except Exception as e:
            yield f"<div class='error'>CRASH: {traceback.format_exc()}</div></body></html>"

    return Response(stream_with_context(generate()))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

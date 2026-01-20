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
# CSS GIAO DIỆN (Đen huyền bí & Log chi tiết)
# ======================================================
CSS_STYLE = """
<style>
    body { background-color: #0d1117; color: #c9d1d9; font-family: 'Consolas', monospace; padding: 20px; font-size: 12px; }
    .log-line { border-bottom: 1px solid #21262d; padding: 2px 0; white-space: pre-wrap; word-wrap: break-word;}
    .success { color: #3fb950; }
    .error { color: #f85149; }
    .warning { color: #d29922; }
    .info { color: #8b949e; }
    .highlight { color: #58a6ff; font-weight: bold; }
    .page-tag { background: #1f6feb; color: white; padding: 1px 4px; border-radius: 3px; font-size: 10px; margin-right: 5px;}
    .status-bar { 
        position: fixed; top: 0; left: 0; right: 0; 
        background: #161b22; border-bottom: 1px solid #30363d; 
        padding: 10px; font-weight: bold; font-size: 14px; color: #e3b341;
        text-align: center; z-index: 999;
    }
    .content { margin-top: 50px; margin-bottom: 100px; }
    .download-area { 
        position: fixed; bottom: 0; left: 0; right: 0;
        background: #161b22; border-top: 1px solid #30363d;
        padding: 15px; text-align: center; z-index: 999;
    }
    .btn { 
        background: #238636; color: white; padding: 10px 20px; 
        text-decoration: none; border-radius: 5px; font-weight: bold; cursor: pointer;
        border: none; font-size: 14px;
    }
    .btn:disabled { background: #484f58; cursor: not-allowed; }
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
    try:
        or_groups = keyword_string.split(',') 
    except: return False

    for group in or_groups:
        try: terms = shlex.split(group)
        except: terms = group.split()
            
        match_group = True
        for term in terms:
            is_negative = term.startswith('-')
            tu_khoa_chuan = term[1:] if is_negative else term
            tu_khoa_chuan = tu_khoa_chuan.lower()
            tim_thay = tu_khoa_chuan in ten_camp_lower
            if is_negative:
                if tim_thay:
                    match_group = False; break
            else:
                if not tim_thay:
                    match_group = False; break
        if match_group: return True 
    return False 

@app.route('/')
def home():
    return f"""
    <h1>Bot V41: Live Streaming Download (Full Log)</h1>
    <ul>
        <li><a href='/fb-download'>/fb-download</a></li>
    </ul>
    """

@app.route('/fb-download')
def download_data_ngay():
    def generate():
        # Gửi Header HTML
        yield f"<html><head>{CSS_STYLE}</head><body>"
        
        args = request.args.to_dict()
        keyword = args.get('keyword', '')
        start_date_str = args.get('start')
        
        if not start_date_str:
            yield "<div class='error'>❌ LỖI: Thiếu ngày bắt đầu (?start=...)</div></body></html>"
            return

        yield f"<div class='status-bar'>ĐANG XỬ LÝ: {start_date_str}</div>"
        yield "<div class='content' id='logArea'>"
        
        # --- JS ĐỂ HỨNG DỮ LIỆU ---
        # Chúng ta sẽ dùng mảng JS để gom dữ liệu thay vì server gom
        yield """
        <script>
            var csvData = [];
            // Hàm thêm dòng vào CSV
            function addRow(rowString) {
                csvData.push(rowString);
            }
            
            // Hàm tải file
            function downloadCSV(filename) {
                var blob = new Blob([csvData.join("\\n")], { type: 'text/csv;charset=utf-8;' });
                var link = document.createElement("a");
                var url = URL.createObjectURL(blob);
                link.setAttribute("href", url);
                link.setAttribute("download", filename);
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            }
        </script>
        """

        # Header CSV
        HEADERS = ["Ngày", "ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", "Tiền tiêu", "Reach", "Data", "Giá Data", "Doanh Thu", "ROAS", "Lượt mua", "AOV", "Rev/Data", "ThruPlay", "View 25%", "View 100%", "Từ khóa (Tag)"]
        # Gửi header xuống JS ngay lập tức
        header_str = ",".join(HEADERS)
        yield f"<script>addRow('{header_str}');</script>"

        # Params Facebook
        KEYWORD_GROUPS = [k.strip() for k in keyword.split(',') if k.strip()]
        range_dict = {'since': start_date_str, 'until': start_date_str}
        time_str = f'insights.time_range({json.dumps(range_dict)})'
        fields_video = "video_p25_watched_actions,video_p100_watched_actions,video_thruplay_watched_actions"
        fields_list = f'name,status,{time_str}{{date_start,spend,reach,actions,action_values,purchase_roas,{fields_video}}}'

        total_rows_day = 0

        # DUYỆT TÀI KHOẢN
        for i, tk_obj in enumerate(DANH_SACH_TKQC):
            if i > 0: time.sleep(1) # Nghỉ nhẹ
            
            id_tk = tk_obj['id']
            ten_tk = tk_obj['name']
            
            yield f"<div class='log-line'>Scanning <b class='highlight'>{ten_tk}</b>...</div>"
            yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"

            base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
            params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 500} # Lấy 500
            
            next_url = base_url
            page_num = 0
            
            # VÒNG LẶP TRANG (PAGING)
            while True:
                page_num += 1
                yield f"<div class='log-line info'> &nbsp;&nbsp; ↳ <span class='page-tag'>Page {page_num}</span> Đang tải từ Facebook...</div>"
                yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"
                
                # Retry Logic
                retries = 3
                data = None
                while retries > 0:
                    try:
                        res = requests.get(next_url, params=params if next_url == base_url else None, timeout=45)
                        data = res.json()
                        if 'error' in data:
                            yield f"<div class='log-line error'> &nbsp;&nbsp; ❌ LỖI API: {data['error']['message']}</div>"
                            data = None; break
                        break # Thành công
                    except Exception as e:
                        retries -= 1
                        yield f"<div class='log-line warning'> &nbsp;&nbsp; ⚠️ Lỗi mạng, thử lại ({retries})...</div>"
                        time.sleep(3)
                
                if not data: break # Nếu lỗi quá 3 lần thì bỏ qua TK này

                campaigns = data.get('data', [])
                count_in_page = len(campaigns)
                yield f"<div class='log-line success'> &nbsp;&nbsp; ✅ Tải xong. Tìm thấy {count_in_page} campaigns. Đang lọc...</div>"
                
                # XỬ LÝ DỮ LIỆU NGAY LẬP TỨC (KHÔNG GOM)
                matched_count = 0
                csv_buffer = [] # Buffer nhỏ cho 1 trang thôi
                
                for camp in campaigns:
                    ten_camp = camp.get('name', 'Không tên')
                    trang_thai = camp.get('status', 'UNKNOWN')
                    
                    if check_keyword_v12(ten_camp, keyword):
                        insights_data = camp.get('insights', {}).get('data', [])
                        if insights_data:
                            stat = insights_data[0]
                            spend = float(stat.get('spend', 0))
                            
                            # Logic tính toán (giữ nguyên)
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
                                        matched_tag = kw_group; break
                            else: matched_tag = "All"

                            # Format CSV Line (Thủ công để tránh lỗi thư viện)
                            # Cần escape dấu phẩy trong tên chiến dịch nếu có
                            safe_ten_tk = ten_tk.replace('"', '""')
                            safe_ten_camp = ten_camp.replace('"', '""')
                            
                            # Tạo dòng CSV chuẩn
                            row_str = f'"{start_date_str}","{id_tk}","{safe_ten_tk}","{safe_ten_camp}","{trang_thai}",{spend},{reach},{total_data},{gia_data},{revenue},{roas},{orders},{aov},{rev_per_data},{thruplay},{view25},{view100},"{matched_tag}"'
                            
                            # Gửi ngay xuống Client
                            yield f"<script>addRow(`{row_str}`);</script>"
                            matched_count += 1
                            total_rows_day += 1

                yield f"<div class='log-line info'> &nbsp;&nbsp; ➡️ Lọc xong trang {page_num}: Lấy được {matched_count} dòng.</div>"

                # Check Next Page
                if 'paging' in data and 'next' in data['paging']:
                    next_url = data['paging']['next']
                    yield f"<div class='log-line'> &nbsp;&nbsp; ⏳ Đang chuyển sang trang {page_num + 1}...</div>"
                    time.sleep(0.5)
                else:
                    break # Hết trang
            
            yield "<div class='log-line success'>---------------------------------------------------</div>"

        # --- HOÀN TẤT & CHUYỂN TRANG ---
        filename = f"Baocao_{start_date_str}.csv"
        
        # Nút tải file & Chuyển trang
        end_date_str = args.get('end', start_date_str)
        current_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
        next_date_obj = current_date_obj + timedelta(days=1)
        
        next_js = ""
        btn_text = "Đã xong ngày này!"
        if next_date_obj <= end_date_obj:
            next_date_str = next_date_obj.strftime("%Y-%m-%d")
            args['start'] = next_date_str
            next_link = request.path + '?' + urlencode(args)
            btn_text = f"Đã xong. Tự chuyển sang {next_date_str}..."
            next_js = f"setTimeout(function() {{ window.location.href = '{next_link}'; }}, 3000);"

        yield f"""
        </div> <div class="download-area">
            <button id="dlBtn" class="btn" onclick="downloadCSV('{filename}')">{btn_text}</button>
        </div>
        <script>
            // Tự động bấm tải
            document.getElementById('dlBtn').click();
            // Tự động chuyển trang
            {next_js}
        </script>
        </body></html>
        """

    return Response(stream_with_context(generate()))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

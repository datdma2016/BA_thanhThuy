import gspread
import requests
import json
import traceback
import shlex
import time
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
    return "<h1>Bot V14: Batch Write & Hacker UI is ready!</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    # Sử dụng stream_with_context để trả về dữ liệu từng dòng (Live Streaming)
    def generate():
        # --- GIAO DIỆN HACKER (CSS) ---
        yield """
        <style>
            body { background-color: #0d1117; color: #58a6ff; font-family: 'Consolas', 'Courier New', monospace; padding: 20px; font-size: 14px; }
            .log { margin-bottom: 5px; border-bottom: 1px dashed #333; padding: 2px 0; }
            .success { color: #2ea043; }
            .error { color: #ff7b72; }
            .warning { color: #d29922; }
            .info { color: #8b949e; }
            .highlight { color: #f0f6fc; font-weight: bold; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; background: #161b22; color: #c9d1d9; }
            th, td { border: 1px solid #30363d; padding: 8px; text-align: left; }
            th { background-color: #21262d; }
            .final-box { background: #161b22; padding: 15px; border: 1px solid #30363d; border-radius: 6px; margin-top: 20px; }
        </style>
        <h3>> KHỞI ĐỘNG HỆ THỐNG QUÉT ADS V14...</h3>
        """
        
        try:
            # --- 1. LẤY THAM SỐ ---
            keyword = request.args.get('keyword', '')
            ten_tab = request.args.get('sheet', 'BaoCaoV14_Batch')
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

            yield f"<div class='log info'>[INIT] Cấu hình: Tab='{ten_tab}' | Time='{thoi_gian_bao_cao}' | Key='{keyword}'</div>"

            # --- 2. KẾT NỐI SHEET ---
            yield f"<div class='log info'>[SHEET] Đang kết nối Google Sheet...</div>"
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
            client = gspread.authorize(creds)
            sh = client.open(FILE_SHEET_GOC)
            
            try:
                worksheet = sh.worksheet(ten_tab)
                yield f"<div class='log success'>[SHEET] Đã tìm thấy tab '{ten_tab}'.</div>"
            except:
                yield f"<div class='log warning'>[SHEET] Chưa có tab. Đang tạo mới...</div>"
                worksheet = sh.add_worksheet(title=ten_tab, rows=100, cols=20)
                header = ["ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", "Thời gian", "Tiền tiêu", "Reach", "Data (Mess+Cmt)", "Giá Data", "Giá trị CĐ", "ROAS"]
                worksheet.append_row(header)

            # --- 3. QUÉT DỮ LIỆU ---
            tong_hop_tk = {}
            # Đây là cái thùng chứa dữ liệu để ghi 1 lần (Batch Write)
            BUFFER_ROWS_TO_WRITE = [] 
            
            fields_list = f'name,status,{time_param}{{spend,reach,actions,action_values,purchase_roas}}'

            for tk_obj in DANH_SACH_TKQC:
                id_tk = tk_obj['id']
                ten_tk = tk_obj['name']
                
                if ten_tk not in tong_hop_tk:
                    tong_hop_tk[ten_tk] = {'id': id_tk, 'spend': 0, 'data': 0, 'revenue': 0, 'reach': 0, 'camp_count': 0}

                yield f"<div class='log info'>[SCAN] Đang quét TK <b>{ten_tk}</b> ({id_tk})...</div>"
                
                base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
                params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 500}
                
                all_campaigns = []
                next_url = base_url
                
                # Vòng lặp lấy full trang
                while True:
                    try:
                        res = requests.get(next_url, params=params if next_url == base_url else None)
                        data = res.json()
                        if 'error' in data:
                            yield f"<div class='log error'>[ERROR] Lỗi TK {ten_tk}: {data['error']['message']}</div>"
                            break
                        all_campaigns.extend(data.get('data', []))
                        if 'paging' in data and 'next' in data['paging']:
                            next_url = data['paging']['next']
                        else: break
                    except Exception as e:
                        yield f"<div class='log error'>[ERROR] Lỗi kết nối: {str(e)}</div>"
                        break

                count_camp_tk = 0
                
                for camp in all_campaigns:
                    ten_camp = camp.get('name', 'Không tên')
                    trang_thai = camp.get('status', 'UNKNOWN')
                    
                    if check_keyword_v12(ten_camp, keyword):
                        insights_data = camp.get('insights', {}).get('data', [])
                        
                        if insights_data:
                            stat = insights_data[0]
                            spend = float(stat.get('spend', 0))
                            reach = int(stat.get('reach', 0))
                            
                            actions = stat.get('actions', [])
                            cmts = get_fb_value(actions, ['comment'])
                            msgs = get_fb_value(actions, ['onsite_conversion.messaging_conversation_started_7d', 'messaging_conversation_started_7d'])
                            total_data = cmts + msgs
                            
                            action_values = stat.get('action_values', [])
                            revenue = get_fb_value(action_values, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                            
                            gia_data = round(spend / total_data) if total_data > 0 else 0
                            roas = (revenue / spend) if spend > 0 else 0

                            if spend > 0:
                                # Thay vì ghi ngay, ta thêm vào bộ nhớ đệm (Buffer)
                                row = [id_tk, ten_tk, ten_camp, trang_thai, thoi_gian_bao_cao, spend, reach, total_data, gia_data, revenue, roas]
                                BUFFER_ROWS_TO_WRITE.append(row)
                                
                                # Cộng tổng
                                tong_hop_tk[ten_tk]['spend'] += spend
                                tong_hop_tk[ten_tk]['data'] += total_data
                                tong_hop_tk[ten_tk]['revenue'] += revenue
                                tong_hop_tk[ten_tk]['reach'] += reach
                                tong_hop_tk[ten_tk]['camp_count'] += 1
                                count_camp_tk += 1
                
                yield f"<div class='log success'>[DONE] TK {ten_tk}: Tìm thấy {count_camp_tk} camp.</div>"
                # Scroll xuống dưới cùng
                yield "<script>window.scrollTo(0, document.body.scrollHeight);</script>"

            # --- 4. GHI DỮ LIỆU VÀO SHEET (BATCH WRITE) ---
            if len(BUFFER_ROWS_TO_WRITE) > 0:
                yield f"<div class='log warning'>[WRITE] Đang ghi {len(BUFFER_ROWS_TO_WRITE)} dòng vào Sheet (Vui lòng chờ)...</div>"
                try:
                    worksheet.append_rows(BUFFER_ROWS_TO_WRITE)
                    yield f"<div class='log success'>[SUCCESS] Đã ghi xong toàn bộ dữ liệu! Không bị lỗi Quota.</div>"
                except Exception as e:
                    yield f"<div class='log error'>[FATAL] Lỗi khi ghi Sheet: {str(e)}</div>"
            else:
                yield f"<div class='log info'>[INFO] Không có dữ liệu nào để ghi.</div>"

            # --- 5. HIỂN THỊ TỔNG HỢP ---
            html_summary = "<table>"
            html_summary += "<tr><th>Tên TK</th><th>Camp</th><th>Tiêu (VNĐ)</th><th>Data</th><th>Giá Data</th><th>Doanh Thu</th><th>ROAS</th></tr>"
            
            grand_spend = 0
            grand_rev = 0
            
            for ten, val in tong_hop_tk.items():
                gia_data_tb = round(val['spend'] / val['data']) if val['data'] > 0 else 0
                roas_tb = (val['revenue'] / val['spend']) if val['spend'] > 0 else 0
                grand_spend += val['spend']
                grand_rev += val['revenue']
                
                html_summary += f"""
                <tr>
                    <td><span class='highlight'>{ten}</span><br><small style='color:#8b949e'>{val['id']}</small></td>
                    <td align='center'>{val['camp_count']}</td>
                    <td align='right'>{fmt_vn(val['spend'])}</td>
                    <td align='center'>{fmt_vn(val['data'])}</td>
                    <td align='right'>{fmt_vn(gia_data_tb)}</td>
                    <td align='right'>{fmt_vn(val['revenue'])}</td>
                    <td align='center' class='highlight'>{roas_tb:.2f}</td>
                </tr>
                """
            html_summary += "</table>"
            
            yield f"""
            <div class='final-box'>
                <h2 style='color:#58a6ff; border-bottom:1px solid #30363d; padding-bottom:10px'>BÁO CÁO HOÀN TẤT</h2>
                <p>💰 TỔNG TIÊU: <span style='color:#ff7b72; font-size:18px'>{fmt_vn(grand_spend)} VNĐ</span></p>
                <p>💎 DOANH THU: <span style='color:#2ea043; font-size:18px'>{fmt_vn(grand_rev)} VNĐ</span></p>
                {html_summary}
            </div>
            <script>window.scrollTo(0, document.body.scrollHeight);</script>
            """

        except Exception as e:
             yield f"<div class='log error'>[CRASH] Hệ thống gặp lỗi nghiêm trọng: {traceback.format_exc()}</div>"

    return Response(stream_with_context(generate()))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

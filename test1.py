import gspread
import requests
import json
import traceback # Để in lỗi chi tiết hơn
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)

# ======================================================
# 1. CẤU HÌNH (SẾP KIỂM TRA KỸ)
# ======================================================

FB_ACCESS_TOKEN = "EAANPvsZANh38BQt8Bcqztr63LDZBieQxO2h5TnOIGpHQtlOnV85cwg7I2ZCVf8vFTccpbB7hX97HYOsGFEKLD3fSZC2BCyKWeZA0vsUJZCXBZAMVZARMwZCvTuPGTsIStG5ro10ltZBXs3yTOzBLjZAjfL8TAeXwgKC73ZBZA3aQD6eludndMkOYFrVCFv2CrIrNe5nX82FScL0TzIXjA7qUl9HZAz" 

DANH_SACH_TKQC = [
    "581662847745376",
    "1934563933738877",
    "995686602001085"
]

FILE_SHEET_GOC = "pancakeTest_260120"

# ======================================================

def log_system(logs, message, type="INFO"):
    """Hàm ghi log thông báo"""
    time_now = datetime.now().strftime("%H:%M:%S")
    color = "black"
    if type == "SUCCESS": color = "green"
    elif type == "ERROR": color = "red"
    elif type == "WARNING": color = "orange"
    
    entry = f"<li style='color:{color}'><b>[{time_now}] [{type}]</b>: {message}</li>"
    logs.append(entry)

def get_val_from_list(data_list, key_target, value_key='value'):
    """Hàm đào dữ liệu trong list actions của Facebook"""
    # Ví dụ data_list = [{'action_type': 'comment', 'value': 10}, ...]
    if not data_list: return 0
    total = 0
    for item in data_list:
        if item.get('action_type') == key_target:
            total += float(item.get(value_key, 0))
    return total

def ket_noi_sheet_tab(logs, ten_tab_moi):
    try:
        log_system(logs, "Bắt đầu kết nối Google Sheet...", "INFO")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        sh = client.open(FILE_SHEET_GOC)
        
        try:
            worksheet = sh.worksheet(ten_tab_moi)
            log_system(logs, f"Đã tìm thấy tab '{ten_tab_moi}'. Sẽ ghi tiếp dữ liệu.", "SUCCESS")
        except:
            log_system(logs, f"Chưa có tab '{ten_tab_moi}'. Đang tạo mới...", "WARNING")
            worksheet = sh.add_worksheet(title=ten_tab_moi, rows=100, cols=20)
            # Tạo Header chuẩn sếp yêu cầu
            header = [
                "ID TK", "Tên Chiến Dịch", "Trạng thái", "Thời gian", 
                "Tiền tiêu (VNĐ)", "Reach", "Mess+Cmt (Data)", "Giá Data", 
                "Giá trị CĐ (VNĐ)", "ROAS"
            ]
            worksheet.append_row(header)
            
        return worksheet
    except Exception as e:
        log_system(logs, f"Lỗi kết nối Sheet: {str(e)}", "ERROR")
        return None

@app.route('/')
def home():
    return "<h1>Bot V9: Dashboard Tỷ Tỉ - Log chi tiết & Full Metrics</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    logs = [] # Danh sách chứa nhật ký
    tong_hop_tk = {} # Dictionary để sum theo từng TKQC
    
    try:
        # --- 1. LẤY THAM SỐ ---
        keyword = request.args.get('keyword', '')
        ten_tab = request.args.get('sheet', 'BaoCaoV9')
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        date_preset = request.args.get('date', 'today')

        # Xử lý thời gian an toàn bằng json
        if start_date and end_date:
            range_dict = {'since': start_date, 'until': end_date}
            time_param = f'insights.time_range({json.dumps(range_dict)})'
            thoi_gian_bao_cao = f"{start_date} đến {end_date}"
        else:
            time_param = f'insights.date_preset({date_preset})'
            thoi_gian_bao_cao = date_preset

        log_system(logs, f"Cấu hình báo cáo: Tab={ten_tab} | Time={thoi_gian_bao_cao} | Key={keyword}", "INFO")

        # --- 2. KẾT NỐI SHEET ---
        sheet = ket_noi_sheet_tab(logs, ten_tab)
        if not sheet: return "<h3>Lỗi kết nối Sheet. Xem log bên dưới.</h3>" + "".join(logs)

        # --- 3. QUÉT DỮ LIỆU ---
        ket_qua_hien_thi = []
        
        # Các trường dữ liệu cần lấy (Thêm actions, action_values để tính mess, roas)
        fields_list = f'name,status,{time_param}{{spend,reach,actions,action_values}}'

        for id_tk in DANH_SACH_TKQC:
            log_system(logs, f"Đang quét TK: {id_tk}...", "INFO")
            
            # Khởi tạo biến tổng cho TK này
            tong_hop_tk[id_tk] = {
                'spend': 0, 'data': 0, 'revenue': 0, 'reach': 0, 'camp_count': 0
            }

            base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
            params = {
                'fields': fields_list,
                'access_token': FB_ACCESS_TOKEN,
                'limit': 500,
            }
            
            # --- THUẬT TOÁN LẬT TRANG ---
            all_campaigns = []
            next_url = base_url
            trang_thu = 1
            
            while True:
                try:
                    if trang_thu == 1:
                        response = requests.get(next_url, params=params)
                    else:
                        response = requests.get(next_url)
                    
                    data = response.json()
                    
                    if 'error' in data:
                        log_system(logs, f"Lỗi API TK {id_tk}: {data['error']['message']}", "ERROR")
                        break
                    
                    batch_data = data.get('data', [])
                    all_campaigns.extend(batch_data)
                    
                    if 'paging' in data and 'next' in data['paging']:
                        next_url = data['paging']['next']
                        trang_thu += 1
                    else:
                        break 
                except Exception as e:
                    log_system(logs, f"Lỗi lật trang TK {id_tk}: {str(e)}", "ERROR")
                    break

            # --- XỬ LÝ & TÍNH TOÁN METRICS ---
            count_camp_tk = 0
            
            for camp in all_campaigns:
                ten_camp = camp.get('name', 'Không tên')
                trang_thai = camp.get('status', 'UNKNOWN')
                
                if keyword.lower() in ten_camp.lower():
                    insights_data = camp.get('insights', {}).get('data', [])
                    
                    if insights_data:
                        stat = insights_data[0]
                        
                        # 1. Lấy chỉ số cơ bản
                        spend = float(stat.get('spend', 0))
                        reach = int(stat.get('reach', 0))
                        
                        # 2. Tính Data (Mess + Comment)
                        actions = stat.get('actions', [])
                        cmts = get_val_from_list(actions, 'comment')
                        # 'onsite_conversion.messaging_conversation_started_7d' là metric chuẩn cho tin nhắn mới
                        msgs = get_val_from_list(actions, 'onsite_conversion.messaging_conversation_started_7d')
                        total_data = cmts + msgs
                        
                        # 3. Tính Doanh thu (Purchase Value) & ROAS
                        action_values = stat.get('action_values', [])
                        # 'purchase' hoặc 'omni_purchase' (tùy tk, thường dùng purchase ok)
                        revenue = get_val_from_list(action_values, 'purchase') 
                        
                        # 4. Tính toán
                        gia_data = (spend / total_data) if total_data > 0 else 0
                        roas = (revenue / spend) if spend > 0 else 0

                        if spend > 0:
                            # Ghi vào Sheet
                            row = [
                                id_tk, ten_camp, trang_thai, thoi_gian_bao_cao, 
                                spend, reach, total_data, gia_data, revenue, roas
                            ]
                            sheet.append_row(row)
                            
                            # Cộng dồn vào tổng TK
                            tong_hop_tk[id_tk]['spend'] += spend
                            tong_hop_tk[id_tk]['data'] += total_data
                            tong_hop_tk[id_tk]['revenue'] += revenue
                            tong_hop_tk[id_tk]['reach'] += reach
                            tong_hop_tk[id_tk]['camp_count'] += 1
                            count_camp_tk += 1

                            ket_qua_hien_thi.append(
                                f"<li>[{id_tk}] {ten_camp}: {spend:,.0f}đ | Data: {total_data}</li>"
                            )
            
            log_system(logs, f"Hoàn thành TK {id_tk}. Tìm thấy {count_camp_tk} camp có tiêu tiền.", "SUCCESS")

        # --- TẠO BẢNG TỔNG HỢP HTML ---
        html_summary = "<table border='1' cellpadding='5' style='border-collapse:collapse; width:100%'>"
        html_summary += "<tr style='background:#f2f2f2'><th>ID TK</th><th>Camp</th><th>Tiêu (VNĐ)</th><th>Data (Mess+Cmt)</th><th>Giá Data</th><th>ROAS</th></tr>"
        
        grand_total_spend = 0
        
        for tk, val in tong_hop_tk.items():
            gia_data_tb = (val['spend'] / val['data']) if val['data'] > 0 else 0
            roas_tb = (val['revenue'] / val['spend']) if val['spend'] > 0 else 0
            grand_total_spend += val['spend']
            
            html_summary += f"""
            <tr>
                <td>{tk}</td>
                <td style='text-align:center'>{val['camp_count']}</td>
                <td style='text-align:right'>{val['spend']:,.0f}</td>
                <td style='text-align:center'>{val['data']:,.0f}</td>
                <td style='text-align:right'>{gia_data_tb:,.0f}</td>
                <td style='text-align:center'>{roas_tb:.2f}</td>
            </tr>
            """
        html_summary += "</table>"

        # --- TRẢ VỀ KẾT QUẢ ---
        html = f"""
        <h2>BÁO CÁO HIỆU QUẢ QUẢNG CÁO (V9)</h2>
        <p><b>Thời gian:</b> {thoi_gian_bao_cao} | <b>Tổng tiêu:</b> <span style="color:red; font-size:20px">{grand_total_spend:,.0f} VNĐ</span></p>
        
        <h3>1. Tổng hợp theo Tài khoản (Group Sum)</h3>
        {html_summary}
        
        <h3>2. Nhật ký hệ thống (System Logs)</h3>
        <ul style="background:#eee; padding:10px; border:1px solid #ccc; max-height:200px; overflow-y:scroll">
            {''.join(logs)}
        </ul>
        
        <h3>3. Chi tiết chiến dịch</h3>
        <ul>{''.join(ket_qua_hien_thi)}</ul>
        """
        return html

    except Exception as e:
        # Nếu lỗi sập nguồn thì in traceback ra xem cho sướng
        err_msg = traceback.format_exc()
        return f"<h3>Hệ thống gặp lỗi Fatal!</h3><pre>{err_msg}</pre>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

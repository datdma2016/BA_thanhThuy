import gspread
import requests
import json
import traceback 
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)

# ======================================================
# 1. CẤU HÌNH (SẾP ĐIỀN LẠI TOKEN & ID)
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
    """Ghi log hệ thống"""
    time_now = datetime.now().strftime("%H:%M:%S")
    color = "black"
    if type == "SUCCESS": color = "green"
    elif type == "ERROR": color = "red"
    elif type == "WARNING": color = "orange"
    logs.append(f"<li style='color:{color}'><b>[{time_now}] [{type}]</b>: {message}</li>")

def fmt_vn(value):
    """
    Hàm định dạng số kiểu Việt Nam: 
    1000 -> 1.000
    Làm tròn số nguyên, ngăn cách bằng dấu chấm.
    """
    if not value: return "0"
    try:
        # Làm tròn thành số nguyên, sau đó format phẩy, rồi thay phẩy thành chấm
        return "{:,.0f}".format(value).replace(",", ".")
    except:
        return str(value)

def get_fb_value(data_list, keys_target, value_key='value'):
    """
    Hàm đào dữ liệu thông minh.
    keys_target: Có thể là 1 list các từ khóa (ví dụ: ['purchase', 'omni_purchase'])
    Nó sẽ tìm ưu tiên từ trái sang phải.
    """
    if not data_list: return 0
    
    # Duyệt qua từng key ưu tiên
    for k in keys_target:
        for item in data_list:
            if item.get('action_type') == k:
                return float(item.get(value_key, 0))
    return 0

def ket_noi_sheet_tab(logs, ten_tab_moi):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        sh = client.open(FILE_SHEET_GOC)
        
        try:
            worksheet = sh.worksheet(ten_tab_moi)
            log_system(logs, f"Tab '{ten_tab_moi}' đã có sẵn.", "SUCCESS")
        except:
            log_system(logs, f"Tạo mới tab '{ten_tab_moi}'...", "WARNING")
            worksheet = sh.add_worksheet(title=ten_tab_moi, rows=100, cols=20)
            header = [
                "ID TK", "Tên Chiến Dịch", "Trạng thái", "Thời gian", 
                "Tiền tiêu", "Reach", "Data (Mess+Cmt)", "Giá Data", 
                "Giá trị CĐ", "ROAS"
            ]
            worksheet.append_row(header)
        return worksheet
    except Exception as e:
        log_system(logs, f"Lỗi Sheet: {str(e)}", "ERROR")
        return None

@app.route('/')
def home():
    return "<h1>Bot V10: Visual Chuẩn Việt Nam & Fix Doanh Thu</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    logs = []
    tong_hop_tk = {}
    
    try:
        # --- 1. LẤY THAM SỐ ---
        keyword = request.args.get('keyword', '')
        ten_tab = request.args.get('sheet', 'BaoCaoV10')
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

        # --- 2. KẾT NỐI SHEET ---
        sheet = ket_noi_sheet_tab(logs, ten_tab)
        if not sheet: return "Lỗi kết nối Sheet"

        # --- 3. QUÉT DỮ LIỆU ---
        ket_qua_hien_thi = []
        
        # Thêm purchase_roas vào fields (đề phòng FB có sẵn)
        fields_list = f'name,status,{time_param}{{spend,reach,actions,action_values,purchase_roas}}'

        for id_tk in DANH_SACH_TKQC:
            tong_hop_tk[id_tk] = {'spend': 0, 'data': 0, 'revenue': 0, 'reach': 0, 'camp_count': 0}
            
            base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
            params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 500}
            
            # --- LẬT TRANG (PAGINATION) ---
            all_campaigns = []
            next_url = base_url
            
            while True:
                try:
                    res = requests.get(next_url, params=params if next_url == base_url else None)
                    data = res.json()
                    if 'error' in data:
                        log_system(logs, f"Lỗi TK {id_tk}: {data['error']['message']}", "ERROR")
                        break
                    
                    all_campaigns.extend(data.get('data', []))
                    
                    if 'paging' in data and 'next' in data['paging']:
                        next_url = data['paging']['next']
                    else: break
                except: break

            # --- TÍNH TOÁN CHI TIẾT ---
            count_camp_tk = 0
            
            for camp in all_campaigns:
                ten_camp = camp.get('name', 'Không tên')
                trang_thai = camp.get('status', 'UNKNOWN')
                
                if keyword.lower() in ten_camp.lower():
                    insights_data = camp.get('insights', {}).get('data', [])
                    
                    if insights_data:
                        stat = insights_data[0]
                        spend = float(stat.get('spend', 0))
                        reach = int(stat.get('reach', 0))
                        
                        # Data = Comment + Message
                        actions = stat.get('actions', [])
                        cmts = get_fb_value(actions, ['comment'])
                        msgs = get_fb_value(actions, ['onsite_conversion.messaging_conversation_started_7d', 'messaging_conversation_started_7d'])
                        total_data = cmts + msgs
                        
                        # DOANH THU (Purchase Value)
                        # Tìm ưu tiên: 'purchase' -> 'omni_purchase' -> 'offsite_conversion.fb_pixel_purchase'
                        action_values = stat.get('action_values', [])
                        revenue = get_fb_value(action_values, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase'])
                        
                        # TÍNH TOÁN
                        # Giá Data (Làm tròn)
                        gia_data = round(spend / total_data) if total_data > 0 else 0
                        
                        # ROAS (Lấy Revenue / Spend cho chuẩn)
                        roas = (revenue / spend) if spend > 0 else 0

                        if spend > 0:
                            # Ghi vào Sheet (Giữ số thô để sếp còn tính toán trong Excel)
                            row = [id_tk, ten_camp, trang_thai, thoi_gian_bao_cao, spend, reach, total_data, gia_data, revenue, roas]
                            sheet.append_row(row)
                            
                            # Cộng tổng
                            tong_hop_tk[id_tk]['spend'] += spend
                            tong_hop_tk[id_tk]['data'] += total_data
                            tong_hop_tk[id_tk]['revenue'] += revenue
                            tong_hop_tk[id_tk]['reach'] += reach
                            tong_hop_tk[id_tk]['camp_count'] += 1
                            count_camp_tk += 1

                            # Hiển thị Web (Đẹp long lanh)
                            ket_qua_hien_thi.append(
                                f"<li>[{id_tk}] {ten_camp}: <b>{fmt_vn(spend)}đ</b> | Data: {fmt_vn(total_data)} | Giá số: {fmt_vn(gia_data)}đ | Rev: {fmt_vn(revenue)}đ</li>"
                            )
            
            log_system(logs, f"TK {id_tk}: Xong {count_camp_tk} camp.", "SUCCESS")

        # --- HTML DASHBOARD (FORMAT VIỆT NAM) ---
        html_summary = "<table border='1' cellpadding='5' style='border-collapse:collapse; width:100%'>"
        html_summary += "<tr style='background:#f2f2f2'><th>ID TK</th><th>Camp</th><th>Tiêu (VNĐ)</th><th>Data</th><th>Giá Data</th><th>Doanh Thu</th><th>ROAS</th></tr>"
        
        grand_spend = 0
        grand_rev = 0
        
        for tk, val in tong_hop_tk.items():
            gia_data_tb = round(val['spend'] / val['data']) if val['data'] > 0 else 0
            roas_tb = (val['revenue'] / val['spend']) if val['spend'] > 0 else 0
            grand_spend += val['spend']
            grand_rev += val['revenue']
            
            html_summary += f"""
            <tr>
                <td>{tk}</td>
                <td align='center'>{val['camp_count']}</td>
                <td align='right'>{fmt_vn(val['spend'])}</td>
                <td align='center'>{fmt_vn(val['data'])}</td>
                <td align='right'>{fmt_vn(gia_data_tb)}</td>
                <td align='right'>{fmt_vn(val['revenue'])}</td>
                <td align='center'><b>{roas_tb:.2f}</b></td>
            </tr>
            """
        
        roas_tong = (grand_rev / grand_spend) if grand_spend > 0 else 0
        
        html = f"""
        <style>body{{font-family:Arial, sans-serif;}} table{{width:100%;}} td,th{{padding:8px;}}</style>
        <h2>DASHBOARD BÁO CÁO (V10)</h2>
        <p>
            ⏱ <b>Thời gian:</b> {thoi_gian_bao_cao}<br>
            💰 <b>Tổng tiêu:</b> <span style="color:red; font-size:18px">{fmt_vn(grand_spend)} VNĐ</span><br>
            💎 <b>Tổng Doanh thu:</b> <span style="color:green; font-size:18px">{fmt_vn(grand_rev)} VNĐ</span><br>
            📈 <b>ROAS Tổng:</b> {roas_tong:.2f}
        </p>
        
        <h3>1. Tổng quan (Group by ID)</h3>
        {html_summary}
        
        <h3>2. Chi tiết (Detail)</h3>
        <ul style='font-size:14px'>{''.join(ket_qua_hien_thi)}</ul>
        
        <h3>3. Logs</h3>
        <div style="background:#eee; padding:5px; height:150px; overflow-y:scroll; border:1px solid #ddd">
            <ul>{''.join(logs)}</ul>
        </div>
        """
        return html

    except Exception as e:
        return f"ERROR: {traceback.format_exc()}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

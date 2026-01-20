import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)

# ======================================================
# KHU VỰC CẤU HÌNH
# ======================================================

FB_ACCESS_TOKEN = "EAANPvsZANh38BQt8Bcqztr63LDZBieQxO2h5TnOIGpHQtlOnV85cwg7I2ZCVf8vFTccpbB7hX97HYOsGFEKLD3fSZC2BCyKWeZA0vsUJZCXBZAMVZARMwZCvTuPGTsIStG5ro10ltZBXs3yTOzBLjZAjfL8TAeXwgKC73ZBZA3aQD6eludndMkOYFrVCFv2CrIrNe5nX82FScL0TzIXjA7qUl9HZAz" 

DANH_SACH_TKQC = [
    "581662847745376",
    "1934563933738877",
    "995686602001085"
]

FILE_SHEET_GOC = "pancakeTest_260120"

# ======================================================

def ket_noi_sheet_tab(ten_tab_moi):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    sh = client.open(FILE_SHEET_GOC)
    try:
        worksheet = sh.worksheet(ten_tab_moi)
    except:
        worksheet = sh.add_worksheet(title=ten_tab_moi, rows=100, cols=20)
        worksheet.append_row(["ID TK", "Tên Chiến Dịch", "Trạng thái", "Thời gian", "Tiền tiêu (VNĐ)", "Clicks", "CTR (%)"])
    return worksheet

@app.route('/')
def home():
    return "<h1>Bot V6: Đã sửa lỗi Invalid Parameter!</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    try:
        # --- THAM SỐ TỪ LINK ---
        keyword = request.args.get('keyword', '')
        ten_tab = request.args.get('sheet', 'BaoCaoV6_Fix')
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        date_preset = request.args.get('date', 'today')

        # --- XỬ LÝ THỜI GIAN ---
        if start_date and end_date:
            time_param = f"insights.time_range({{'since':'{start_date}','until':'{end_date}'}})"
            thoi_gian_bao_cao = f"{start_date} đến {end_date}"
        else:
            time_param = f"insights.date_preset({date_preset})"
            thoi_gian_bao_cao = date_preset

        # --- BẮT ĐẦU QUÉT ---
        ket_qua_hien_thi = []
        nhat_ky_quet = [] 
        tong_tien_all = 0
        
        sheet = ket_noi_sheet_tab(ten_tab)
        
        for id_tk in DANH_SACH_TKQC:
            base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
            
            # Cấu hình lấy dữ liệu
            fields = f'name,status,{time_param}{{spend,impressions,clicks,cpc,ctr}}'
            
            # ĐÃ SỬA: Xóa 'level' và chuẩn hóa effective_status
            params = {
                'fields': fields,
                'access_token': FB_ACCESS_TOKEN,
                'limit': 500,
                'effective_status': '["ACTIVE","PAUSED","DELETED","ARCHIVED"]',
            }
            
            # --- THUẬT TOÁN LẬT TRANG (GIỮ NGUYÊN) ---
            all_campaigns = []
            next_url = base_url
            trang_thu = 1
            
            while True:
                if trang_thu == 1:
                    response = requests.get(next_url, params=params)
                else:
                    response = requests.get(next_url) # Trang sau dùng link có sẵn
                
                data = response.json()
                
                if 'error' in data:
                    nhat_ky_quet.append(f"<li style='color:red'>TK {id_tk} lỗi: {data['error']['message']}</li>")
                    break
                
                batch_data = data.get('data', [])
                all_campaigns.extend(batch_data)
                
                if 'paging' in data and 'next' in data['paging']:
                    next_url = data['paging']['next']
                    trang_thu += 1
                else:
                    break 
            
            # --- TÍNH TOÁN ---
            dem_camp_co_tien = 0
            
            for camp in all_campaigns:
                ten_camp = camp.get('name', 'Không tên')
                trang_thai = camp.get('status', 'UNKNOWN')
                
                if keyword.lower() in ten_camp.lower():
                    insights_data = camp.get('insights', {}).get('data', [])
                    
                    if insights_data:
                        stat = insights_data[0]
                        spend = float(stat.get('spend', 0))
                        clicks = stat.get('clicks', 0)
                        ctr = float(stat.get('ctr', 0))
                        
                        if spend > 0:
                            row = [id_tk, ten_camp, trang_thai, thoi_gian_bao_cao, spend, clicks, ctr]
                            sheet.append_row(row)
                            tong_tien_all += spend
                            dem_camp_co_tien += 1
                            ket_qua_hien_thi.append(f"<li>[{id_tk}] {ten_camp}: {spend:,.0f}đ</li>")
            
            nhat_ky_quet.append(f"<li style='color:blue'>TK {id_tk}: Quét xong {len(all_campaigns)} camp (qua {trang_thu} trang). Lấy được {dem_camp_co_tien} camp có tiền.</li>")

        html = f"""
        <h3>Báo cáo V6 (Đã Fix Lỗi)</h3>
        <ul>
            <li><b>Tổng tiền chuẩn:</b> <span style="color:red; font-size:24px; font-weight:bold">{tong_tien_all:,.0f} VNĐ</span></li>
            <li><b>Thời gian:</b> {thoi_gian_bao_cao}</li>
        </ul>
        <hr>
        <h4>Nhật ký hệ thống:</h4>
        <ul>{''.join(nhat_ky_quet)}</ul>
        <hr>
        <h4>Chi tiết:</h4>
        <ul>{''.join(ket_qua_hien_thi)}</ul>
        """
        return html

    except Exception as e:
        return f"Lỗi: {str(e)}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

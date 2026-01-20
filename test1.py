import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)

# ======================================================
# KHU VỰC CẤU HÌNH (SẾP KIỂM TRA LẠI ID Ở ĐÂY NHÉ)
# ======================================================

FB_ACCESS_TOKEN = "EAANPvsZANh38BQt8Bcqztr63LDZBieQxO2h5TnOIGpHQtlOnV85cwg7I2ZCVf8vFTccpbB7hX97HYOsGFEKLD3fSZC2BCyKWeZA0vsUJZCXBZAMVZARMwZCvTuPGTsIStG5ro10ltZBXs3yTOzBLjZAjfL8TAeXwgKC73ZBZA3aQD6eludndMkOYFrVCFv2CrIrNe5nX82FScL0TzIXjA7qUl9HZAz" 

# Danh sách ID Tài khoản (Nhập ID số, để trong ngoặc kép)
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
    return "<h1>Bot V4: Đã mở full camp (kể cả camp tắt)!</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    try:
        # --- LẤY THAM SỐ TỪ LINK ---
        keyword = request.args.get('keyword', '')
        ten_tab = request.args.get('sheet', 'BaoCaoV4')
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
        nhat_ky_quet = [] # Để debug xem TK nào bị lỗi
        tong_tien_all = 0
        
        sheet = ket_noi_sheet_tab(ten_tab)
        
        for id_tk in DANH_SACH_TKQC:
            url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
            
            # Lấy thêm trường 'status' để biết camp đang bật hay tắt
            fields_string = f'name,status,{time_param}{{spend,impressions,clicks,cpc,ctr}}'
            
            params = {
                'fields': fields_string,
                'access_token': FB_ACCESS_TOKEN,
                'limit': 500, # Tăng lên 500 để không bị sót
                # ĐÃ XÓA DÒNG effective_status ĐỂ LẤY HẾT
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            # --- KIỂM TRA LỖI TỪNG TÀI KHOẢN ---
            if 'error' in data:
                loi = data['error']['message']
                nhat_ky_quet.append(f"<li style='color:red'>TK {id_tk}: LỖI - {loi}</li>")
                continue

            campaigns = data.get('data', [])
            dem_camp_tk = 0

            for camp in campaigns:
                ten_camp = camp.get('name', 'Không tên')
                trang_thai = camp.get('status', 'UNKNOWN') # ACTIVE hoặc PAUSED
                
                # Logic lọc từ khóa (Nếu không nhập keyword thì luôn đúng)
                if keyword.lower() in ten_camp.lower():
                    insights_data = camp.get('insights', {}).get('data', [])
                    
                    if insights_data:
                        stat = insights_data[0]
                        spend = float(stat.get('spend', 0))
                        clicks = stat.get('clicks', 0)
                        ctr = float(stat.get('ctr', 0))
                        
                        # CHỈ LẤY NẾU CÓ TIÊU TIỀN (Dù tắt hay bật)
                        if spend > 0:
                            row = [id_tk, ten_camp, trang_thai, thoi_gian_bao_cao, spend, clicks, ctr]
                            sheet.append_row(row)
                            tong_tien_all += spend
                            dem_camp_tk += 1
                            ket_qua_hien_thi.append(f"<li>[{id_tk}] {ten_camp} ({trang_thai}): {spend:,.0f}đ</li>")
            
            nhat_ky_quet.append(f"<li style='color:green'>TK {id_tk}: Quét xong. Lấy được {dem_camp_tk} camp có tiêu tiền.</li>")

        html = f"""
        <h3>Báo cáo V4 hoàn tất!</h3>
        <ul>
            <li><b>Tab Sheet:</b> {ten_tab}</li>
            <li><b>Thời gian:</b> {thoi_gian_bao_cao}</li>
            <li><b>Từ khóa:</b> "{keyword}"</li>
            <li><b>Tổng tiền:</b> <span style="color:red; font-size:20px">{tong_tien_all:,.0f} VNĐ</span></li>
        </ul>
        <hr>
        <h4>Nhật ký quét (Kiểm tra xem có thiếu TK nào không):</h4>
        <ul>{''.join(nhat_ky_quet)}</ul>
        <hr>
        <h4>Chi tiết chiến dịch:</h4>
        <ul>{''.join(ket_qua_hien_thi)}</ul>
        """
        return html

    except Exception as e:
        return f"Lỗi hệ thống: {str(e)}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

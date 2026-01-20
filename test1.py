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
        worksheet.append_row(["ID TK", "Tên Chiến Dịch", "Thời gian", "Tiền tiêu (VNĐ)", "Clicks", "CTR (%)"])
    return worksheet

@app.route('/')
def home():
    return "<h1>Bot Báo Cáo V3: Đã hỗ trợ Custom Date Range!</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    try:
        # --- LẤY THAM SỐ TỪ LINK ---
        keyword = request.args.get('keyword', '')
        ten_tab = request.args.get('sheet', 'BaoCaoTuyChinh')
        
        # Lấy ngày tùy chỉnh
        start_date = request.args.get('start') # Định dạng YYYY-MM-DD
        end_date = request.args.get('end')     # Định dạng YYYY-MM-DD
        date_preset = request.args.get('date', 'today')

        # --- XỬ LÝ LOGIC THỜI GIAN ---
        # Nếu sếp nhập cả ngày bắt đầu và kết thúc -> Dùng Custom Range
        if start_date and end_date:
            time_param = f"insights.time_range({{'since':'{start_date}','until':'{end_date}'}})"
            thoi_gian_bao_cao = f"{start_date} đến {end_date}"
        else:
            # Nếu không nhập -> Dùng Preset (Hôm nay, hôm qua...)
            time_param = f"insights.date_preset({date_preset})"
            thoi_gian_bao_cao = date_preset

        # --- BẮT ĐẦU QUÉT ---
        ket_qua_hien_thi = []
        tong_tien_all = 0
        dem_camp = 0
        
        sheet = ket_noi_sheet_tab(ten_tab)
        
        for id_tk in DANH_SACH_TKQC:
            url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
            
            # Ghép cái time_param đã xử lý ở trên vào chuỗi fields
            fields_string = f'name,status,{time_param}{{spend,impressions,clicks,cpc,ctr}}'
            
            params = {
                'fields': fields_string,
                'access_token': FB_ACCESS_TOKEN,
                'limit': 50,
                'effective_status': '["ACTIVE"]'
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if 'error' in data:
                ket_qua_hien_thi.append(f"<li>Lỗi TK {id_tk}: {data['error']['message']}</li>")
                continue

            campaigns = data.get('data', [])

            for camp in campaigns:
                ten_camp = camp.get('name', 'Không tên')
                
                if keyword.lower() in ten_camp.lower():
                    insights_data = camp.get('insights', {}).get('data', [])
                    
                    if insights_data:
                        stat = insights_data[0]
                        spend = float(stat.get('spend', 0))
                        clicks = stat.get('clicks', 0)
                        ctr = float(stat.get('ctr', 0))
                        
                        if spend > 0:
                            row = [id_tk, ten_camp, thoi_gian_bao_cao, spend, clicks, ctr]
                            sheet.append_row(row)
                            tong_tien_all += spend
                            dem_camp += 1
                            ket_qua_hien_thi.append(f"<li>[{id_tk}] {ten_camp}: {spend:,.0f}đ</li>")

        html = f"""
        <h3>Báo cáo tùy chỉnh hoàn tất!</h3>
        <ul>
            <li><b>Tab Sheet:</b> {ten_tab}</li>
            <li><b>Thời gian:</b> {thoi_gian_bao_cao}</li>
            <li><b>Từ khóa:</b> "{keyword}"</li>
            <li><b>Tổng tiền:</b> <span style="color:red">{tong_tien_all:,.0f} VNĐ</span></li>
        </ul>
        <hr>
        <ul>{''.join(ket_qua_hien_thi)}</ul>
        """
        return html

    except Exception as e:
        return f"Lỗi: {str(e)}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

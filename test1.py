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
    return "<h1>Bot V7 (Final): Chế độ 'Xe Ủi' - Quét sạch không chừa một ai!</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    try:
        # --- THAM SỐ TỪ LINK ---
        keyword = request.args.get('keyword', '')
        ten_tab = request.args.get('sheet', 'BaoCaoV7_Final')
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        date_preset = request.args.get('date', 'today')

        # --- XỬ LÝ THỜI GIAN (Sửa lại dấu nháy kép cho chuẩn JSON) ---
        if start_date and end_date:
            # Dùng dấu nháy kép " thay vì nháy đơn ' để Facebook dễ đọc
            time_param = f'insights.time_range({{"since":"{start_date}","until":"{end_date}"}})'
            thoi_gian_bao_cao = f"{start_date} đến {end_date}"
        else:
            time_param = f'insights.date_preset({date_preset})'
            thoi_gian_bao_cao = date_preset

        # --- BẮT ĐẦU QUÉT ---
        ket_qua_hien_thi = []
        nhat_ky_quet = [] 
        tong_tien_all = 0
        
        sheet = ket_noi_sheet_tab(ten_tab)
        
        for id_tk in DANH_SACH_TKQC:
            base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
            
            # Cấu hình fields
            fields = f'name,status,{time_param}{{spend,impressions,clicks,cpc,ctr}}'
            
            # --- CẤU HÌNH ĐƠN GIẢN NHẤT (ĐỂ KHÔNG BỊ LỖI PARAM) ---
            # Bỏ hoàn toàn effective_status -> Lấy TẤT CẢ (Active, Paused, Deleted...)
            params = {
                'fields': fields,
                'access_token': FB_ACCESS_TOKEN,
                'limit': 500, # Mỗi lần xúc 500 ông
            }
            
            # --- THUẬT TOÁN LẬT TRANG ---
            all_campaigns = []
            next_url = base_url
            trang_thu = 1
            
            while True:
                if trang_thu == 1:
                    response = requests.get(next_url, params=params)
                else:
                    response = requests.get(next_url)
                
                data = response.json

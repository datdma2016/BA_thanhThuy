import gspread
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask
from datetime import datetime, timedelta # <--- Đã thêm thư viện xử lý giờ

app = Flask(__name__)

def ket_noi_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    # --- NHỚ SỬA TÊN SHEET CỦA BẠN LẠI NHÉ ---
    sheet = client.open("pancakeTest_260120").sheet1 
    return sheet

@app.route('/')
def home():
    try:
        sheet = ket_noi_sheet()
        
        # --- ĐOẠN NÀY ĐỂ CHỈNH GIỜ VIỆT NAM ---
        # Lấy giờ hiện tại của server (UTC) cộng thêm 7 tiếng
        gio_vn = datetime.now() + timedelta(hours=7) 
        thoi_gian_dep = gio_vn.strftime("%H:%M:%S - Ngày %d/%m/%Y")
        
        # Ghi vào sheet
        sheet.append_row(["Test múi giờ", thoi_gian_dep, "Đã chuẩn giờ VN!"])
        return f"Đã ghi xong! Giờ Việt Nam là: {thoi_gian_dep}"
    except Exception as e:
        return f"Lỗi: {str(e)}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

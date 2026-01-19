import gspread
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask
from datetime import datetime

# Cài đặt Flask để giữ web luôn sống
app = Flask(__name__)

# Cấu hình kết nối Google Sheets
def ket_noi_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    # Bot sẽ tự tìm file credentials.json mà bạn đã cất trong Secret File của Render
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    
    # --- QUAN TRỌNG: SỬA TÊN FILE SHEET CỦA BẠN Ở DƯỚI ĐÂY ---
    # Ví dụ file sheet tên là "Data Khach Hang" thì điền y hệt vào
    sheet = client.open("pancakeTest_260120").sheet1 
    return sheet

@app.route('/')
def home():
    try:
        # Thử kết nối và ghi một dòng vào Sheet
        sheet = ket_noi_sheet()
        thoi_gian = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sheet.append_row(["Test kết nối", thoi_gian, "Thành công rực rỡ!"])
        return f"Đã ghi dữ liệu vào Sheet lúc {thoi_gian}"
    except Exception as e:
        return f"Có lỗi rồi: {str(e)}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

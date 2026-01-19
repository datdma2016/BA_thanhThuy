import gspread
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)

# --- CẤU HÌNH KẾT NỐI SHEET ---
def ket_noi_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    # Tên sheet chuẩn của bạn
    sheet = client.open("pancakeTest_260120").sheet1 
    return sheet

# --- TRANG CHỦ (Để bạn test xem web còn sống không) ---
@app.route('/')
def home():
    return "Hệ thống Webhook Pancake đang chạy ngon lành!"

# --- CỔNG NHẬN DỮ LIỆU (WEBHOOK) ---
@app.route('/webhook', methods=['POST'])
def nhan_webhook():
    try:
        # 1. Nhận gói tin từ Pancake
        du_lieu = request.json
        print("Dữ liệu nhận được:", du_lieu) # In ra màn hình log để kiểm tra
        
        # 2. Xử lý thời gian
        gio_vn = datetime.now() + timedelta(hours=7)
        thoi_gian = gio_vn.strftime("%H:%M:%S - %d/%m/%Y")
        
        # 3. Chuyển dữ liệu thành dạng chuỗi để ghi vào Sheet (test trước)
        # Lát nữa biết cấu trúc rồi mình sẽ tách lấy Tên, SĐT sau
        noidung_tho = str(du_lieu)
        
        # 4. Ghi vào Google Sheet
        sheet = ket_noi_sheet()
        sheet.append_row(["WEBHOOK PANCAKE", thoi_gian, noidung_tho])
        
        return jsonify({"status": "success", "message": "Đã nhận được tin!"}), 200
        
    except Exception as e:
        print(f"Lỗi: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

import gspread
import requests
import json
import traceback
import shlex
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)

# ======================================================
# 1. CẤU HÌNH DANH SÁCH TÀI KHOẢN (V13)
# ======================================================

FB_ACCESS_TOKEN = "EAANPvsZANh38BQt8Bcqztr63LDZBieQxO2h5TnOIGpHQtlOnV85cwg7I2ZCVf8vFTccpbB7hX97HYOsGFEKLD3fSZC2BCyKWeZA0vsUJZCXBZAMVZARMwZCvTuPGTsIStG5ro10ltZBXs3yTOzBLjZAjfL8TAeXwgKC73ZBZA3aQD6eludndMkOYFrVCFv2CrIrNe5nX82FScL0TzIXjA7qUl9HZAz" 

# Cấu trúc mới: Mỗi dòng là một cặp {"id": "...", "name": "..."}
DANH_SACH_TKQC = [
    {"id": "581662847745376", "name": "tick_xanh_001"},
    {"id": "1934563933738877", "name": "Juul_001"}, # Lưu ý: ID này đang trùng dòng trên
    {"id": "995686602001085", "name": "Juul_004"},
    {"id": "689891917184988", "name": "116_1"},
    {"id": "2228203290991345", "name": "116_2"},
    {"id": "1369968844500935", "name": "116_3"},
    {"id": "828619376785021", "name": "116_4"},
    {"id": "757870177275480", "name": "116_5"}
]

FILE_SHEET_GOC = "pancakeTest_260120"

# ======================================================

def log_system(logs, message, type="INFO"):
    time_now = datetime.now().strftime("%H:%M:%S")
    color = "black"
    if type == "SUCCESS": color = "green"
    elif type == "ERROR": color = "red"
    elif type == "WARNING": color = "orange"
    logs.append(f"<li style='color:{color}'><b>[{time_now}] [{type}]</b>: {message}</li>")

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
            # HEADER MỚI: Thêm cột "Tên TK" vào cột số 2
            header = ["ID TK", "Tên TK", "Tên Chiến Dịch", "Trạng thái", "Thời gian", "Tiền tiêu", "Reach", "Data (Mess+Cmt)", "Giá Data", "Giá trị CĐ", "ROAS"]
            worksheet.append_row(header)
        return worksheet
    except Exception as e:
        log_system(logs, f"Lỗi Sheet: {str(e)}", "ERROR")
        return None

@app.route('/')
def home():
    return "<h1>Bot V13: Đã cập nhật Tên Tài Khoản & Danh sách ID mới!</h1>"

@app.route('/fb-ads')
def lay_data_fb():
    logs = []
    tong_hop_tk = {} # Dictionary để sum theo TÊN TK
    
    try:
        keyword = request.args.get('keyword', '')
        ten_tab = request.args.get('sheet', 'BaoCaoV13')
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

        sheet = ket_noi_sheet_tab(logs, ten_tab)
        if not sheet: return "Lỗi kết nối Sheet"

        ket_qua_hien_thi = []
        fields_list = f'name,status,{time_param}{{spend,reach,actions,action_values,purchase_roas}}'

        # DUYỆT QUA DANH SÁCH MỚI (List of Dictionaries)
        for tk_obj in DANH_SACH_TKQC:
            id_tk = tk_obj['id']
            ten_tk = tk_obj['name']
            
            # Khởi tạo biến tổng cho TK này (nếu chưa có)
            if ten_tk not in tong_hop_tk:
                tong_hop_tk[ten_tk] = {'id': id_tk, 'spend': 0, 'data': 0, 'revenue': 0, 'reach': 0, 'camp_count': 0}

            base_url = f"https://graph.facebook.com/v19.0/act_{id_tk}/campaigns"
            params = {'fields': fields_list, 'access_token': FB_ACCESS_TOKEN, 'limit': 500}
            
            all_campaigns = []
            next_url = base_url
            
            while True:
                try:
                    res = requests.get(next_url, params=params if next_url == base_url else None)
                    data = res.json()
                    if 'error' in data:
                        log_system(logs, f"Lỗi TK {ten_tk} ({id_tk}): {data['error']['message']}", "ERROR")
                        break
                    all_campaigns.extend(data.get('data', []))
                    if 'paging' in data and 'next' in data['paging']:
                        next_url = data['paging']['next']
                    else: break
                except: break

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
                            # THÊM CỘT ten_tk VÀO SHEET
                            row = [id_tk, ten_tk, ten_camp, trang_thai, thoi_gian_bao_cao, spend, reach, total_data, gia_data, revenue, roas]
                            sheet.append_row(row)
                            
                            # Cộng tổng theo TÊN TK
                            tong_hop_tk[ten_tk]['spend'] += spend
                            tong_hop_tk[ten_tk]['data'] += total_data
                            tong_hop_tk[ten_tk]['revenue'] += revenue
                            tong_hop_tk[ten_tk]['reach'] += reach
                            tong_hop_tk[ten_tk]['camp_count'] += 1
                            count_camp_tk += 1

                            ket_qua_hien_thi.append(
                                f"<li>[{ten_tk}] {ten_camp}: <b>{fmt_vn(spend)}đ</b> | Rev: {fmt_vn(revenue)}đ</li>"
                            )
            
            log_system(logs, f"TK {ten_tk}: Xong {count_camp_tk} camp.", "SUCCESS")

        # HTML SUMMARY (Sửa header bảng tổng hợp)
        html_summary = "<table border='1' cellpadding='5' style='border-collapse:collapse; width:100%'>"
        html_summary += "<tr style='background:#f2f2f2'><th>Tên TK</th><th>Camp</th><th>Tiêu (VNĐ)</th><th>Data</th><th>Giá Data</th><th>Doanh Thu</th><th>ROAS</th></tr>"
        
        grand_spend = 0
        grand_rev = 0
        
        for ten, val in tong_hop_tk.items():
            gia_data_tb = round(val['spend'] / val['data']) if val['data'] > 0 else 0
            roas_tb = (val['revenue'] / val['spend']) if val['spend'] > 0 else 0
            grand_spend += val['spend']
            grand_rev += val['revenue']
            
            html_summary += f"""
            <tr>
                <td><b>{ten}</b><br><small>{val['id']}</small></td>
                <td align='center'>{val['camp_count']}</td>
                <td align='right'>{fmt_vn(val['spend'])}</td>
                <td align='center'>{fmt_vn(val['data'])}</td>
                <td align='right'>{fmt_vn(gia_data_tb)}</td>
                <td align='right'>{fmt_vn(val['revenue'])}</td>
                <td align='center'><b>{roas_tb:.2f}</b></td>
            </tr>
            """
        
        html = f"""
        <style>body{{font-family:Arial, sans-serif;}} table{{width:100%;}} td,th{{padding:8px;}}</style>
        <h2>BÁO CÁO V13 (CÓ TÊN TK)</h2>
        <p>
            🔍 <b>Từ khóa:</b> "{keyword}"<br>
            💰 <b>Tổng tiêu:</b> <span style="color:red">{fmt_vn(grand_spend)} VNĐ</span>
        </p>
        {html_summary}
        <ul style='font-size:14px'>{''.join(ket_qua_hien_thi)}</ul>
        <h3>Logs</h3>
        <div style="background:#eee; padding:5px; height:100px; overflow-y:scroll; border:1px solid #ddd"><ul>{''.join(logs)}</ul></div>
        """
        return html

    except Exception as e:
        return f"ERROR: {traceback.format_exc()}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

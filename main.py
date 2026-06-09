"""
taiwan-chips-relay / main.py
Zeabur 台灣機房中繼站：每日 15:35 直連證交所抓取三大法人籌碼
提供 /api/chips JSON API 供 Streamlit 前端讀取
"""
import os, json, time, threading, requests, pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── 48 檔戰備庫
TARGET_STOCKS = {
    "2330","2317","2454","2308","2357","3037","8299","3289","2301","2313",
    "2345","2383","3044","6274","6285","2634","3149","7828","3491","2359",
    "6188","3661","3324","3017","3653","3533","6669","3131","6187","6510",
    "6515","3455","6239","2059","2368","6271","3665","3481","2404","2327",
    "2344","2379","8358","1519","1503","8033","8027","8064"
}

CSV_PATH   = "data/chips_twse.csv"
MARGIN_PATH = "data/margin_twse.csv"
TZ_TW      = ZoneInfo("Asia/Taipei")


def clean_num(s):
    try:
        return int(str(s).replace(",", "").replace("+", "").strip())
    except:
        return 0


def fetch_twse_chips():
    """爬取證交所三大法人個股買賣超（T86W）"""
    today = datetime.now(TZ_TW).strftime("%Y%m%d")
    today_dash = datetime.now(TZ_TW).strftime("%Y-%m-%d")
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={today}&selectType=ALLBUT0999&response=json"
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        r = requests.get(url, headers=hdrs, timeout=15)
        d = r.json()
        if "data" not in d:
            print(f"[{today}] 證交所尚未公布或休市")
            return
        rows = []
        for row in d["data"]:
            code = str(row[0]).strip()
            if code not in TARGET_STOCKS:
                continue
            rows.append({
                "date": today_dash,
                "stock_id": code,
                "name": "Foreign_Investor",
                "buy": 0, "sell": 0,
                "net": clean_num(row[4]),
                "source": "institutional"
            })
            rows.append({
                "date": today_dash,
                "stock_id": code,
                "name": "Investment_Trust",
                "buy": 0, "sell": 0,
                "net": clean_num(row[7]),
                "source": "institutional"
            })
            rows.append({
                "date": today_dash,
                "stock_id": code,
                "name": "Dealer_Hedging",
                "buy": 0, "sell": 0,
                "net": clean_num(row[10]),
                "source": "institutional"
            })
        if not rows:
            return
        df_new = pd.DataFrame(rows)
        os.makedirs("data", exist_ok=True)
        if os.path.exists(CSV_PATH):
            df_old = pd.read_csv(CSV_PATH)
            df_old = df_old[df_old["date"] != today_dash]
            df_final = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_final = df_new
        df_final.to_csv(CSV_PATH, index=False)
        print(f"[{today}] 三大法人更新完成，共 {len(rows)} 筆")
    except Exception as e:
        print(f"[chips] 錯誤：{e}")


def fetch_twse_margin():
    """爬取證交所融資融券（每日 16:30 後公布）"""
    today = datetime.now(TZ_TW).strftime("%Y%m%d")
    today_dash = datetime.now(TZ_TW).strftime("%Y-%m-%d")
    url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={today}&selectType=stock&response=json"
    hdrs = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=hdrs, timeout=15)
        d = r.json()
        tables = d.get("tables", [])
        if not tables:
            print(f"[{today}] 融資券尚未公布")
            return
        rows = []
        for row in tables[0].get("data", []):
            code = str(row[0]).strip()
            if code not in TARGET_STOCKS:
                continue
            rows.append({
                "date": today_dash,
                "stock_id": code,
                "margin_balance": clean_num(row[4]),   # 融資餘額
                "short_balance":  clean_num(row[10]),  # 融券餘額
                "source": "margin"
            })
        if not rows:
            return
        df_new = pd.DataFrame(rows)
        os.makedirs("data", exist_ok=True)
        if os.path.exists(MARGIN_PATH):
            df_old = pd.read_csv(MARGIN_PATH)
            df_old = df_old[df_old["date"] != today_dash]
            df_final = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_final = df_new
        df_final.to_csv(MARGIN_PATH, index=False)
        print(f"[{today}] 融資券更新完成，共 {len(rows)} 筆")
    except Exception as e:
        print(f"[margin] 錯誤：{e}")


def scheduler():
    print("排程器啟動：週一到週五 15:35 抓三大法人，16:35 抓融資券")
    triggered_chips  = False
    triggered_margin = False
    last_date = ""
    while True:
        now = datetime.now(TZ_TW)
        today = now.strftime("%Y-%m-%d")
        # 每天重置觸發旗標
        if today != last_date:
            triggered_chips  = False
            triggered_margin = False
            last_date = today
        if now.weekday() < 5:
            if now.hour == 15 and now.minute >= 35 and not triggered_chips:
                print("15:35 觸發三大法人爬蟲...")
                fetch_twse_chips()
                triggered_chips = True
            if now.hour == 16 and now.minute >= 35 and not triggered_margin:
                print("16:35 觸發融資券爬蟲...")
                fetch_twse_margin()
                triggered_margin = True
        time.sleep(30)


threading.Thread(target=scheduler, daemon=True).start()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # 靜音 access log

    def do_GET(self):
        if self.path == "/health":
            self._respond(200, {"status": "ok",
                                "time": datetime.now(TZ_TW).strftime("%Y-%m-%d %H:%M")})
        elif self.path == "/api/chips":
            self._serve_csv(CSV_PATH)
        elif self.path == "/api/margin":
            self._serve_csv(MARGIN_PATH)
        else:
            self.send_error(404)

    def _respond(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _serve_csv(self, path):
        if not os.path.exists(path):
            self.send_error(404, "Data not ready yet")
            return
        df = pd.read_csv(path)
        # 只回傳最近 90 天
        if "date" in df.columns:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= cutoff]
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        body = df.to_json(orient="records", force_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 台灣籌碼中繼站啟動，port {port}")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

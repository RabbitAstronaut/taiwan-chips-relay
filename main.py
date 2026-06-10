"""
taiwan-chips-relay / main.py
Zeabur/Render 台灣機房中繼站：每日 15:35 直連證交所+櫃買抓取三大法人籌碼
提供 /api/chips JSON API 供 Streamlit 前端讀取
"""
import os, json, time, threading, requests, pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── 動態從 GitHub watchlist.json 讀取儲備庫
GITHUB_WATCHLIST_URL = (
    "https://raw.githubusercontent.com/RabbitAstronaut/"
    "taiwan-stock-dashboard/main/data/watchlist.json"
)

def load_target_stocks():
    """從 GitHub watchlist.json 讀取儲備庫股號，動態更新監控清單"""
    try:
        r = requests.get(GITHUB_WATCHLIST_URL, timeout=10)
        if r.status_code == 200:
            data = r.json()
            stocks = set()
            # 讀取 reserve（戰略儲備庫）
            for item in data.get("reserve", []):
                sid = str(item.get("id","")).strip()
                if sid: stocks.add(sid)
            # 也讀取 manual（持股監控）
            for item in data.get("manual", []):
                sid = str(item.get("id","")).strip()
                if sid: stocks.add(sid)
            if stocks:
                print(f"[stocks] 從 GitHub 讀取 {len(stocks)} 檔監控標的", flush=True)
                return stocks
    except Exception as e:
        print(f"[stocks] 讀取失敗，使用預設清單：{e}", flush=True)

    # fallback：預設 48 檔
    return {
        "2330","2317","2454","2308","2357","3037","8299","3289","2301","2313",
        "2345","2383","3044","6274","6285","2634","3149","7828","3491","2359",
        "6188","3661","3324","3017","3653","3533","6669","3131","6187","6510",
        "6515","3455","6239","2059","2368","6271","3665","3481","2404","2327",
        "2344","2379","8358","1519","1503","8033","8027","8064"
    }

TARGET_STOCKS = load_target_stocks()

CSV_PATH     = "data/chips_twse.csv"
MARGIN_PATH  = "data/margin_twse.csv"
FUTURES_PATH = "data/futures_data.csv"
TZ_TW       = ZoneInfo("Asia/Taipei")
HDRS        = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def clean_num(s):
    try:
        return int(str(s).replace(",","").replace("+","").strip())
    except:
        return 0


def parse_date(date_str):
    if date_str:
        t = date_str.replace("-","")
        return t, f"{t[:4]}-{t[4:6]}-{t[6:]}"
    today = datetime.now(TZ_TW)
    return today.strftime("%Y%m%d"), today.strftime("%Y-%m-%d")


def _fetch_twse(today, today_dash):
    """爬取上市（TWSE）三大法人"""
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={today}&selectType=ALLBUT0999&response=json"
    print(f"[TWSE] 抓取 {today}", flush=True)
    rows = []
    try:
        r = requests.get(url, headers=HDRS, timeout=15, verify=False)
        d = r.json()
        if "data" not in d:
            print(f"[TWSE] 尚未公布：{str(d)[:100]}", flush=True)
            return rows
        for row in d["data"]:
            code = str(row[0]).strip()
            if code not in TARGET_STOCKS: continue
            rows += [
                {"date":today_dash,"stock_id":code,"name":"Foreign_Investor","net":clean_num(row[4]),"source":"twse"},
                {"date":today_dash,"stock_id":code,"name":"Investment_Trust","net":clean_num(row[7]),"source":"twse"},
                {"date":today_dash,"stock_id":code,"name":"Dealer_Hedging",  "net":clean_num(row[10]),"source":"twse"},
            ]
        print(f"[TWSE] 完成 {len(rows)//3} 檔", flush=True)
    except Exception as e:
        import traceback; print(f"[TWSE] 錯誤：{e}",flush=True); traceback.print_exc()
    return rows


def _fetch_otc(today, today_dash):
    """爬取上櫃（OTC/TPEx）三大法人"""
    try:
        dt = datetime.strptime(today, "%Y%m%d")
        roc = f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"
    except:
        return []
    url = (f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
           f"3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&d={roc}")
    print(f"[OTC] 抓取 {today}（民國 {roc}）", flush=True)
    rows = []
    try:
        r = requests.get(url, headers=HDRS, timeout=15, verify=False)
        d = r.json()
        tables = d.get("tables", [])
        if not tables:
            print(f"[OTC] 尚未公布", flush=True)
            return rows
        for row in tables[0].get("data", []):
            code = str(row[0]).strip()
            if code not in TARGET_STOCKS: continue
            rows += [
                {"date":today_dash,"stock_id":code,"name":"Foreign_Investor","net":clean_num(row[4]),"source":"otc"},
                {"date":today_dash,"stock_id":code,"name":"Investment_Trust","net":clean_num(row[7]),"source":"otc"},
                {"date":today_dash,"stock_id":code,"name":"Dealer_Hedging",  "net":clean_num(row[10]),"source":"otc"},
            ]
        print(f"[OTC] 完成 {len(rows)//3} 檔", flush=True)
    except Exception as e:
        import traceback; print(f"[OTC] 錯誤：{e}",flush=True); traceback.print_exc()
    return rows


GITHUB_REPO = "RabbitAstronaut/taiwan-stock-dashboard"

def _push_to_github(local_path, github_path):
    """把本地 CSV 推回 GitHub（用 GH_TOKEN 環境變數）"""
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        print(f"[github] 無 GH_TOKEN，跳過推送", flush=True)
        return
    try:
        with open(local_path, "rb") as f:
            content = __import__("base64").b64encode(f.read()).decode()
        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{github_path}"
        headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
        # 取得現有 SHA（更新時需要）
        r = requests.get(api_url, headers=headers, timeout=10)
        sha = r.json().get("sha", "") if r.status_code == 200 else ""
        today = datetime.now(TZ_TW).strftime("%Y-%m-%d")
        body = {"message": f"auto: update {github_path} {today}", "content": content}
        if sha:
            body["sha"] = sha
        r2 = requests.put(api_url, headers=headers, json=body, timeout=20)
        if r2.status_code in (200, 201):
            print(f"[github] ✅ 推送成功：{github_path}", flush=True)
        else:
            print(f"[github] ❌ 推送失敗 {r2.status_code}：{r2.text[:100]}", flush=True)
    except Exception as e:
        print(f"[github] 錯誤：{e}", flush=True)


def fetch_chips(date_str=None):
    """合併上市+上櫃三大法人，寫入 CSV"""
    today, today_dash = parse_date(date_str)
    rows = _fetch_twse(today, today_dash) + _fetch_otc(today, today_dash)
    if not rows:
        print(f"[chips] 無資料", flush=True)
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
    print(f"[chips] 寫入完成，共 {len(rows)} 筆", flush=True)
    _push_to_github(CSV_PATH, "data/chips_twse.csv")


def fetch_margin(date_str=None):
    """爬取上市融資融券"""
    today, today_dash = parse_date(date_str)
    url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={today}&selectType=stock&response=json"
    print(f"[margin] 抓取 {today}", flush=True)
    try:
        r = requests.get(url, headers=HDRS, timeout=15, verify=False)
        d = r.json()
        tables = d.get("tables", [])
        if not tables:
            print(f"[margin] 尚未公布", flush=True)
            return
        rows = []
        for row in tables[0].get("data", []):
            code = str(row[0]).strip()
            if code not in TARGET_STOCKS: continue
            rows.append({
                "date": today_dash, "stock_id": code,
                "margin_balance": clean_num(row[4]),
                "short_balance":  clean_num(row[10]),
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
        print(f"[margin] 完成 {len(rows)} 檔", flush=True)
        _push_to_github(MARGIN_PATH, "data/margin_twse.csv")
    except Exception as e:
        import traceback; print(f"[margin] 錯誤：{e}",flush=True); traceback.print_exc()


def fetch_futures(date_str=None):
    """爬取期交所三大法人未平倉（TX大台 + MTX小台）"""
    today, today_dash = parse_date(date_str)
    try:
        dt = datetime.strptime(today, "%Y%m%d")
        query_date = f"{dt.year}/{dt.month:02d}/{dt.day:02d}"
    except:
        return
    # 期交所 JSON API
    url = "https://www.taifex.com.tw/cht/3/futContractsDate"
    print(f"[futures] 抓取 {today}", flush=True)
    rows = []
    try:
        r = requests.post(url, headers={**HDRS, "Accept": "application/json"},
                          timeout=15, verify=False,
                          data={"queryDate": query_date, "commodityId": ""})
        # 嘗試 JSON 解析
        try:
            data = r.json()
            items = data if isinstance(data, list) else data.get("data", [])
        except Exception:
            # JSON 失敗改用 CSV 下載端點
            csv_url = f"https://www.taifex.com.tw/cht/3/futContractsDateDown?queryDate={query_date}&commodityId="
            r2 = requests.get(csv_url, headers=HDRS, timeout=15, verify=False)
            r2.encoding = "big5"
            from io import StringIO
            df_raw = pd.read_csv(StringIO(r2.text), header=1)
            df_raw.columns = [str(c).strip() for c in df_raw.columns]
            items = df_raw.to_dict("records")

        contract_map = {"臺股期貨": "TX", "小型臺指期貨": "MTX"}
        identity_map = {"自營商": "自營商", "投信": "投信",
                        "外資及陸資": "外資", "外資": "外資"}

        for item in items:
            contract_name = str(item.get("商品名稱", item.get("契約", ""))).strip()
            futures_id = contract_map.get(contract_name)
            if not futures_id:
                continue
            identity = str(item.get("身份別", "")).strip()
            identity_clean = identity_map.get(identity, identity)
            if identity_clean not in ("自營商", "投信", "外資"):
                continue

            def _n(key):
                try:
                    return int(str(item.get(key, 0)).replace(",", "").strip())
                except:
                    return 0

            rows.append({
                "futures_id": futures_id,
                "date": today_dash,
                "institutional_investors": identity_clean,
                "long_deal_volume":  _n("多方交易口數"),
                "long_deal_amount":  _n("多方交易契約金額(千元)"),
                "short_deal_volume": _n("空方交易口數"),
                "short_deal_amount": _n("空方交易契約金額(千元)"),
                "long_open_interest_balance_volume":  _n("多方未平倉口數"),
                "long_open_interest_balance_amount":  _n("多方未平倉契約金額(千元)"),
                "short_open_interest_balance_volume": _n("空方未平倉口數"),
                "short_open_interest_balance_amount": _n("空方未平倉契約金額(千元)"),
                "contract": futures_id,
                "source": "institutional",
            })

        if not rows:
            print(f"[futures] 解析無資料", flush=True)
            return

        df_new = pd.DataFrame(rows)
        os.makedirs("data", exist_ok=True)
        if os.path.exists(FUTURES_PATH):
            df_old = pd.read_csv(FUTURES_PATH)
            df_old = df_old[df_old["date"] != today_dash]
            df_final = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_final = df_new
        df_final.to_csv(FUTURES_PATH, index=False)
        print(f"[futures] 寫入完成，共 {len(rows)} 筆", flush=True)
        _push_to_github(FUTURES_PATH, "data/futures_data.csv")

    except Exception as e:
        import traceback
        print(f"[futures] 錯誤：{e}", flush=True)
        traceback.print_exc()


def has_today_data(csv_path):
    """檢查 CSV 是否已有今日 TWSE 和 OTC 的資料（兩者都要有才算完整）"""
    try:
        if not os.path.exists(csv_path):
            return False
        df = pd.read_csv(csv_path)
        today_dash = datetime.now(TZ_TW).strftime("%Y-%m-%d")
        today_df = df[df["date"] == today_dash]
        has_twse = "twse" in today_df["source"].values
        has_otc  = "otc"  in today_df["source"].values
        if not has_twse:
            print(f"[has_today_data] TWSE 尚無資料", flush=True)
        if not has_otc:
            print(f"[has_today_data] OTC 尚無資料", flush=True)
        return has_twse and has_otc
    except:
        return False


def scheduler():
    print("排程器啟動：週一到週五 15:35 起抓籌碼，16:00 起抓期貨，16:35 起抓融資券，每15分鐘重試至17:30", flush=True)
    last_date = ""
    chips_ok = margin_ok = futures_ok = False
    chips_last_try = margin_last_try = futures_last_try = 0

    RETRY_INTERVAL = 15 * 60
    CHIPS_START   = (15, 35)
    FUTURES_START = (16,  0)
    MARGIN_START  = (16, 35)
    DEADLINE      = (17, 30)

    while True:
        now = datetime.now(TZ_TW)
        today = now.strftime("%Y-%m-%d")

        if today != last_date:
            chips_ok = margin_ok = futures_ok = False
            chips_last_try = margin_last_try = futures_last_try = 0
            last_date = today
            global TARGET_STOCKS
            TARGET_STOCKS = load_target_stocks()

        if now.weekday() < 5:
            now_mins  = now.hour * 60 + now.minute
            s_chips   = CHIPS_START[0]   * 60 + CHIPS_START[1]
            s_futures = FUTURES_START[0] * 60 + FUTURES_START[1]
            s_margin  = MARGIN_START[0]  * 60 + MARGIN_START[1]
            deadline  = DEADLINE[0]      * 60 + DEADLINE[1]
            now_epoch = time.time()

            # ── 籌碼：15:35 ~ 17:30
            if (s_chips <= now_mins <= deadline) and not chips_ok:
                if now_epoch - chips_last_try >= RETRY_INTERVAL:
                    attempt = int((now_mins - s_chips) // 15) + 1
                    print(f"[chips] 第{attempt}次嘗試（{now.strftime('%H:%M')}）...", flush=True)
                    chips_last_try = now_epoch
                    fetch_chips()
                    if has_today_data(CSV_PATH):
                        print(f"[chips] ✅ 資料確認，停止重試", flush=True)
                        chips_ok = True
                    else:
                        print(f"[chips] ⚠️ 尚無資料，15分鐘後重試", flush=True)
            if now_mins > deadline and not chips_ok:
                print(f"[chips] ❌ 已超過17:30，今日放棄重試", flush=True)
                chips_ok = True

            # ── 期貨：16:00 ~ 17:30
            if (s_futures <= now_mins <= deadline) and not futures_ok:
                if now_epoch - futures_last_try >= RETRY_INTERVAL:
                    attempt = int((now_mins - s_futures) // 15) + 1
                    print(f"[futures] 第{attempt}次嘗試（{now.strftime('%H:%M')}）...", flush=True)
                    futures_last_try = now_epoch
                    fetch_futures()
                    if has_today_data(FUTURES_PATH):
                        print(f"[futures] ✅ 資料確認，停止重試", flush=True)
                        futures_ok = True
                    else:
                        print(f"[futures] ⚠️ 尚無資料，15分鐘後重試", flush=True)
            if now_mins > deadline and not futures_ok:
                print(f"[futures] ❌ 已超過17:30，今日放棄重試", flush=True)
                futures_ok = True

            # ── 融資券：16:35 ~ 17:30
            if (s_margin <= now_mins <= deadline) and not margin_ok:
                if now_epoch - margin_last_try >= RETRY_INTERVAL:
                    attempt = int((now_mins - s_margin) // 15) + 1
                    print(f"[margin] 第{attempt}次嘗試（{now.strftime('%H:%M')}）...", flush=True)
                    margin_last_try = now_epoch
                    fetch_margin()
                    if has_today_data(MARGIN_PATH):
                        print(f"[margin] ✅ 資料確認，停止重試", flush=True)
                        margin_ok = True
                    else:
                        print(f"[margin] ⚠️ 尚無資料，15分鐘後重試", flush=True)
            if now_mins > deadline and not margin_ok:
                print(f"[margin] ❌ 已超過17:30，今日放棄重試", flush=True)
                margin_ok = True

        time.sleep(30)


threading.Thread(target=scheduler, daemon=True).start()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        path = parsed.path

        if path == "/health":
            self._respond(200, {"status":"ok","time":datetime.now(TZ_TW).strftime("%Y-%m-%d %H:%M")})
        elif path == "/api/chips":
            self._serve_csv(CSV_PATH)
        elif path == "/api/margin":
            self._serve_csv(MARGIN_PATH)
        elif path == "/api/futures":
            self._serve_csv(FUTURES_PATH)
        elif path == "/api/fetch_now":
            date_str = qs.get("date",[None])[0]
            fetch_chips(date_str)
            fetch_margin(date_str)
            fetch_futures(date_str)
            self._respond(200,{"status":"done","date":date_str or "today",
                               "chips":os.path.exists(CSV_PATH),
                               "margin":os.path.exists(MARGIN_PATH),
                               "futures":os.path.exists(FUTURES_PATH)})
        else:
            self.send_error(404)

    def _respond(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type","application/json")
        self.send_header("Access-Control-Allow-Origin","*")
        self.end_headers()
        self.wfile.write(body)

    def _serve_csv(self, path):
        if not os.path.exists(path):
            self.send_error(404,"Data not ready yet")
            return
        df = pd.read_csv(path)
        if "date" in df.columns:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= cutoff]
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        body = df.to_json(orient="records",force_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.send_header("Access-Control-Allow-Origin","*")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 台灣籌碼中繼站啟動，port {port}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

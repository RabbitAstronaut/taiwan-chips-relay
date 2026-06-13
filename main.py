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

def _push_to_cf_kv(local_path, kv_key):
    """把本地 CSV 最新日期的資料推到 Cloudflare KV（只存今日，不存歷史）"""
    cf_url   = os.environ.get("CF_KV_URL", "")
    cf_token = os.environ.get("CF_KV_TOKEN", "")
    if not cf_url or not cf_token:
        print(f"[cf_kv] 無 CF_KV_URL 或 CF_KV_TOKEN，跳過", flush=True)
        return
    try:
        df = pd.read_csv(local_path)
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
            latest = df["date"].max()
            df = df[df["date"] == latest]  # 只取最新日期
        json_content = df.to_json(orient="records", force_ascii=False)
        r = requests.put(
            f"{cf_url}/put?key={kv_key}",
            headers={"Authorization": f"Bearer {cf_token}", "Content-Type": "application/json"},
            data=json_content,
            timeout=15
        )
        if r.status_code == 200:
            print(f"[cf_kv] ✅ 推送成功：{kv_key}（{latest}，{len(df)} 筆）", flush=True)
        else:
            print(f"[cf_kv] ❌ 推送失敗 {r.status_code}：{r.text[:100]}", flush=True)
    except Exception as e:
        print(f"[cf_kv] 錯誤：{e}", flush=True)


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
        # 相容舊格式（有 buy 欄位）→ 只保留需要的欄位
        if "buy" in df_old.columns:
            _keep = [c for c in ["date","stock_id","name","net","source"] if c in df_old.columns]
            df_old = df_old[_keep].copy()
            df_old = df_old.dropna(subset=["name","net"])
            df_old = df_old[df_old["name"].astype(str) != "nan"]
            if "source" not in df_old.columns:
                df_old["source"] = "institutional"
            print(f"[chips] 舊格式自動轉換，保留 {len(df_old)} 筆", flush=True)
        df_old = df_old[df_old["date"] != today_dash]
        cutoff = (pd.Timestamp.now() - pd.Timedelta(days=90)).strftime("%Y-%m-%d")
        df_old = df_old[df_old["date"] >= cutoff]
        df_final = pd.concat([df_old, df_new], ignore_index=True) if not df_old.empty else df_new
    else:
        df_final = df_new
    df_final.to_csv(CSV_PATH, index=False)
    print(f"[chips] 寫入完成，共 {len(df_final)} 筆，最新日期={df_final['date'].max()}", flush=True)
    _push_to_github(CSV_PATH, "data/chips_data.csv")
    _push_to_cf_kv(CSV_PATH, "chips_data")


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
        _push_to_cf_kv(MARGIN_PATH, "margin_data")
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
    url = "https://www.taifex.com.tw/cht/3/futContractsDate"
    print(f"[futures] 抓取 {today}", flush=True)
    rows = []
    try:
        hdrs = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "zh-TW,zh;q=0.9",
            "Referer": "https://www.taifex.com.tw/cht/3/futContractsDate",
        }
        s = requests.Session()
        s.get(url, headers=hdrs, timeout=10, verify=False)
        r = s.post(url, headers=hdrs, timeout=15, verify=False, data={
            "queryDate": query_date,
            "commodityId": "",
            "queryType": "1",
            "doQuery": "1",
        })
        r.encoding = "utf-8"
        print(f"[futures] status={r.status_code} len={len(r.text)}", flush=True)

        import re
        tbody = re.search(r'<TBODY>(.*?)</TBODY>', r.text, re.DOTALL | re.IGNORECASE)
        if not tbody:
            print(f"[futures] 找不到 TBODY", flush=True)
            return

        trs = re.findall(r'<TR[^>]*>(.*?)</TR>', tbody.group(1), re.DOTALL | re.IGNORECASE)
        print(f"[futures] 找到 {len(trs)} 個 TR", flush=True)

        contract_map = {"臺股期貨": "TX", "小型臺指期貨": "MTX"}
        identity_map = {"自營商": "自營商", "投信": "投信",
                        "外資及陸資": "外資", "外資": "外資"}
        current_contract = None

        for tr in trs:
            tds = re.findall(r'<T[DH][^>]*>(.*?)</T[DH]>', tr, re.DOTALL | re.IGNORECASE)
            cells = [re.sub(r'<[^>]+>', '', td).strip().replace('\xa0','').replace(',','').replace('\n','').replace('\r','').strip() for td in tds]
            cells = [c for c in cells if c]
            if not cells:
                continue

            # 找契約名稱，非 TX/MTX 的契約重置
            all_contracts = {"臺股期貨","電子期貨","金融期貨","小型臺指期貨","微型臺指期貨",
                             "小型電子期貨","小型金融期貨","股票期貨","ETF期貨","櫃買指數期貨",
                             "非金電期貨","富櫃200期貨","臺灣中型100期貨","臺灣永續期貨",
                             "臺灣生技期貨","半導體30期貨","航運期貨","東證期貨",
                             "美國標普500期貨","美國那斯達克100期貨","美國道瓊期貨",
                             "美國費城半導體期貨","英國富時100期貨","期貨小計"}
            for c in cells:
                if c in contract_map:
                    current_contract = contract_map[c]
                    break
                elif c in all_contracts:
                    current_contract = None  # 非目標契約，重置
                    break

            if not current_contract:
                continue

            identity_clean = None
            for c in cells:
                if c in identity_map:
                    identity_clean = identity_map[c]
                    break
            if identity_clean is None and cells and cells[0] == "外資":
                identity_clean = "外資"

            if not identity_clean:
                continue

            # ── 直接從 cells 按位置取未平倉數字
            # 表格固定結構：
            # 自營商行(15格): [序號, 契約名, 身份別, 多交易口, 多交易金, 空交易口, 空交易金, 多空淨口, 多空淨金, 多未平口, 多未平金, 空未平口, 空未平金, 淨口, 淨金]
            # 投信/外資行(13格): [身份別, 多交易口, 多交易金, 空交易口, 空交易金, 多空淨口, 多空淨金, 多未平口, 多未平金, 空未平口, 空未平金, 淨口, 淨金]
            def _get_cell(idx):
                try:
                    v = cells[idx].replace(',','').replace('+','').strip()
                    return int(v) if v.lstrip('-').isdigit() else 0
                except:
                    return 0

            if len(cells) >= 15:
                # 自營商行（有序號和契約名）
                long_deal   = _get_cell(3)
                short_deal  = _get_cell(5)
                long_oi     = _get_cell(9)
                short_oi    = _get_cell(11)
            elif len(cells) >= 13:
                # 投信/外資行（無序號和契約名）
                long_deal   = _get_cell(1)
                short_deal  = _get_cell(3)
                long_oi     = _get_cell(7)
                short_oi    = _get_cell(9)
            else:
                continue

            rows.append({
                "futures_id": current_contract,
                "date": today_dash,
                "institutional_investors": identity_clean,
                "long_deal_volume":  long_deal,
                "long_deal_amount":  _get_cell(4) if len(cells) >= 15 else _get_cell(2),
                "short_deal_volume": short_deal,
                "short_deal_amount": _get_cell(6) if len(cells) >= 15 else _get_cell(4),
                "long_open_interest_balance_volume":  long_oi,
                "long_open_interest_balance_amount":  _get_cell(10) if len(cells) >= 15 else _get_cell(8),
                "short_open_interest_balance_volume": short_oi,
                "short_open_interest_balance_amount": _get_cell(12) if len(cells) >= 15 else _get_cell(10),
                "contract": current_contract,
                "source": "institutional",
            })

        print(f"[futures] 解析到 {len(rows)} 筆", flush=True)
        if not rows:
            return

        df_new = pd.DataFrame(rows)
        os.makedirs("data", exist_ok=True)
        if os.path.exists(FUTURES_PATH):
            df_old = pd.read_csv(FUTURES_PATH)
            if "buy" not in df_old.columns:
                df_old = df_old[df_old["date"] != today_dash]
                df_final = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_final = df_new
        else:
            df_final = df_new
        df_final.to_csv(FUTURES_PATH, index=False)
        print(f"[futures] 寫入完成，共 {len(df_final)} 筆，最新日期={df_final['date'].max()}", flush=True)
        _push_to_github(FUTURES_PATH, "data/futures_data.csv")
        _push_to_cf_kv(FUTURES_PATH, "futures_data")

    except Exception as e:
        import traceback
        print(f"[futures] 錯誤：{e}", flush=True)
        traceback.print_exc()

def trigger_github_actions():
    """觸發 GitHub Actions daily_update workflow"""
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        return
    try:
        url = "https://api.github.com/repos/RabbitAstronaut/taiwan-stock-dashboard/actions/workflows/daily_update.yml/dispatches"
        headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
        r = requests.post(url, headers=headers, json={"ref": "main"}, timeout=10)
        if r.status_code == 204:
            print(f"[actions] ✅ 已觸發 GitHub Actions", flush=True)
        else:
            print(f"[actions] ❌ 觸發失敗 {r.status_code}: {r.text[:100]}", flush=True)
    except Exception as e:
        print(f"[actions] 錯誤：{e}", flush=True)


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
    gh_triggered  = False

    while True:
        now = datetime.now(TZ_TW)
        today = now.strftime("%Y-%m-%d")

        if today != last_date:
            chips_ok = margin_ok = futures_ok = False
            chips_last_try = margin_last_try = futures_last_try = 0
            last_date = today
            gh_triggered = False
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

            # ── 17:35 觸發 GitHub Actions（補抓 K線/財務等資料）
            if now_mins >= 17*60+35 and not gh_triggered:
                print(f"[actions] 17:35 觸發 GitHub Actions...", flush=True)
                trigger_github_actions()
                gh_triggered = True

        time.sleep(30)


def restore_from_github():
    """啟動時從 GitHub 還原 CSV 資料到容器"""
    files = {
        "data/chips_twse.csv":   "https://raw.githubusercontent.com/RabbitAstronaut/taiwan-stock-dashboard/main/data/chips_data.csv",
        "data/margin_twse.csv":  "https://raw.githubusercontent.com/RabbitAstronaut/taiwan-stock-dashboard/main/data/margin_twse.csv",
        "data/futures_data.csv": "https://raw.githubusercontent.com/RabbitAstronaut/taiwan-stock-dashboard/main/data/futures_data.csv",
    }
    os.makedirs("data", exist_ok=True)
    for local_path, url in files.items():
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                # chips_twse.csv：自動相容新舊格式
                if local_path == "data/chips_twse.csv":
                    first_line = r.text.split("\n")[0]
                    if "buy" in first_line:
                        # 舊格式：自動轉換，只保留需要的欄位
                        try:
                            import io
                            df_old = pd.read_csv(io.StringIO(r.text))
                            _keep = [c for c in ["date","stock_id","name","net","source"] if c in df_old.columns]
                            df_old = df_old[_keep].copy()
                            df_old = df_old.dropna(subset=["name","net"])
                            df_old = df_old[df_old["name"].astype(str) != "nan"]
                            if "source" not in df_old.columns:
                                df_old["source"] = "institutional"
                            df_old.to_csv(local_path, index=False)
                            print(f"[restore] ✅ {local_path}（舊格式轉換，{len(df_old)} 筆）", flush=True)
                        except Exception as e:
                            print(f"[restore] ⚠️ {local_path} 舊格式轉換失敗：{e}", flush=True)
                        continue
                with open(local_path, "wb") as f:
                    f.write(r.content)
                print(f"[restore] ✅ {local_path}", flush=True)
            else:
                print(f"[restore] ⚠️ {local_path} status={r.status_code}", flush=True)
        except Exception as e:
            print(f"[restore] ❌ {local_path} 錯誤：{e}", flush=True)

restore_from_github()
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
            # 背景執行，立即回傳避免超時
            import threading as _t
            def _run():
                fetch_chips(date_str)
                fetch_margin(date_str)
                fetch_futures(date_str)
            _t.Thread(target=_run, daemon=True).start()
            self._respond(200,{"status":"started","date":date_str or "today",
                               "message":"抓取已在背景啟動，30秒後查看 /api/chips"})
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

"""
ALTCOIN OI + FUNDING RATE MONİTÖRÜ
=====================================
Binance Futures'taki TÜM USDT-M perpetual coinleri tarar.

SİNYAL MANTIĞI (kullanıcı tarafından belirlendi):
  - Open Interest son 2 saatte (%5+) ARTMIŞ  (yeni pozisyon açılıyor, tasfiye değil)
  - Funding Rate POZİTİF  -> agresif yeni LONG'lar giriyor -> LONG sinyali
  - Funding Rate NEGATİF  -> agresif yeni SHORT'lar giriyor -> SHORT sinyali

Bu TREND TEYİDİ / MOMENTUM mantığıdır (önceki "liquidation cascade reversal"
stratejisinin tam tersi bir felsefe - o tasfiye SONRASI tersine dönüş
bahsi yapıyordu, bu ise aktif pozisyon açılışını takip ediyor).

ÇALIŞTIRMA MODELİ:
  Bu script TEK SEFERLİK bir tarama yapar (sürekli açık kalan bir döngü
  DEĞİLDİR). "Her 15 dakikada bir" çalışması için Windows Görev
  Zamanlayıcı (Task Scheduler) ile 15 dakikada bir tetiklenecek şekilde
  ayarlanmalı. Bunun sebebi: günlerce açık bir Python prosesi bellek/
  bağlantı sorunları biriktirebilir; Task Scheduler ile her çalıştırmada
  temiz bir başlangıç daha güvenilirdir.

KURULUM:
  1. pip install requests
  2. email_config.json.example dosyasını email_config.json olarak kopyala,
     kendi Gmail App Password bilgilerinle doldur.
  3. Test için: python altcoin_oi_funding_monitor.py --dry-run
     (e-posta ATMAZ, ne yapacağını konsola yazar)
  4. Gerçek çalıştırma: python altcoin_oi_funding_monitor.py
  5. Windows Görev Zamanlayıcı'ya ekle (15 dakikada bir tetiklensin).
"""

from __future__ import annotations

import os
import sys
import json
import csv
import time
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta

import requests

MONITOR_VERSION = "2026-07-04-v5-liq-heatmap"
BASE_URL = "https://fapi.binance.com"
STATE_DIR = "monitor_state"
SIGNALS_CSV = os.path.join(STATE_DIR, "signals_log.csv")
LAST_SIGNALS_JSON = os.path.join(STATE_DIR, "last_signals.json")
SUMMARY_JSON = os.path.join(STATE_DIR, "performance_summary.json")
EMAIL_CONFIG_PATH = "email_config.json"

# ============================== AYARLAR ==================================
OI_LOOKBACK_BARS = 8          # 8 x 15dk = 2 saat
OI_RISE_THRESHOLD = 0.05      # %5+ OI artışı (kullanıcı: "sıkı")
FUNDING_EPS = 0.00002         # bunun altındaki funding "nötr" sayılır, sinyal yok
COOLDOWN_MINUTES = 240        # aynı sembol+yön için 4 saat tekrar aday oluşturma yok
EVAL_MINUTES = 480            # sinyal 8 saat sonra WIN/LOSS olarak değerlendirilir (stop tetiklenmezse)
SUCCESS_THRESHOLD = 0.005     # %0.5+ lehte hareket = WIN, %0.5+ aleyhte = LOSS (8 saat sonunda)
STOP_LOSS_PCT = -0.01         # fiyat -%1 aleyhe giderse 8 saati beklemeden HEMEN LOSS olarak kapat
                               # (kullanıcı 30x kaldıraç kullanıyor, -%1 fiyat hareketi ~-%30 kaldıraçlı zarar demek)
REQUEST_SLEEP = 0.15          # her sembol taramasından sonra bekleme (rate limit)
KLINES_INTERVAL = "15m"

# --- YENİ: Hacim Profili (Volume Profile) Destek/Direnç ---
VP_LOOKBACK_BARS = 96         # 96 x 15dk = son 24 saat
VP_BINS = 20                  # fiyat aralığı kaç dilime bölünsün
VP_TOP_LEVELS = 5             # en yüksek hacimli kaç seviye seviye adayı olsun

# --- YENİ: İzleme Listesi (Watchlist) mekaniği ---
# OI+funding tetiklenince HEMEN alarm atılmaz. Sembol izleme listesine girer.
# Fiyat WATCH_WINDOW_MINUTES içinde hedef destek/direnç seviyesine dokunursa
# GERÇEK giriş alarmı gönderilir. Dokunmazsa süre sonunda aday silinir (sıfırlanır).
WATCH_WINDOW_MINUTES = 240    # "birkaç saat" -> 4 saat izleme penceresi
WATCHLIST_JSON = os.path.join(STATE_DIR, "watchlist.json")

# --- YENİ: Kalite filtreleri (gürültülü/düşük likiditeli sinyalleri ele) ---
MIN_24H_QUOTE_VOLUME_USDT = 5_000_000   # son 24 saatte en az 5M USDT işlem hacmi
MIN_OPEN_INTEREST_USDT = 2_000_000      # en az 2M USDT toplam açık pozisyon
FUNDING_CEILING = 0.003                 # %0.3 üstü funding = aşırı kalabalık pozisyon, ele
# ===========================================================================


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# BINANCE API
# ---------------------------------------------------------------------------
def get_all_perpetual_symbols(session: requests.Session) -> list[str]:
    resp = session.get(f"{BASE_URL}/fapi/v1/exchangeInfo", timeout=15)
    resp.raise_for_status()
    data = resp.json()
    symbols = [
        s["symbol"] for s in data["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    ]
    return sorted(symbols)


def fetch_symbol_snapshot(session: requests.Session, symbol: str) -> dict | None:
    """Bir sembol için güncel fiyat, OI değişimi, funding rate, hacim profili
    seviyeleri ve top trader long/short oranını döner."""
    try:
        # --- OHLCV (fiyat + hacim profili için 24 saatlik pencere) ---
        kl_resp = session.get(
            f"{BASE_URL}/fapi/v1/klines",
            params={"symbol": symbol, "interval": KLINES_INTERVAL, "limit": VP_LOOKBACK_BARS},
            timeout=10,
        )
        kl_resp.raise_for_status()
        klines = kl_resp.json()
        if len(klines) < 10:
            return None
        current_price = float(klines[-1][4])  # son mumun kapanışı
        # klines[i] = [openTime, open, high, low, close, volume, closeTime,
        #              quoteAssetVolume, trades, takerBuyBase, takerBuyQuote, ignore]
        quote_volume_24h = sum(float(k[7]) for k in klines)

        # --- Funding Rate (güncel/tahmini) ---
        fr_resp = session.get(
            f"{BASE_URL}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            timeout=10,
        )
        fr_resp.raise_for_status()
        funding_rate = float(fr_resp.json()["lastFundingRate"])

        # --- Open Interest geçmişi (kısa pencere, tek istek yeterli) ---
        oi_resp = session.get(
            f"{BASE_URL}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": KLINES_INTERVAL, "limit": OI_LOOKBACK_BARS + 1},
            timeout=10,
        )
        oi_resp.raise_for_status()
        oi_data = oi_resp.json()
        if len(oi_data) < 2:
            return None
        oi_start = float(oi_data[0]["sumOpenInterest"])
        oi_end = float(oi_data[-1]["sumOpenInterest"])
        if oi_start <= 0:
            return None
        oi_change_pct = (oi_end - oi_start) / oi_start
        oi_value_usd = float(oi_data[-1].get("sumOpenInterestValue", oi_end * current_price))

        # --- Top Trader Long/Short Pozisyon Oranı (bilgi amaçlı, gerçek Binance verisi) ---
        top_ratio = None
        try:
            ratio_resp = session.get(
                f"{BASE_URL}/futures/data/topLongShortPositionRatio",
                params={"symbol": symbol, "period": KLINES_INTERVAL, "limit": 1},
                timeout=10,
            )
            ratio_resp.raise_for_status()
            ratio_data = ratio_resp.json()
            if ratio_data:
                top_ratio = float(ratio_data[-1]["longShortRatio"])
        except requests.exceptions.RequestException:
            pass  # bu bilgi opsiyonel, hata olursa sinyali engellemesin

        vp_levels = compute_volume_profile(klines)

        return {
            "symbol": symbol,
            "price": current_price,
            "funding_rate": funding_rate,
            "oi_change_pct": oi_change_pct,
            "oi_value_usd": oi_value_usd,
            "quote_volume_24h": quote_volume_24h,
            "top_long_short_ratio": top_ratio,
            "vp_levels": vp_levels,
            "klines": klines,
        }
    except requests.exceptions.RequestException:
        return None
    except (KeyError, ValueError, IndexError):
        return None


def compute_volume_profile(klines: list) -> list[float]:
    """Son VP_LOOKBACK_BARS mumdan basit bir hacim profili çıkarır.
    En yüksek hacimli VP_TOP_LEVELS fiyat dilimini (destek/direnç adayı)
    fiyata göre sıralı döner."""
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    closes = [float(k[4]) for k in klines]
    volumes = [float(k[5]) for k in klines]

    price_min, price_max = min(lows), max(highs)
    if price_max <= price_min:
        return []

    bin_size = (price_max - price_min) / VP_BINS
    bin_volumes = [0.0] * VP_BINS
    for close, vol in zip(closes, volumes):
        idx = int((close - price_min) / bin_size)
        idx = min(max(idx, 0), VP_BINS - 1)
        bin_volumes[idx] += vol

    ranked_bins = sorted(range(VP_BINS), key=lambda i: bin_volumes[i], reverse=True)[:VP_TOP_LEVELS]
    levels = sorted(price_min + (i + 0.5) * bin_size for i in ranked_bins)
    return levels


def fetch_oi_history_long(session: requests.Session, symbol: str, bars: int = VP_LOOKBACK_BARS) -> list[dict]:
    """Hacim profili penceresiyle aynı uzunlukta (24 saat) OI geçmişi çeker.
    Sadece filtreden geçen (watchlist adayı olan) semboller için çağrılır -
    tüm 529 sembolde kullanılırsa API yükü çok artar."""
    try:
        resp = session.get(
            f"{BASE_URL}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": KLINES_INTERVAL, "limit": bars},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException:
        return []


def compute_liquidation_heatmap(klines: list, oi_history: list[dict], bins: int = 40) -> list[float]:
    """
    TAHMİNİ likidasyon kümesi hesabı - GERÇEK likidasyon verisi DEĞİLDİR.
    Mantık: OI'deki her pozitif artış (delta), o barın fiyatında YENİ pozisyon
    açıldığı varsayılır. Yaygın kaldıraç oranları (5x-100x) için bu pozisyonların
    hangi fiyatta tasfiye olacağı hesaplanır ve fiyat dilimlerine (bin) toplanır.
    En yoğun tasfiye tahmini olan dilimler döner (fiyata göre sıralı).

    VARSAYIMLAR (gerçek veri yok, bunlar tahmindir):
      - Kaldıraç dağılımı: 5x,10x,20x,25x,50x,75x,100x arasında EŞİT ağırlıklı
      - Her OI artışının yarısı LONG, yarısı SHORT varsayılır (yön verisi yok)
      - Bakım marjini: %0.5
    """
    if len(oi_history) < 2 or len(klines) < 2:
        return []

    prices = [float(k[4]) for k in klines]
    oi_values = [float(o["sumOpenInterest"]) for o in oi_history]
    n = min(len(prices), len(oi_values))
    if n < 2:
        return []
    prices = prices[-n:]
    oi_values = oi_values[-n:]

    LEVERAGE_TIERS = [5, 10, 20, 25, 50, 75, 100]
    MAINTENANCE_MARGIN = 0.005
    tier_weight = 1.0 / len(LEVERAGE_TIERS)

    current_price = prices[-1]
    price_min = current_price * 0.5
    price_max = current_price * 1.5
    bin_size = (price_max - price_min) / bins
    liq_volume = [0.0] * bins

    for i in range(1, n):
        delta = oi_values[i] - oi_values[i - 1]
        if delta <= 0:
            continue  # sadece YENİ pozisyon açılışları (pozitif delta) dikkate alınır
        entry_price = prices[i]

        for lev in LEVERAGE_TIERS:
            long_liq_price = entry_price * (1 - 1 / lev + MAINTENANCE_MARGIN)
            idx = int((long_liq_price - price_min) / bin_size)
            if 0 <= idx < bins:
                liq_volume[idx] += delta * tier_weight * 0.5  # yarısı LONG varsayımı

            short_liq_price = entry_price * (1 + 1 / lev - MAINTENANCE_MARGIN)
            idx2 = int((short_liq_price - price_min) / bin_size)
            if 0 <= idx2 < bins:
                liq_volume[idx2] += delta * tier_weight * 0.5  # yarısı SHORT varsayımı

    ranked = sorted(range(bins), key=lambda i: liq_volume[i], reverse=True)[:5]
    levels = sorted(price_min + (i + 0.5) * bin_size for i in ranked if liq_volume[i] > 0)
    return levels


def get_target_level(direction: str, current_price: float, vp_levels: list[float],
                      liq_levels: list[float] | None = None) -> float | None:
    """LONG için en yakın ALTTAKİ, SHORT için en yakın ÜSTTEKİ hedef seviyeyi döner.
    Hem hacim profili (gerçek veri) hem tahmini likidasyon kümeleri (model/tahmin)
    birleştirilip en yakın aday seçilir. Hiçbiri yoksa None döner (aday açılmaz)."""
    combined = list(vp_levels or [])
    if liq_levels:
        combined.extend(liq_levels)
    if not combined:
        return None
    if direction == "LONG":
        below = [lv for lv in combined if lv < current_price]
        return max(below) if below else None
    else:  # SHORT
        above = [lv for lv in combined if lv > current_price]
        return min(above) if above else None


# ---------------------------------------------------------------------------
# SİNYAL MANTIĞI
# ---------------------------------------------------------------------------
def compute_signal_score(snapshot: dict, direction: str) -> int:
    """
    0-100 arası sinyal gücü puanı. Ağırlıklar:
      - OI artış şiddeti      : %40
      - Funding şiddeti       : %30
      - Hacim büyüklüğü       : %20
      - Long/Short oran teyidi: %10 (bonus)
    """
    oi_ratio = snapshot["oi_change_pct"] / (2 * OI_RISE_THRESHOLD)
    oi_score = min(max(oi_ratio, 0.0), 1.0) * 100

    funding_ratio = abs(snapshot["funding_rate"]) / FUNDING_CEILING
    funding_score = min(max(funding_ratio, 0.0), 1.0) * 100

    volume_ratio = snapshot["quote_volume_24h"] / (5 * MIN_24H_QUOTE_VOLUME_USDT)
    volume_score = min(max(volume_ratio, 0.0), 1.0) * 100

    ratio_score = 50.0  # veri yoksa nötr
    top_ratio = snapshot.get("top_long_short_ratio")
    if top_ratio is not None:
        if direction == "LONG" and top_ratio > 1.0:
            ratio_score = 100.0
        elif direction == "SHORT" and top_ratio < 1.0:
            ratio_score = 100.0
        else:
            ratio_score = 0.0  # top traderlar sinyalin TERSİ yönde pozisyonlu

    total = (oi_score * 0.40) + (funding_score * 0.30) + (volume_score * 0.20) + (ratio_score * 0.10)
    return round(total)


def compute_signal(snapshot: dict) -> str | None:
    # --- Kalite filtreleri (gürültülü/düşük likiditeli sinyalleri ele) ---
    if snapshot["quote_volume_24h"] < MIN_24H_QUOTE_VOLUME_USDT:
        return None
    if snapshot["oi_value_usd"] < MIN_OPEN_INTEREST_USDT:
        return None
    if abs(snapshot["funding_rate"]) > FUNDING_CEILING:
        return None  # aşırı kalabalık pozisyon -> geç kalınmış, tersine dönüş riski yüksek

    # --- Ana sinyal mantığı ---
    if snapshot["oi_change_pct"] < OI_RISE_THRESHOLD:
        return None
    if snapshot["funding_rate"] > FUNDING_EPS:
        return "LONG"
    if snapshot["funding_rate"] < -FUNDING_EPS:
        return "SHORT"
    return None


# ---------------------------------------------------------------------------
# DURUM (STATE) YÖNETİMİ
# ---------------------------------------------------------------------------
def load_json(path: str, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def ensure_signals_csv() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(SIGNALS_CSV):
        with open(SIGNALS_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "symbol", "direction", "signal_time", "entry_price",
                "eval_time", "eval_price", "pnl_pct", "result", "exit_reason",
            ])


def append_signal_row(symbol: str, direction: str, signal_time: str, entry_price: float) -> None:
    with open(SIGNALS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([symbol, direction, signal_time, entry_price, "", "", "", "PENDING", ""])


def read_all_signal_rows() -> list[dict]:
    ensure_signals_csv()
    with open(SIGNALS_CSV, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_all_signal_rows(rows: list[dict]) -> None:
    fieldnames = ["symbol", "direction", "signal_time", "entry_price",
                  "eval_time", "eval_price", "pnl_pct", "result", "exit_reason"]
    with open(SIGNALS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def evaluate_pending_signals(current_prices: dict[str, float]) -> list[dict]:
    """Süresi dolmuş PENDING sinyalleri WIN/LOSS/FLAT olarak günceller.
    Yeni çözülen (resolved) satırları döner (e-posta özetinde kullanmak için)."""
    rows = read_all_signal_rows()
    now = datetime.now(timezone.utc)
    newly_resolved = []

    for row in rows:
        if row["result"] != "PENDING":
            continue
        signal_time = datetime.fromisoformat(row["signal_time"])
        elapsed_minutes = (now - signal_time).total_seconds() / 60.0

        symbol = row["symbol"]
        current_price = current_prices.get(symbol)
        if current_price is None:
            continue  # bu tur veri gelmedi, sonraki turda tekrar denenir

        entry_price = float(row["entry_price"])
        direction_mult = 1 if row["direction"] == "LONG" else -1
        pnl_pct = direction_mult * (current_price / entry_price - 1)

        # --- STOP-LOSS: süre dolmasa bile fiyat esik asildiysa HEMEN kapat ---
        if pnl_pct <= STOP_LOSS_PCT:
            row["eval_time"] = now.isoformat()
            row["eval_price"] = str(current_price)
            row["pnl_pct"] = f"{pnl_pct:.5f}"
            row["result"] = "LOSS"
            row["exit_reason"] = "STOP_LOSS"
            newly_resolved.append(row)
            continue

        # --- Süre dolmadıysa ve stop'a çarpmadıysa, beklemeye devam ---
        if elapsed_minutes < EVAL_MINUTES:
            continue

        # --- Süre doldu, normal WIN/LOSS/FLAT değerlendirmesi ---
        if pnl_pct >= SUCCESS_THRESHOLD:
            result = "WIN"
        elif pnl_pct <= -SUCCESS_THRESHOLD:
            result = "LOSS"
        else:
            result = "FLAT"

        row["eval_time"] = now.isoformat()
        row["eval_price"] = str(current_price)
        row["pnl_pct"] = f"{pnl_pct:.5f}"
        row["result"] = result
        row["exit_reason"] = "TIME_EXIT"
        newly_resolved.append(row)

    write_all_signal_rows(rows)
    return newly_resolved


def update_performance_summary() -> dict:
    rows = read_all_signal_rows()
    resolved = [r for r in rows if r["result"] in ("WIN", "LOSS", "FLAT")]
    wins = [r for r in resolved if r["result"] == "WIN"]
    losses = [r for r in resolved if r["result"] == "LOSS"]
    flats = [r for r in resolved if r["result"] == "FLAT"]
    pending = [r for r in rows if r["result"] == "PENDING"]

    decisive = len(wins) + len(losses)
    win_rate = (len(wins) / decisive * 100) if decisive > 0 else None

    avg_win_pct = (sum(float(r["pnl_pct"]) for r in wins) / len(wins) * 100) if wins else None
    avg_loss_pct = (sum(float(r["pnl_pct"]) for r in losses) / len(losses) * 100) if losses else None

    # Expectancy: ortalama beklenen getiri/işlem (FLAT'lar dahil, gerçek dünyada hepsi işlem)
    all_resolved_pnls = [float(r["pnl_pct"]) for r in resolved]
    expectancy_pct = (sum(all_resolved_pnls) / len(all_resolved_pnls) * 100) if all_resolved_pnls else None

    summary = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_signals": len(rows),
        "pending": len(pending),
        "resolved": len(resolved),
        "wins": len(wins),
        "losses": len(losses),
        "flats": len(flats),
        "win_rate_pct": round(win_rate, 2) if win_rate is not None else None,
        "avg_win_pct": round(avg_win_pct, 3) if avg_win_pct is not None else None,
        "avg_loss_pct": round(avg_loss_pct, 3) if avg_loss_pct is not None else None,
        "expectancy_pct_per_trade": round(expectancy_pct, 3) if expectancy_pct is not None else None,
    }
    save_json(SUMMARY_JSON, summary)
    return summary


# ---------------------------------------------------------------------------
# İZLEME LİSTESİ (WATCHLIST) YÖNETİMİ
# ---------------------------------------------------------------------------
def load_watchlist() -> dict:
    """{'BTCUSDT_LONG': {'symbol':..,'direction':..,'target_level':..,'created_time':..}}"""
    return load_json(WATCHLIST_JSON, {})


def save_watchlist(watchlist: dict) -> None:
    save_json(WATCHLIST_JSON, watchlist)


def process_watchlist(watchlist: dict, current_prices: dict[str, float],
                       snapshots: dict[str, dict], now: datetime) -> tuple[list[dict], dict, list[str]]:
    """
    Mevcut izleme listesindeki adayları kontrol eder:
      - Fiyat hedef seviyeye ULAŞTIYSA -> aktive et (gerçek sinyal olarak döner)
      - Süre (WATCH_WINDOW_MINUTES) DOLDUYSA -> sil (sıfırla), sinyal yok
      - Aksi halde -> listede bekletmeye devam et
    Döner: (aktive_olan_sinyaller, guncellenmis_watchlist, kapanan_anahtarlar)
    kapanan_anahtarlar: cooldown uygulanması için (hem aktive hem sıfırlanan dahil)
    """
    activated = []
    remaining = {}
    closed_keys = []

    for key, candidate in watchlist.items():
        symbol = candidate["symbol"]
        direction = candidate["direction"]
        target = candidate["target_level"]
        created = datetime.fromisoformat(candidate["created_time"])
        elapsed_min = (now - created).total_seconds() / 60.0

        current_price = current_prices.get(symbol)
        if current_price is None:
            # bu turda veri gelmedi; süresi dolmadıysa listede tut
            if elapsed_min < WATCH_WINDOW_MINUTES:
                remaining[key] = candidate
            else:
                closed_keys.append(key)  # veri yok + süre doldu -> sıfırla
            continue

        touched = (current_price <= target) if direction == "LONG" else (current_price >= target)

        if touched:
            snap = snapshots.get(symbol, {})
            activated.append({
                "symbol": symbol,
                "direction": direction,
                "price": current_price,
                "target_level": target,
                "funding_rate": snap.get("funding_rate", 0.0),
                "oi_change_pct": snap.get("oi_change_pct", 0.0),
                "top_long_short_ratio": snap.get("top_long_short_ratio"),
                "score": candidate.get("score", 0),
            })
            closed_keys.append(key)
        elif elapsed_min >= WATCH_WINDOW_MINUTES:
            closed_keys.append(key)  # süre doldu, dokunmadı -> sıfırlanır
        else:
            remaining[key] = candidate  # hâlâ bekliyor

    return activated, remaining, closed_keys


def add_new_candidates(watchlist: dict, symbol: str, direction: str,
                        target_level: float, now: datetime, score: int = 0) -> None:
    key = f"{symbol}_{direction}"
    if key in watchlist:
        return  # zaten izleniyor
    watchlist[key] = {
        "symbol": symbol,
        "direction": direction,
        "target_level": target_level,
        "created_time": now.isoformat(),
        "score": score,
    }


# ---------------------------------------------------------------------------
# E-POSTA
# ---------------------------------------------------------------------------
def load_email_config() -> dict | None:
    if not os.path.exists(EMAIL_CONFIG_PATH):
        return None
    with open(EMAIL_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if "PLACEHOLDER" in cfg.get("sender_app_password", "") or cfg.get("sender_app_password", "").startswith("xxxx"):
        return None
    return cfg


def send_email(subject: str, body: str, cfg: dict) -> bool:
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = cfg["sender_email"]
    msg["To"] = cfg["recipient_email"]
    try:
        with smtplib.SMTP_SSL(cfg["smtp_server"], cfg["smtp_port"], timeout=20) as server:
            server.login(cfg["sender_email"], cfg["sender_app_password"])
            server.sendmail(cfg["sender_email"], [cfg["recipient_email"]], msg.as_string())
        return True
    except Exception as e:
        log(f"E-POSTA GÖNDERİM HATASI: {type(e).__name__}: {e}")
        return False


def build_email_body(new_signals: list[dict], resolved: list[dict], summary: dict,
                      watchlist: dict, now: datetime) -> str:
    lines = []
    lines.append("=== YENİ GİRİŞ SİNYALLERİ (fiyat hedef seviyeye ulaştı) ===")
    if new_signals:
        for s in sorted(new_signals, key=lambda x: x.get("score", 0), reverse=True):
            ratio_txt = f"{s['top_long_short_ratio']:.2f}" if s.get("top_long_short_ratio") is not None else "n/a"
            lines.append(f"  [PUAN {s.get('score', 0):>3}] {s['direction']:<5} {s['symbol']:<12} @ {s['price']:.6f}  "
                         f"(hedef seviye: {s['target_level']:.6f}, OI +{s['oi_change_pct']*100:.1f}%, "
                         f"funding {s['funding_rate']*100:.4f}%, top L/S oranı: {ratio_txt})")
    else:
        lines.append("  (yok)")

    lines.append(f"\n=== İZLEME LİSTESİ ({len(watchlist)} aday bekliyor) ===")
    lines.append("  (OI+funding tetiklendi, fiyat henüz hedef destek/direnç seviyesine ulaşmadı)")
    if watchlist:
        sorted_candidates = sorted(watchlist.values(), key=lambda c: c.get("score", 0), reverse=True)
        for c in sorted_candidates:
            created = datetime.fromisoformat(c["created_time"])
            elapsed_min = (now - created).total_seconds() / 60.0
            remaining_min = max(WATCH_WINDOW_MINUTES - elapsed_min, 0)
            lines.append(f"  [PUAN {c.get('score', 0):>3}] {c['direction']:<5} {c['symbol']:<12}  "
                         f"hedef: {c['target_level']:.6f}  "
                         f"(kalan süre: {remaining_min/60:.1f} saat)")
    else:
        lines.append("  (izlemede aday yok)")

    lines.append("\n=== YENİ SONUÇLANAN SİNYALLER (2 saat sonrası) ===")
    if resolved:
        for r in resolved:
            lines.append(f"  {r['result']:<5} {r['symbol']:<12} {r['direction']:<5} "
                         f"giriş={float(r['entry_price']):.6f} çıkış={float(r['eval_price']):.6f} "
                         f"pnl={float(r['pnl_pct'])*100:.2f}%")
    else:
        lines.append("  (yok)")

    lines.append("\n=== GENEL BAŞARI ÖZETİ (tüm zamanlar) ===")
    lines.append(f"  Toplam sinyal      : {summary['total_signals']}")
    lines.append(f"  Sonuçlanan         : {summary['resolved']}  (Bekleyen: {summary['pending']})")
    lines.append(f"  Kazanan / Kaybeden : {summary['wins']} / {summary['losses']}  (Nötr: {summary['flats']})")
    wr = summary["win_rate_pct"]
    lines.append(f"  Win Rate           : {wr if wr is not None else 'yeterli veri yok'}%")
    aw = summary.get("avg_win_pct")
    al = summary.get("avg_loss_pct")
    exp = summary.get("expectancy_pct_per_trade")
    lines.append(f"  Ort. Kazanç/Kayıp  : {aw if aw is not None else '-'}% / {al if al is not None else '-'}%")
    lines.append(f"  Beklenti (expectancy): {exp if exp is not None else 'yeterli veri yok'}% / işlem "
                 f"{'(POZİTİF = uzun vadede karlı olabilir)' if exp is not None and exp > 0 else ''}"
                 f"{'(NEGATİF = win rate yüksek olsa bile zarar ediyor)' if exp is not None and exp <= 0 else ''}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# ANA AKIŞ
# ---------------------------------------------------------------------------
def main(dry_run: bool = False) -> None:
    scan_start = time.time()
    log(f"### MONITOR_VERSION = {MONITOR_VERSION} ###")
    ensure_signals_csv()
    watchlist = load_watchlist()
    last_closed = load_json(LAST_SIGNALS_JSON, {})  # cooldown: {"BTCUSDT_LONG": "iso_timestamp"}

    session = requests.Session()
    log("Tüm USDT-M perpetual semboller çekiliyor...")
    symbols = get_all_perpetual_symbols(session)
    log(f"{len(symbols)} sembol bulundu. Taranıyor...")

    current_prices: dict[str, float] = {}
    snapshots: dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    errors = 0
    new_candidates_count = 0

    for i, symbol in enumerate(symbols, 1):
        snap = fetch_symbol_snapshot(session, symbol)
        if snap is None:
            errors += 1
            time.sleep(REQUEST_SLEEP)
            continue

        current_prices[symbol] = snap["price"]
        snapshots[symbol] = snap

        # OI+funding tetiklendi mi? (henüz alarm değil, sadece ADAY oluşturur)
        direction = compute_signal(snap)
        if direction is not None:
            key = f"{symbol}_{direction}"

            in_cooldown = False
            last_ts = last_closed.get(key)
            if last_ts:
                elapsed = (now - datetime.fromisoformat(last_ts)).total_seconds() / 60.0
                in_cooldown = elapsed < COOLDOWN_MINUTES

            if not in_cooldown and key not in watchlist:
                # Sadece aday olan (filtreden geçen) semboller için ekstra
                # OI geçmişi çekip TAHMİNİ likidasyon heatmap'i hesapla
                oi_history_long = fetch_oi_history_long(session, symbol)
                liq_levels = compute_liquidation_heatmap(snap["klines"], oi_history_long)

                target = get_target_level(direction, snap["price"], snap["vp_levels"], liq_levels)
                if target is not None:
                    score = compute_signal_score(snap, direction)
                    add_new_candidates(watchlist, symbol, direction, target, now, score)
                    new_candidates_count += 1

        if i % 50 == 0:
            log(f"  ... {i}/{len(symbols)} tarandı")
        time.sleep(REQUEST_SLEEP)

    log(f"Tarama bitti. {len(symbols)-errors}/{len(symbols)} başarılı, "
        f"{new_candidates_count} yeni izleme adayı eklendi.")

    scan_duration_sec = time.time() - scan_start
    scan_duration_min = scan_duration_sec / 60.0
    log(f"TOPLAM TARAMA SÜRESİ: {scan_duration_min:.1f} dakika ({scan_duration_sec:.0f} saniye)")
    if scan_duration_min > 13:
        log(f"UYARI: Tarama 13 dakikayı geçti (15dk'lık pencerenin sınırına yakın). "
            f"REQUEST_SLEEP'i düşürmeyi ({REQUEST_SLEEP}s -> daha az) veya Task Scheduler "
            f"aralığını 20-30 dakikaya çıkarmayı düşün.")

    # --- İzleme listesini işle: hedefe ulaşan var mı? süresi dolan var mı? ---
    activated_signals, watchlist, closed_keys = process_watchlist(watchlist, current_prices, snapshots, now)
    log(f"İzleme listesi işlendi: {len(activated_signals)} sinyal AKTİVE oldu "
        f"(fiyat hedefe ulaştı), {len(watchlist)} aday hâlâ bekliyor, "
        f"{len(closed_keys)} aday kapandı (aktive+sıfırlanan).")

    for key in closed_keys:
        last_closed[key] = now.isoformat()

    if not dry_run:
        for s in activated_signals:
            append_signal_row(s["symbol"], s["direction"], now.isoformat(), s["price"])
        save_watchlist(watchlist)
        save_json(LAST_SIGNALS_JSON, last_closed)

    # --- Bekleyen eski sinyalleri değerlendir (2 saat dolmuş olanlar) ---
    resolved = evaluate_pending_signals(current_prices) if not dry_run else []
    summary = update_performance_summary() if not dry_run else {
        "total_signals": 0, "pending": 0, "resolved": 0, "wins": 0,
        "losses": 0, "flats": 0, "win_rate_pct": None,
    }

    # --- E-posta ---
    # HER TURDA e-posta at: watchlist doluysa (bilgi amaçlı), yeni aktivasyon/
    # sonuçlanma varsa (önemli), veya yeni aday eklendiyse (sistem çalışıyor teyidi)
    if activated_signals or resolved or new_candidates_count > 0 or len(watchlist) > 0:
        body = build_email_body(activated_signals, resolved, summary, watchlist, now)
        if activated_signals:
            subject = f"[Altcoin Monitor] {len(activated_signals)} yeni giriş, {len(resolved)} sonuçlanan"
        elif new_candidates_count > 0:
            subject = f"[Altcoin Monitor] {new_candidates_count} yeni izleme adayı eklendi ({len(watchlist)} toplam bekliyor)"
        else:
            subject = f"[Altcoin Monitor] {len(watchlist)} aday izlemede, yeni aktivasyon yok"
        print("\n" + "=" * 60)
        print(subject)
        print("=" * 60)
        print(body)
        print("=" * 60 + "\n")

        if dry_run:
            log("(--dry-run modunda: e-posta GÖNDERİLMEDİ, sadece konsola yazıldı, watchlist kaydedilmedi)")
        else:
            cfg = load_email_config()
            if cfg is None:
                log("UYARI: email_config.json bulunamadı ya da şifre hala placeholder. "
                    "E-posta gönderilemedi. email_config.json.example dosyasına bak.")
            else:
                ok = send_email(subject, body, cfg)
                log("E-posta gönderildi." if ok else "E-posta gönderilemedi (yukarıdaki hataya bak).")
    else:
        log(f"Aktive olan sinyal veya sonuçlanan işlem yok, e-posta atılmadı. "
            f"(İzleme listesinde {len(watchlist)} aday bekliyor)")


if __name__ == "__main__":
    dry_run_flag = "--dry-run" in sys.argv
    main(dry_run=dry_run_flag)
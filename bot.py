"""
Financial Markets Scalper for Kalshi. Buys cheap hourly contracts on
S&P 500, Nasdaq, Forex (EUR/USD, GBP/USD, USD/JPY, AUD/USD), and WTI Oil.
Same scalping strategy as the crypto bot — buy cheap near expiry, ride to settlement.
"""

import os, time, logging, traceback, math
from datetime import datetime, timezone
from flask import Flask, jsonify
from threading import Thread
import psycopg2
from psycopg2.extras import RealDictCursor
from kalshi_auth import KalshiAuth
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === CONFIG ===
KALSHI_HOST = os.environ.get('KALSHI_API_HOST', 'https://api.elections.kalshi.com')
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://kalshi:kalshi@localhost:5432/kalshi_scalper')
PORT = int(os.environ.get('PORT', 8082))
ENABLE_TRADING = os.environ.get('ENABLE_TRADING', 'false').lower() == 'true'

# === STRATEGY (matches crypto bot) ===
BUY_MIN = 0.01
BUY_MAX = 0.99
SELL_THRESHOLD = 1.50        # +150%
CONTRACTS = 1
MAX_POSITIONS = 50
MAX_PER_SERIES = 99999
CYCLE_SECONDS = 2            # fast — 2 seconds like crypto bot
STARTING_BALANCE = 100000.00
CASH_RESERVE = 0.50
TAKER_FEE_RATE = 0.07
MAX_MINS_TO_EXPIRY = 60      # hourly contracts — buy within 60 min
MAX_BUYS_PER_CYCLE = 1000

# === MARKET SERIES ===
# Hourly financial markets on Kalshi
FINANCIAL_SERIES = [
    # S&P 500
    'KXINXI', 'KXINXU',
    # Nasdaq 100
    'KXNASDAQ100U',
    # Forex
    'KXEURUSDH', 'KXGBPUSDH', 'KXAUDUSDH', 'KXUSDJPYH',
    # Oil
    'KXWTIH',
    # Extra crypto hourly (beyond what crypto bot covers)
    'KXBNB', 'KXBNBD', 'KXHYPE', 'KXHYPED',
]

SERIES_DISPLAY = {
    'KXINXI': 'S&P 500', 'KXINXU': 'S&P 500',
    'KXNASDAQ100U': 'Nasdaq',
    'KXEURUSDH': 'EUR/USD', 'KXGBPUSDH': 'GBP/USD',
    'KXAUDUSDH': 'AUD/USD', 'KXUSDJPYH': 'USD/JPY',
    'KXWTIH': 'WTI Oil',
    'KXBNB': 'BNB', 'KXBNBD': 'BNB',
    'KXHYPE': 'HYPE', 'KXHYPED': 'HYPE',
}

# === DATABASE ===

def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT,
                    side TEXT,
                    action TEXT,
                    price NUMERIC,
                    count INTEGER,
                    current_bid NUMERIC,
                    peak_bid NUMERIC,
                    pnl NUMERIC,
                    series TEXT,
                    mins_to_expiry NUMERIC,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            for col, typ in [('series', 'TEXT'), ('mins_to_expiry', 'NUMERIC'), ('peak_bid', 'NUMERIC')]:
                try:
                    cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
                except:
                    pass
    finally:
        conn.close()


# === INIT ===
init_db()
auth = KalshiAuth()
app = Flask(__name__)

current_hot_markets = []


def sf(val):
    try:
        return float(val) if val is not None else 0.0
    except:
        return 0.0


def kalshi_fee(price, count):
    return min(math.ceil(TAKER_FEE_RATE * count * price * (1 - price) * 100) / 100, 0.02 * count)


# === KALSHI API ===

def _make_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

session = _make_session()


def kalshi_get(path):
    url = f"{KALSHI_HOST}/trade-api/v2{path}"
    headers = auth.get_headers("GET", f"/trade-api/v2{path}")
    resp = session.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def kalshi_post(path, data):
    url = f"{KALSHI_HOST}/trade-api/v2{path}"
    headers = auth.get_headers("POST", f"/trade-api/v2{path}")
    headers['Content-Type'] = 'application/json'
    resp = session.post(url, headers=headers, json=data, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_market(ticker):
    try:
        resp = kalshi_get(f"/markets/{ticker}")
        return resp.get('market', resp)
    except:
        return None


def place_order(ticker, side, action, price, count):
    if not ENABLE_TRADING:
        logger.info(f"PAPER {action.upper()}: {ticker} {side} x{count} @ ${price:.2f}")
        return ('paper', count)
    price_cents = int(round(price * 100))
    try:
        resp = kalshi_post('/portfolio/orders', {
            'ticker': ticker, 'action': action, 'side': side,
            'type': 'limit', 'count': count,
            'yes_price' if side == 'yes' else 'no_price': price_cents,
        })
        order = resp.get('order', {})
        status = order.get('status', '')
        filled = order.get('place_count', 0) - order.get('remaining_count', 0)
        if filled <= 0:
            filled = count if status in ('executed', 'filled') else 0
        logger.info(f"ORDER {action.upper()}: {ticker} status={status} filled={filled}/{count}")
        return (order.get('order_id', ''), filled) if filled > 0 else None
    except Exception as e:
        logger.error(f"ORDER FAILED: {action.upper()} {ticker} -- {e}")
        return None


# === BALANCE ===

def get_kalshi_balance():
    try:
        resp = kalshi_get('/portfolio/balance')
        return resp.get('balance', 0) / 100.0
    except:
        return None


def get_balance():
    if ENABLE_TRADING:
        real = get_kalshi_balance()
        if real is not None:
            return real
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT price, count FROM trades WHERE action = 'buy'")
            buys = cur.fetchall()
            buy_cost = sum(sf(t['price']) * (t.get('count') or 1) for t in buys)
            cur.execute("SELECT pnl FROM trades WHERE pnl IS NOT NULL")
            pnl_data = cur.fetchall()
            total_pnl = sum(sf(t['pnl']) for t in pnl_data)
            return max(0, STARTING_BALANCE - buy_cost + total_pnl)
    except Exception as e:
        logger.error(f"Balance calc failed: {e}")
        return 0.0
    finally:
        conn.close()


def get_open_positions():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM trades WHERE action = 'buy' AND pnl IS NULL")
            return cur.fetchall()
    except Exception as e:
        logger.error(f"get_open_positions failed: {e}")
        return []
    finally:
        conn.close()


# === SELL LOGIC (same as crypto bot) ===

def check_sells():
    logger.info("--- SELL CHECK ---")
    open_positions = get_open_positions()
    if not open_positions:
        logger.info("No open positions")
        return

    logger.info(f"Checking {len(open_positions)} open positions")
    sold = 0
    expired = 0

    for trade in open_positions:
        ticker = trade['ticker']
        side = trade['side']
        entry_price = sf(trade['price'])
        count = trade.get('count') or 1

        if entry_price <= 0:
            continue

        market = get_market(ticker)
        if not market:
            continue

        status = market.get('status', '')
        result_val = market.get('result', '')

        # === SETTLED ===
        if result_val:
            buy_fee = kalshi_fee(entry_price, count)
            if result_val == side:
                sell_fee = kalshi_fee(1.0, count)
                pnl = round((1.0 - entry_price) * count - buy_fee - sell_fee, 4)
                reason = "WIN settled @$1.00"
            else:
                pnl = round(-entry_price * count - buy_fee, 4)
                reason = "LOSS settled"
            logger.info(f"SETTLED: {ticker} {side} | {reason} | pnl=${pnl:.4f}")
            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET pnl = %s WHERE id = %s", (float(pnl), trade['id']))
            except Exception as e:
                logger.error(f"Settle DB error: {e}")
            finally:
                conn.close()
            expired += 1
            continue

        if status in ('closed', 'settled', 'finalized'):
            logger.info(f"WAITING: {ticker} status={status}, no result yet")
            continue

        # Get current bid
        if side == 'yes':
            current_bid = sf(market.get('yes_bid_dollars', '0'))
        else:
            current_bid = sf(market.get('no_bid_dollars', '0'))

        # Update bid and peak in DB
        if current_bid > 0:
            current_peak = sf(trade.get('peak_bid') or 0)
            new_peak = max(current_peak, current_bid)
            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET current_bid = %s, peak_bid = %s WHERE id = %s",
                                (float(current_bid), float(new_peak), trade['id']))
            except:
                pass
            finally:
                conn.close()

        if current_bid <= 0:
            logger.info(f"SKIP: {ticker} bid=$0, waiting for settlement")
            continue

        gain = (current_bid - entry_price) / entry_price
        gain_pct = gain * 100

        logger.info(f"  POS: {ticker} {side} entry=${entry_price:.2f} bid=${current_bid:.2f} {gain_pct:+.0f}% x{count}")

        # Take profit
        if SELL_THRESHOLD is not None and gain >= SELL_THRESHOLD:
            buy_fee = kalshi_fee(entry_price, count)
            sell_fee = kalshi_fee(current_bid, count)
            pnl = round((current_bid - entry_price) * count - buy_fee - sell_fee, 4)

            logger.info(f"SELL: {ticker} {side} x{count} @ ${current_bid:.2f} | pnl=${pnl:.4f}")
            result = place_order(ticker, side, 'sell', current_bid, count)
            if not result:
                continue

            order_id, filled = result
            if filled < count:
                pnl = round((current_bid - entry_price) * filled - kalshi_fee(entry_price, filled) - kalshi_fee(current_bid, filled), 4)

            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET pnl = %s, current_bid = %s WHERE id = %s",
                                (float(pnl), float(current_bid), trade['id']))
            except Exception as e:
                logger.error(f"Sell DB error: {e}")
            finally:
                conn.close()
            sold += 1

    logger.info(f"SELL SUMMARY: sold={sold} expired={expired}")


# === BUY LOGIC (same as crypto bot) ===

def fetch_all_markets():
    all_markets = []
    for series in FINANCIAL_SERIES:
        cursor = None
        try:
            while True:
                url = f'/markets?series_ticker={series}&status=open&limit=200'
                if cursor:
                    url += f'&cursor={cursor}'
                resp = kalshi_get(url)
                batch = resp.get('markets', [])
                all_markets.extend(batch)
                cursor = resp.get('cursor')
                if not cursor or not batch:
                    break
        except Exception as e:
            if '404' not in str(e) and 'not found' not in str(e).lower():
                logger.error(f"Fetch {series} failed: {e}")
    logger.info(f"Fetched {len(all_markets)} markets from {len(FINANCIAL_SERIES)} series")
    return all_markets


def _get_volume(market):
    for key in ('volume', 'volume_24h'):
        val = market.get(key)
        if val is not None and val != '' and val != 0:
            try:
                return int(float(val))
            except:
                pass
    return 0


def _get_series(ticker):
    """Determine which series a ticker belongs to."""
    for s in FINANCIAL_SERIES:
        if ticker.upper().startswith(s):
            return s
    return ''


def buy_candidates(markets):
    balance = get_balance()
    open_positions = get_open_positions()
    logger.info(f"Balance: ${balance:.2f} | {len(open_positions)} positions open")

    deployable = balance * (1.0 - CASH_RESERVE)
    if deployable <= 1.0:
        logger.info(f"Deployable ${deployable:.2f} too low -- skipping buys")
        return

    candidates = []
    now = datetime.now(timezone.utc)

    for market in markets:
        ticker = market.get('ticker', '')

        # Expiry filter
        close_time = market.get('close_time') or market.get('expected_expiration_time')
        if close_time:
            try:
                close_dt = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
                mins_left = (close_dt - now).total_seconds() / 60
                if mins_left > MAX_MINS_TO_EXPIRY or mins_left < 0:
                    continue
            except:
                continue
        else:
            continue

        yes_ask = float(market.get('yes_ask_dollars') or '999')
        yes_bid = float(market.get('yes_bid_dollars') or '0')
        no_ask = float(market.get('no_ask_dollars') or '999')
        no_bid = float(market.get('no_bid_dollars') or '0')

        series = _get_series(ticker)

        # Buy cheapest side in range
        if yes_ask <= no_ask and BUY_MIN <= yes_ask <= BUY_MAX and yes_bid > 0:
            side, price, bid = 'yes', yes_ask, yes_bid
        elif BUY_MIN <= no_ask <= BUY_MAX and no_bid > 0:
            side, price, bid = 'no', no_ask, no_bid
        elif BUY_MIN <= yes_ask <= BUY_MAX and yes_bid > 0:
            side, price, bid = 'yes', yes_ask, yes_bid
        else:
            continue

        candidates.append({
            'ticker': ticker, 'side': side, 'price': price, 'bid': bid,
            'series': series, 'mins_left': mins_left,
        })

    candidates.sort(key=lambda x: x['price'])
    candidates = candidates[:MAX_BUYS_PER_CYCLE]
    logger.info(f"Found {len(candidates)} buy candidates")

    bought = 0
    for c in candidates:
        if bought >= MAX_BUYS_PER_CYCLE:
            break

        cost = c['price'] * CONTRACTS
        if cost > deployable:
            continue

        result = place_order(c['ticker'], c['side'], 'buy', c['price'], CONTRACTS)
        if not result:
            continue

        order_id, filled = result
        if filled <= 0:
            continue

        logger.info(f"BUY: {c['ticker']} {c['side']} x{filled} @ ${c['price']:.2f} mins_left={c['mins_left']:.0f}")
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO trades (ticker, side, action, price, count, current_bid, series, mins_to_expiry) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (c['ticker'], c['side'], 'buy', float(c['price']), filled,
                     float(c['bid']), c.get('series', ''), round(c.get('mins_left', 0), 1))
                )
        except Exception as e:
            logger.error(f"Buy DB insert failed: {e}")
        finally:
            conn.close()

        deployable -= cost
        bought += 1

    logger.info(f"Bought {bought} positions")


def update_hot_markets(markets):
    global current_hot_markets
    active = [m for m in markets if sf(m.get('yes_ask_dollars', '0')) < 0.99]
    by_vol = sorted(active, key=lambda m: _get_volume(m), reverse=True)[:15]
    current_hot_markets = [
        {'ticker': m.get('ticker', ''),
         'title': (m.get('subtitle', '') or m.get('title', ''))[:50],
         'yes_ask': sf(m.get('yes_ask_dollars', '0')),
         'no_ask': sf(m.get('no_ask_dollars', '0')),
         'volume': _get_volume(m)}
        for m in by_vol
    ]


# === MAIN CYCLE ===

_cycle = 0

def run_cycle():
    global _cycle
    _cycle += 1
    mode = "PAPER" if not ENABLE_TRADING else "LIVE"
    balance = get_balance()
    logger.info(f"=== CYCLE {_cycle} [{mode}] === Balance: ${balance:.2f}")

    check_sells()
    markets = fetch_all_markets()
    update_hot_markets(markets)
    buy_candidates(markets)

    balance = get_balance()
    logger.info(f"=== CYCLE END [{mode}] === Balance: ${balance:.2f}")


# === API ===

@app.route('/')
def health():
    return 'OK'


@app.route('/api/status')
def api_status():
    try:
        cash = get_balance()
        positions = get_open_positions()
        pos_val = sum(sf(t.get('current_bid', 0)) * (t.get('count') or 1) for t in positions)
        portfolio = cash + pos_val

        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT pnl, count FROM trades WHERE action = 'buy' AND pnl IS NOT NULL")
                resolved = cur.fetchall()
                cur.execute("SELECT count, price FROM trades WHERE action = 'buy'")
                all_buys = cur.fetchall()
        finally:
            conn.close()

        total_pnl = sum(sf(t['pnl']) for t in resolved)
        wins = sum(1 for t in resolved if sf(t['pnl']) > 0)
        losses = sum(1 for t in resolved if sf(t['pnl']) <= 0)
        win_rate = round(wins / max(wins + losses, 1) * 100, 1)
        avg_win = round(sum(sf(t['pnl']) for t in resolved if sf(t['pnl']) > 0) / max(wins, 1), 4)
        avg_loss = round(sum(sf(t['pnl']) for t in resolved if sf(t['pnl']) <= 0) / max(losses, 1), 4)
        win_pnl = sum(sf(t['pnl']) for t in resolved if sf(t['pnl']) > 0)
        total_contracts = sum((t.get('count') or 1) for t in all_buys)
        total_fees = round(sum(kalshi_fee(sf(t.get('price')), t.get('count') or 1) for t in all_buys), 4)

        round_cost = sum(sf(t.get('price')) * (t.get('count') or 1) for t in positions)
        round_pnl = round(pos_val - round_cost, 4)
        round_pct = round((round_pnl / round_cost * 100), 1) if round_cost > 0 else 0
        overall = round(portfolio - STARTING_BALANCE, 2)
        overall_pct = round((overall / STARTING_BALANCE * 100), 2)
        mode = "PAPER" if not ENABLE_TRADING else "LIVE"

        return jsonify({
            'portfolio': round(portfolio, 2), 'cash': round(cash, 2),
            'positions_value': round(pos_val, 2),
            'overall_pnl': overall, 'overall_pct': overall_pct,
            'round_pnl': round_pnl, 'round_pct': round_pct,
            'net_pnl': round(total_pnl, 4), 'total_fees': total_fees,
            'total_contracts': total_contracts,
            'wins': wins, 'losses': losses, 'win_rate': win_rate,
            'avg_win': avg_win, 'avg_loss': avg_loss,
            'open_count': len(positions), 'mode': mode, 'cycle': _cycle,
        })
    except Exception as e:
        logger.error(f"API status error: {e}")
        return jsonify({'portfolio': 0, 'cash': 0, 'mode': 'PAPER'})


@app.route('/api/open')
def api_open():
    try:
        positions = []
        for t in get_open_positions():
            price = sf(t.get('price'))
            current = sf(t.get('current_bid'))
            count = int(t.get('count') or 1)
            if price > 0 and current > 0:
                unrealized = round((current - price) * count, 4)
                gain_pct = round(((current - price) / price) * 100, 1)
            else:
                unrealized = 0
                gain_pct = 0
            series = t.get('series', '')
            display = SERIES_DISPLAY.get(series, series)
            positions.append({
                'ticker': t.get('ticker', ''), 'side': t.get('side', ''),
                'market': display, 'series': series,
                'count': count, 'entry': price, 'current_bid': current,
                'peak_bid': sf(t.get('peak_bid', 0)),
                'unrealized': unrealized, 'gain_pct': gain_pct,
                'mins_to_expiry': sf(t.get('mins_to_expiry')),
            })
        positions.sort(key=lambda x: x['gain_pct'], reverse=True)
        return jsonify(positions)
    except Exception as e:
        logger.error(f"API open error: {e}")
        return jsonify([])


@app.route('/api/trades')
def api_trades():
    try:
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM trades WHERE action = 'buy' AND pnl IS NOT NULL ORDER BY created_at DESC LIMIT 50")
                data = cur.fetchall()
        finally:
            conn.close()
        trades = []
        for t in data:
            entry = sf(t.get('price'))
            exit_price = sf(t.get('current_bid'))
            gain_pct = round(((exit_price - entry) / entry) * 100, 1) if entry > 0 else 0
            series = t.get('series', '')
            trades.append({
                'created_at': str(t.get('created_at', '')),
                'ticker': t.get('ticker', ''),
                'side': t.get('side', ''),
                'market': SERIES_DISPLAY.get(series, series),
                'count': t.get('count', 1),
                'entry': entry, 'exit': exit_price,
                'pnl': sf(t.get('pnl')), 'gain_pct': gain_pct,
            })
        return jsonify(trades)
    except Exception as e:
        logger.error(f"API trades error: {e}")
        return jsonify([])


@app.route('/api/hot')
def api_hot():
    return jsonify(current_hot_markets)


@app.route('/dashboard')
def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Financial Scalper Terminal</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700;800&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#06080d;--bg1:#0c1017;--bg2:#111820;--bg3:#1a2130;
  --border:#1a2235;--border2:#243050;
  --text:#c8d0e0;--text2:#6a7490;--text3:#3a4260;
  --green:#00e68a;--green2:#00cc7a;--green-bg:rgba(0,230,138,.06);
  --red:#ff4466;--red2:#ee3355;--red-bg:rgba(255,68,102,.06);
  --gold:#f0b040;--gold2:#e0a030;
  --blue:#4488ff;--cyan:#40d0e0;
}
body{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.5;min-height:100vh;display:flex;flex-direction:column}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
.animate-num{transition:all .4s cubic-bezier(.4,0,.2,1)}

.live-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;animation:pulse 1.8s ease-in-out infinite;vertical-align:middle}
.dot-paper{background:var(--gold);box-shadow:0 0 8px var(--gold)}
.dot-live{background:var(--green);box-shadow:0 0 8px var(--green)}

.header-bar{background:var(--bg1);border-bottom:1px solid var(--border);padding:10px 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
.header-left{display:flex;align-items:center;gap:16px}
.brand{font-size:13px;font-weight:700;color:var(--gold);letter-spacing:2px;text-transform:uppercase}
.mode-badge{font-size:10px;padding:3px 10px;border-radius:3px;font-weight:600;letter-spacing:1px}
.mode-paper{background:rgba(240,176,64,.15);color:var(--gold);border:1px solid rgba(240,176,64,.3)}
.mode-live{background:rgba(0,230,138,.15);color:var(--green);border:1px solid rgba(0,230,138,.3)}
.header-right{display:flex;align-items:center;gap:16px;font-size:10px;color:var(--text2)}
.countdown-box{color:var(--gold);font-weight:700;font-size:12px}

.main-wrap{flex:1;padding:16px 20px;max-width:1600px;margin:0 auto;width:100%}
.grid-layout{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:1100px){.grid-layout{grid-template-columns:1fr}}
.full-width{grid-column:1/-1}

.hero-card{background:var(--bg1);border:1px solid var(--border);border-radius:8px;padding:24px 32px;text-align:center;position:relative;overflow:hidden;margin-bottom:14px}
.hero-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--gold),transparent)}
.hero-label{font-size:10px;color:var(--text2);text-transform:uppercase;letter-spacing:2px;margin-bottom:6px}
.hero-value{font-size:40px;font-weight:800;letter-spacing:-1px;line-height:1.1}
.hero-sub{display:flex;justify-content:center;gap:32px;margin-top:14px;flex-wrap:wrap}
.hero-sub-item{text-align:center}
.hero-sub-label{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px}
.hero-sub-value{font-size:15px;font-weight:600;margin-top:2px}

.stats-bar{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:14px}
.stat-card{background:var(--bg1);border:1px solid var(--border);border-radius:6px;padding:12px 14px;position:relative;overflow:hidden}
.stat-card::after{content:'';position:absolute;bottom:0;left:0;right:0;height:1px}
.stat-card.accent-green::after{background:var(--green)}
.stat-card.accent-red::after{background:var(--red)}
.stat-card.accent-gold::after{background:var(--gold)}
.stat-card.accent-blue::after{background:var(--blue)}
.stat-label{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px}
.stat-value{font-size:15px;font-weight:700}

.panel{background:var(--bg1);border:1px solid var(--border);border-radius:8px;overflow:hidden;display:flex;flex-direction:column}
.panel-header{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;background:var(--bg2)}
.panel-header h2{color:var(--gold);font-size:11px;text-transform:uppercase;letter-spacing:1.5px;font-weight:600;display:flex;align-items:center;gap:8px}
.panel-header h2::before{content:'';display:inline-block;width:3px;height:12px;background:var(--gold);border-radius:1px}
.panel-header .count{color:var(--text2);font-size:10px}
.panel-body{max-height:380px;overflow-y:auto;flex:1}
.panel-body::-webkit-scrollbar{width:4px}
.panel-body::-webkit-scrollbar-track{background:var(--bg1)}
.panel-body::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}

table{width:100%;border-collapse:collapse;font-size:11px}
th{color:var(--text3);text-align:left;padding:8px 10px;border-bottom:1px solid var(--border);text-transform:uppercase;font-size:9px;letter-spacing:.8px;font-weight:600;position:sticky;top:0;background:var(--bg1);z-index:1}
td{padding:7px 10px;border-bottom:1px solid rgba(26,34,53,.5)}
tr{transition:background .15s ease}
tr:hover{background:var(--bg3) !important}
.green{color:var(--green)}.red{color:var(--red)}.gray{color:var(--text3)}.gold{color:var(--gold)}.cyan{color:var(--cyan)}

.status-bar{background:var(--bg1);border-top:1px solid var(--border);padding:8px 24px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;font-size:10px;color:var(--text3)}
.empty-state{color:var(--text3);text-align:center;padding:24px;font-size:11px;font-style:italic}
</style>
</head>
<body>

<div class="header-bar">
  <div class="header-left">
    <span class="brand">Financial Scalper</span>
    <span class="live-dot dot-paper" id="mode-dot"></span>
    <span class="mode-badge mode-paper" id="mode-badge">PAPER MODE</span>
  </div>
  <div class="header-right">
    <span>S&P 500 | Nasdaq | Forex | Oil</span><span>|</span>
    <span>Buy $0.01 - $0.99</span><span>|</span>
    <span>Sell +150%</span><span>|</span>
    <span>Cycle: <span class="countdown-box" id="hd-cycle">--</span></span>
  </div>
</div>

<div class="main-wrap">

<div class="hero-card full-width">
  <div class="hero-label">Overall Profit &amp; Loss</div>
  <div class="hero-value animate-num" id="hero-pnl">...</div>
  <div class="hero-sub">
    <div class="hero-sub-item"><div class="hero-sub-label">Unrealized</div><div class="hero-sub-value animate-num" id="h-unreal">...</div></div>
    <div class="hero-sub-item"><div class="hero-sub-label">Realized</div><div class="hero-sub-value animate-num" id="h-real">...</div></div>
    <div class="hero-sub-item"><div class="hero-sub-label">Fees Paid</div><div class="hero-sub-value animate-num" id="h-fees">...</div></div>
    <div class="hero-sub-item"><div class="hero-sub-label">Win Rate</div><div class="hero-sub-value animate-num" id="h-wr">...</div></div>
  </div>
</div>

<div class="stats-bar full-width">
  <div class="stat-card accent-blue"><div class="stat-label">Cash</div><div class="stat-value" id="st-cash">--</div></div>
  <div class="stat-card accent-gold"><div class="stat-label">Positions Value</div><div class="stat-value" id="st-posval">--</div></div>
  <div class="stat-card accent-green"><div class="stat-label">Record</div><div class="stat-value" id="st-record">--</div></div>
  <div class="stat-card accent-green"><div class="stat-label">Avg Win</div><div class="stat-value green" id="st-avgw">--</div></div>
  <div class="stat-card accent-red"><div class="stat-label">Avg Loss</div><div class="stat-value red" id="st-avgl">--</div></div>
  <div class="stat-card accent-blue"><div class="stat-label">Contracts</div><div class="stat-value" id="st-contracts">--</div></div>
  <div class="stat-card accent-gold"><div class="stat-label">Open</div><div class="stat-value" id="st-open">--</div></div>
</div>

<div class="grid-layout">

<div class="panel full-width">
  <div class="panel-header"><h2>Open Positions</h2><span class="count" id="op-count">0</span></div>
  <div class="panel-body">
    <table><thead><tr><th>Ticker</th><th>Market</th><th>Side</th><th>Qty</th><th>Entry</th><th>Bid</th><th>Peak</th><th>P&L</th></tr></thead><tbody id="op-tbody"></tbody></table>
  </div>
</div>

<div class="panel">
  <div class="panel-header"><h2>Recent Trades</h2></div>
  <div class="panel-body">
    <table><thead><tr><th>Time</th><th>Ticker</th><th>Market</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead><tbody id="tr-tbody"></tbody></table>
  </div>
</div>

<div class="panel">
  <div class="panel-header"><h2>Hot Markets</h2><span class="count" id="hot-count">0</span></div>
  <div class="panel-body">
    <table><thead><tr><th>Market</th><th>Yes</th><th>No</th><th>Vol</th></tr></thead><tbody id="hot-tbody"></tbody></table>
  </div>
</div>

</div>
</div>

<div class="status-bar">
  <span>Financial Scalper | S&P 500 + Nasdaq + Forex + Oil | Hourly Contracts | 2s Cycles</span>
  <span>Last: <span id="sb-time">--</span></span>
</div>

<script>
const $=s=>document.getElementById(s);
const pc=v=>v>0?'green':v<0?'red':'gray';
const fmt=v=>'$'+Math.abs(v).toFixed(2);
const pf=v=>(v>=0?'+$':'-$')+Math.abs(v).toFixed(2);

async function refresh(){
  try{
    const [st,op,tr,hot]=await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/open').then(r=>r.json()),
      fetch('/api/trades').then(r=>r.json()),
      fetch('/api/hot').then(r=>r.json()),
    ]);

    if(st.mode==='LIVE'){$('mode-dot').className='live-dot dot-live';$('mode-badge').className='mode-badge mode-live';$('mode-badge').textContent='LIVE';}

    const ov=st.overall_pnl||0;const ovp=st.overall_pct||0;
    $('hero-pnl').innerHTML=`<span class="${pc(ov)}">${pf(ov)} <span style="font-size:18px">(${ovp>=0?'+':''}${ovp.toFixed(2)}%)</span></span>`;
    const ur=st.round_pnl||0;const urp=st.round_pct||0;
    $('h-unreal').innerHTML=`<span class="${pc(ur)}">${pf(ur)} (${urp>=0?'+':''}${urp}%)</span>`;
    $('h-real').innerHTML=`<span class="${pc(st.net_pnl||0)}">${pf(st.net_pnl||0)}</span>`;
    $('h-fees').innerHTML=`<span class="gold">$${(st.total_fees||0).toFixed(2)}</span>`;
    $('h-wr').innerHTML=st.win_rate!=null?`<span class="${st.win_rate>50?'green':st.win_rate>0?'red':'gray'}">${st.win_rate}%</span>`:'--';
    $('hd-cycle').textContent=st.cycle||'--';

    $('st-cash').textContent=fmt(st.cash||0);
    $('st-posval').textContent=fmt(st.positions_value||0);
    $('st-record').innerHTML=`<span class="green">${st.wins||0}W</span> / <span class="red">${st.losses||0}L</span>`;
    $('st-avgw').textContent=st.avg_win?pf(st.avg_win):'--';
    $('st-avgl').textContent=st.avg_loss?pf(st.avg_loss):'--';
    $('st-contracts').textContent=st.total_contracts||0;
    $('st-open').textContent=st.open_count||0;

    $('op-count').textContent=op.length;
    $('op-tbody').innerHTML=op.map(p=>{
      const cls=pc(p.unrealized);
      return `<tr><td>${p.ticker}</td><td class="gold">${p.market||''}</td><td>${p.side}</td><td>x${p.count}</td><td>$${p.entry?.toFixed(2)}</td><td>$${p.current_bid?.toFixed(2)}</td><td>$${p.peak_bid?.toFixed(2)||'--'}</td><td class="${cls}">${pf(p.unrealized)} (${p.gain_pct>0?'+':''}${p.gain_pct}%)</td></tr>`;
    }).join('')||'<tr><td colspan=8 class="empty-state">No positions</td></tr>';

    $('tr-tbody').innerHTML=tr.map(t=>{
      const cls=pc(t.pnl);
      return `<tr><td style="font-size:9px">${new Date(t.created_at).toLocaleString()}</td><td>${t.ticker}</td><td class="gold">${t.market||''}</td><td>${t.side}</td><td>$${t.entry?.toFixed(2)}</td><td>$${t.exit?.toFixed(2)}</td><td class="${cls}">${pf(t.pnl)} (${t.gain_pct>0?'+':''}${t.gain_pct}%)</td></tr>`;
    }).join('')||'<tr><td colspan=7 class="empty-state">No trades yet</td></tr>';

    $('hot-count').textContent=hot.length;
    $('hot-tbody').innerHTML=hot.map(m=>`<tr><td title="${m.title}">${m.ticker}</td><td>$${m.yes_ask?.toFixed(2)}</td><td>$${m.no_ask?.toFixed(2)}</td><td>${m.volume?.toLocaleString()}</td></tr>`).join('')||'<tr><td colspan=4 class="empty-state">No markets</td></tr>';

    $('sb-time').textContent=new Date().toLocaleTimeString();
  }catch(e){console.error(e)}
}
refresh();setInterval(refresh,5000);
</script>
</body>
</html>"""


def bot_loop():
    mode = "PAPER" if not ENABLE_TRADING else "LIVE"
    sell_str = f"+{SELL_THRESHOLD*100:.0f}%" if SELL_THRESHOLD else "settlement"
    logger.info(f"=== FINANCIAL SCALPER [{mode}] ===")
    logger.info(f"Buy ${BUY_MIN}-${BUY_MAX}, sell {sell_str}, {CONTRACTS} contracts, {CASH_RESERVE*100:.0f}% reserve")
    logger.info(f"Series: {FINANCIAL_SERIES}")
    logger.info(f"Markets: S&P 500, Nasdaq, EUR/USD, GBP/USD, AUD/USD, USD/JPY, WTI Oil, BNB, HYPE")

    while True:
        try:
            run_cycle()
        except Exception as e:
            logger.error(f"Cycle error: {e}")
            traceback.print_exc()
        time.sleep(CYCLE_SECONDS)


if __name__ == '__main__':
    bot_thread = Thread(target=bot_loop, daemon=True)
    bot_thread.start()
    app.run(host='0.0.0.0', port=PORT)

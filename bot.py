"""
Weather Scalper for Kalshi. Buys cheap near-expiry weather contracts
where the outcome is nearly certain based on real-time NWS observations.
Same scalping strategy as the crypto bot but for weather.

The edge: current temp at the station already tells us the answer.
If it's 85F at 3pm in Phoenix and there's a "will it hit 80F?" contract
at $0.03, that's free money — it already happened.
"""

import os, time, logging, traceback, math, re
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify
from threading import Thread
import psycopg2
from psycopg2.extras import RealDictCursor
from kalshi_auth import KalshiAuth
from realtime import fetch_all_observations, STATIONS
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

# === SCALPING STRATEGY ===
BUY_MIN = 0.01              # buy contracts as cheap as $0.01
BUY_MAX = 0.50              # don't buy above $0.50
SELL_THRESHOLD = 1.50        # +150% take profit (like crypto bot)
CONTRACTS = 100              # contracts per trade
MAX_POSITIONS = 50           # max open positions
MAX_PER_TICKER = 3           # max 3 buys on same ticker
CYCLE_SECONDS = 10           # fast cycles — 10 seconds
STARTING_BALANCE = 10000.00
CASH_RESERVE = 0.30
TAKER_FEE_RATE = 0.07
MAX_HOURS_TO_EXPIRY = 8      # only buy contracts expiring within 8 hours
MIN_CONFIDENCE = 0.85         # need 85% confidence the outcome is decided

# UTC offsets for each city (standard time / daylight time)
# Used for accurate local hour estimation
CITY_UTC_OFFSET = {
    'NYC': -5, 'LGA': -5, 'CHI': -6, 'ORD': -6, 'LA': -8, 'MIA': -5,
    'DEN': -7, 'ATL': -5, 'DFW': -6, 'DAL': -6, 'SEA': -8, 'PHX': -7,
    'DCA': -5, 'BOS': -5, 'CLT': -5, 'DTW': -5, 'HOU': -6, 'JAX': -5,
    'LAS': -8, 'MSP': -6, 'BNA': -6, 'MSY': -6, 'OKC': -6, 'PHL': -5,
    'AUS': -6, 'SAT': -6, 'SFO': -8, 'TPA': -5, 'BKF': -7,
}
# DST: Most US cities spring forward (except PHX). Mar-Nov add 1 hour.
DST_EXEMPT = {'PHX'}  # Arizona doesn't do DST

# All KXHIGH series
TEMP_SERIES = {
    'KXHIGHNY': 'NYC', 'KXHIGHLGA': 'LGA',
    'KXHIGHCHI': 'ORD', 'KXHIGHMDW': 'CHI',
    'KXHIGHLA': 'LA', 'KXHIGHMIA': 'MIA',
    'KXHIGHDEN': 'DEN', 'KXHIGHATL': 'ATL',
    'KXHIGHDFW': 'DFW', 'KXHIGHDAL': 'DAL',
    'KXHIGHSEA': 'SEA', 'KXHIGHPHX': 'PHX',
    'KXHIGHDCA': 'DCA', 'KXHIGHBOS': 'BOS',
    'KXHIGHCLT': 'CLT', 'KXHIGHDTW': 'DTW',
    'KXHIGHHOU': 'HOU', 'KXHIGHJAX': 'JAX',
    'KXHIGHLAS': 'LAS', 'KXHIGHMSP': 'MSP',
    'KXHIGHBNA': 'BNA', 'KXHIGHMSY': 'MSY',
    'KXHIGHOKC': 'OKC', 'KXHIGHPHL': 'PHL',
    'KXHIGHAUS': 'AUS', 'KXHIGHSAT': 'SAT',
    'KXHIGHSFO': 'SFO', 'KXHIGHTPA': 'TPA',
    'KXHIGHBKF': 'BKF',
    'NHIGHNY': 'NYC', 'NHIGHLGA': 'LGA',
    'NHIGHCHI': 'ORD', 'NHIGHMDW': 'CHI',
    'NHIGHLA': 'LA', 'NHIGHMIA': 'MIA',
    'NHIGHDEN': 'DEN', 'NHIGHATL': 'ATL',
    'NHIGHDFW': 'DFW', 'NHIGHDAL': 'DAL',
    'NHIGHSEA': 'SEA', 'NHIGHPHX': 'PHX',
    'NHIGHDCA': 'DCA', 'NHIGHBOS': 'BOS',
    'NHIGHCLT': 'CLT', 'NHIGHDTW': 'DTW',
    'NHIGHHOU': 'HOU', 'NHIGHJAX': 'JAX',
    'NHIGHLAS': 'LAS', 'NHIGHMSP': 'MSP',
    'NHIGHBNA': 'BNA', 'NHIGHMSY': 'MSY',
    'NHIGHOKC': 'OKC', 'NHIGHPHL': 'PHL',
    'NHIGHAUS': 'AUS', 'NHIGHSAT': 'SAT',
    'NHIGHSFO': 'SFO', 'NHIGHTPA': 'TPA',
    'NHIGHBKF': 'BKF',
}

ALL_SERIES = list(set(TEMP_SERIES.keys()))

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
                    pnl NUMERIC,
                    city TEXT,
                    station TEXT,
                    threshold NUMERIC,
                    current_temp NUMERIC,
                    day_high NUMERIC,
                    confidence NUMERIC,
                    hours_to_expiry NUMERIC,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            for col, typ in [
                ('city', 'TEXT'), ('station', 'TEXT'), ('threshold', 'NUMERIC'),
                ('current_temp', 'NUMERIC'), ('day_high', 'NUMERIC'),
                ('confidence', 'NUMERIC'), ('hours_to_expiry', 'NUMERIC'),
            ]:
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

current_obs = {}
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


# === LOCAL TIME HELPER ===

def get_local_hour(city_key):
    """Get approximate local hour for a city, accounting for DST."""
    now = datetime.now(timezone.utc)
    offset = CITY_UTC_OFFSET.get(city_key, -6)

    # Simple DST check: second Sunday in March to first Sunday in November
    month = now.month
    if city_key not in DST_EXEMPT and 3 <= month <= 10:
        offset += 1  # DST: spring forward

    local_hour = (now.hour + offset) % 24
    return local_hour


# === SCALPING LOGIC ===

def parse_market(ticker):
    ticker_upper = ticker.upper()
    for prefix, city in TEMP_SERIES.items():
        if ticker_upper.startswith(prefix):
            match = re.search(r'[BT](\d+\.?\d*)', ticker_upper)
            if match:
                return city, float(match.group(1))
    return None, None


def assess_confidence(city_key, threshold, side):
    """Determine confidence that this contract will settle in our favor.

    Uses real-time observations: if the daily high already exceeded the
    threshold, "yes above" is a lock. If it's late afternoon and temps
    are falling well below, "no above" is a lock.
    """
    if city_key not in current_obs:
        return 0.0

    obs = current_obs[city_key]
    current_temp = obs.get('temp_f')
    day_high = obs.get('day_high_f')

    if current_temp is None:
        return 0.0

    local_hour = get_local_hour(city_key)

    if side == 'yes':
        # Betting the high WILL be >= threshold

        # Already hit today? Lock.
        if day_high is not None and day_high >= threshold:
            return 0.99

        # Current temp is above threshold? Very likely.
        if current_temp >= threshold:
            return 0.97

        # Close and still warming (before 3pm)
        if current_temp >= threshold - 2 and local_hour < 15:
            return 0.88

        # Within 3F and early afternoon
        if current_temp >= threshold - 3 and local_hour < 14:
            return 0.80

        # Within 5F and still morning
        if current_temp >= threshold - 5 and local_hour < 12:
            return 0.65

        # Late day, hasn't hit, unlikely
        if local_hour >= 17 and day_high is not None and day_high < threshold - 3:
            return 0.05

        if local_hour >= 16 and day_high is not None and day_high < threshold:
            return 0.15

        return 0.3

    else:
        # Betting the high will NOT reach threshold (side=no)

        # Already hit — we lose
        if day_high is not None and day_high >= threshold:
            return 0.01

        if current_temp >= threshold:
            return 0.02

        # Late afternoon, well below — lock
        if local_hour >= 16 and current_temp < threshold - 3:
            return 0.96

        if local_hour >= 17 and current_temp < threshold - 2:
            return 0.95

        # Past peak, decent buffer
        if local_hour >= 15 and current_temp < threshold - 4:
            return 0.90

        # Afternoon, moderate buffer
        if local_hour >= 14 and current_temp < threshold - 5:
            return 0.85

        # Early afternoon, big buffer
        if local_hour >= 13 and current_temp < threshold - 8:
            return 0.80

        # Morning, too early
        if local_hour < 12 and current_temp < threshold - 10:
            return 0.60

        return 0.3


def check_sells():
    logger.info("--- SELL CHECK ---")
    positions = get_open_positions()
    if not positions:
        return

    sold = 0
    settled = 0

    for trade in positions:
        ticker = trade['ticker']
        side = trade['side']
        entry = sf(trade['price'])
        count = trade.get('count') or 1

        if entry <= 0:
            continue

        market = get_market(ticker)
        if not market:
            continue

        result_val = market.get('result', '')
        status = market.get('status', '')

        # Settled
        if result_val:
            buy_fee = kalshi_fee(entry, count)
            if result_val == side:
                sell_fee = kalshi_fee(1.0, count)
                pnl = round((1.0 - entry) * count - buy_fee - sell_fee, 4)
            else:
                pnl = round(-entry * count - buy_fee, 4)
            logger.info(f"SETTLED: {ticker} {side} {'WIN' if pnl > 0 else 'LOSS'} pnl=${pnl:.4f}")
            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET pnl = %s WHERE id = %s", (float(pnl), trade['id']))
            finally:
                conn.close()
            settled += 1
            continue

        if status in ('closed', 'settled', 'finalized'):
            continue

        # Current bid — only update if > 0
        if side == 'yes':
            bid = sf(market.get('yes_bid_dollars', '0'))
        else:
            bid = sf(market.get('no_bid_dollars', '0'))

        if bid > 0:
            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET current_bid = %s WHERE id = %s", (float(bid), trade['id']))
            except:
                pass
            finally:
                conn.close()
        else:
            bid = sf(trade.get('current_bid', 0))  # keep last known bid

        if bid <= 0:
            continue

        gain = (bid - entry) / entry

        # Take profit
        if SELL_THRESHOLD and gain >= SELL_THRESHOLD:
            buy_fee = kalshi_fee(entry, count)
            sell_fee = kalshi_fee(bid, count)
            pnl = round((bid - entry) * count - buy_fee - sell_fee, 4)
            result = place_order(ticker, side, 'sell', bid, count)
            if result:
                conn = get_db()
                try:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE trades SET pnl = %s, current_bid = %s WHERE id = %s",
                                    (float(pnl), float(bid), trade['id']))
                finally:
                    conn.close()
                sold += 1

    logger.info(f"SELL: sold={sold} settled={settled}")


def fetch_weather_markets():
    all_markets = []
    fetched = set()
    for series in ALL_SERIES:
        if series in fetched:
            continue
        try:
            cursor = None
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
            fetched.add(series)
        except:
            pass
    logger.info(f"Fetched {len(all_markets)} markets")
    return all_markets


def buy_candidates(markets):
    balance = get_balance()
    positions = get_open_positions()
    logger.info(f"Balance: ${balance:.2f} | {len(positions)} open")

    if len(positions) >= MAX_POSITIONS:
        return

    deployable = balance * (1.0 - CASH_RESERVE)
    if deployable <= 1.0:
        return

    now = datetime.now(timezone.utc)

    ticker_counts = {}
    for t in positions:
        tk = t.get('ticker', '')
        ticker_counts[tk] = ticker_counts.get(tk, 0) + 1

    candidates = []

    for market in markets:
        ticker = market.get('ticker', '')
        city_key, threshold = parse_market(ticker)
        if not city_key or not threshold:
            continue

        if ticker_counts.get(ticker, 0) >= MAX_PER_TICKER:
            continue

        close_time = market.get('close_time') or market.get('expected_expiration_time', '')
        if not close_time:
            continue
        try:
            close_dt = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
            hours_left = (close_dt - now).total_seconds() / 3600
            if hours_left > MAX_HOURS_TO_EXPIRY or hours_left < 0:
                continue
        except:
            continue

        yes_ask = float(market.get('yes_ask_dollars') or '999')
        no_ask = float(market.get('no_ask_dollars') or '999')

        for side, ask in [('yes', yes_ask), ('no', no_ask)]:
            if not (BUY_MIN <= ask <= BUY_MAX):
                continue

            confidence = assess_confidence(city_key, threshold, side)
            if confidence < MIN_CONFIDENCE:
                continue

            obs = current_obs.get(city_key, {})
            candidates.append({
                'ticker': ticker, 'side': side, 'price': ask,
                'confidence': confidence, 'city': city_key,
                'station': STATIONS.get(city_key, {}).get('icao', ''),
                'threshold': threshold,
                'current_temp': obs.get('temp_f', 0),
                'day_high': obs.get('day_high_f', 0),
                'hours_left': hours_left,
            })

    candidates.sort(key=lambda x: (x['price'], -x['confidence']))
    logger.info(f"Found {len(candidates)} scalp candidates (>={MIN_CONFIDENCE*100:.0f}% confidence)")

    bought = 0
    for c in candidates:
        if len(positions) + bought >= MAX_POSITIONS:
            break

        cost = c['price'] * CONTRACTS
        if cost > deployable:
            continue

        logger.info(
            f"SCALP: {c['ticker']} {c['side']} x{CONTRACTS} @ ${c['price']:.2f} | "
            f"conf={c['confidence']:.0%} temp={c['current_temp']:.0f}F high={c['day_high']:.0f}F "
            f"thresh={c['threshold']:.0f}F {c['city']}({c['station']}) exp={c['hours_left']:.1f}h"
        )

        result = place_order(c['ticker'], c['side'], 'buy', c['price'], CONTRACTS)
        if not result:
            continue

        order_id, filled = result
        if filled <= 0:
            continue

        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO trades
                       (ticker, side, action, price, count, current_bid, city, station,
                        threshold, current_temp, day_high, confidence, hours_to_expiry)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (c['ticker'], c['side'], 'buy', float(c['price']), filled,
                     float(c['price']), c['city'], c['station'],
                     float(c['threshold']), float(c['current_temp']),
                     float(c['day_high'] or 0), float(c['confidence']),
                     round(c['hours_left'], 2))
                )
        except Exception as e:
            logger.error(f"DB insert failed: {e}")
        finally:
            conn.close()

        deployable -= cost
        bought += 1

    logger.info(f"Scalped {bought} positions")


def update_hot_markets(markets):
    global current_hot_markets
    active = [m for m in markets if sf(m.get('yes_ask_dollars', '0')) < 0.99]
    by_vol = sorted(active, key=lambda m: int(m.get('volume', 0) or 0), reverse=True)[:15]
    current_hot_markets = [
        {'ticker': m.get('ticker', ''), 'title': (m.get('subtitle', '') or m.get('title', ''))[:60],
         'yes_ask': sf(m.get('yes_ask_dollars', '0')), 'no_ask': sf(m.get('no_ask_dollars', '0')),
         'volume': int(m.get('volume', 0) or 0)}
        for m in by_vol
    ]


# === MAIN CYCLE ===

_cycle = 0

def run_cycle():
    global current_obs, _cycle
    _cycle += 1

    mode = "PAPER" if not ENABLE_TRADING else "LIVE"
    balance = get_balance()
    logger.info(f"=== CYCLE {_cycle} [{mode}] === Balance: ${balance:.2f}")

    # Refresh observations every 6 cycles (~60 sec)
    if _cycle % 6 == 1 or not current_obs:
        logger.info("Fetching real-time NWS observations...")
        current_obs = fetch_all_observations()
        for city, obs in sorted(current_obs.items()):
            logger.info(f"  {city}({obs.get('station','?')}): now={obs.get('temp_f', '?')}F high={obs.get('day_high_f', '?')}F")

    check_sells()
    markets = fetch_weather_markets()
    update_hot_markets(markets)
    buy_candidates(markets)

    balance = get_balance()
    logger.info(f"=== CYCLE END [{mode}] === Balance: ${balance:.2f}")


# === DASHBOARD API ===

@app.route('/')
def health():
    return 'OK'


@app.route('/api/status')
def api_status():
    try:
        cash = get_balance()
        positions = get_open_positions()
        pos_val = sum(sf(t.get('current_bid', 0)) * (t.get('count') or 1) for t in positions)

        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT pnl FROM trades WHERE action='buy' AND pnl IS NOT NULL")
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
        total_contracts = sum((t.get('count') or 1) for t in all_buys)
        total_fees = round(sum(kalshi_fee(sf(t.get('price')), t.get('count') or 1) for t in all_buys), 4)

        round_cost = sum(sf(t.get('price')) * (t.get('count') or 1) for t in positions)
        round_pnl = round(pos_val - round_cost, 4)
        round_pct = round((round_pnl / round_cost * 100), 1) if round_cost > 0 else 0
        overall = round((cash + pos_val) - STARTING_BALANCE, 2)
        overall_pct = round((overall / STARTING_BALANCE * 100), 2)

        return jsonify({
            'portfolio': round(cash + pos_val, 2), 'cash': round(cash, 2),
            'positions_value': round(pos_val, 2),
            'overall_pnl': overall, 'overall_pct': overall_pct,
            'round_pnl': round_pnl, 'round_pct': round_pct,
            'net_pnl': round(total_pnl, 4), 'total_fees': total_fees,
            'total_contracts': total_contracts,
            'wins': wins, 'losses': losses, 'win_rate': win_rate,
            'avg_win': avg_win, 'avg_loss': avg_loss,
            'open_count': len(positions),
            'mode': "PAPER" if not ENABLE_TRADING else "LIVE",
            'stations': len(current_obs), 'cycle': _cycle,
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
            unrealized = round((current - price) * count, 4) if price > 0 and current > 0 else 0
            gain_pct = round(((current - price) / price) * 100, 1) if price > 0 and current > 0 else 0
            positions.append({
                'ticker': t.get('ticker', ''), 'side': t.get('side', ''),
                'city': t.get('city', ''), 'station': t.get('station', ''),
                'threshold': sf(t.get('threshold')),
                'current_temp': sf(t.get('current_temp')),
                'day_high': sf(t.get('day_high')),
                'confidence': sf(t.get('confidence')),
                'hours_to_expiry': sf(t.get('hours_to_expiry')),
                'count': count, 'entry': price, 'current_bid': current,
                'unrealized': unrealized, 'gain_pct': gain_pct,
            })
        positions.sort(key=lambda x: x['gain_pct'], reverse=True)
        return jsonify(positions)
    except:
        return jsonify([])


@app.route('/api/trades')
def api_trades():
    try:
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM trades WHERE action='buy' AND pnl IS NOT NULL ORDER BY created_at DESC LIMIT 50")
                data = cur.fetchall()
        finally:
            conn.close()
        return jsonify([{
            'created_at': str(t.get('created_at', '')), 'ticker': t.get('ticker', ''),
            'side': t.get('side', ''), 'city': t.get('city', ''),
            'confidence': sf(t.get('confidence')),
            'count': t.get('count', 1),
            'entry': sf(t.get('price')), 'exit': sf(t.get('current_bid')),
            'pnl': sf(t.get('pnl')),
            'gain_pct': round(((sf(t.get('current_bid')) - sf(t.get('price'))) / sf(t.get('price'))) * 100, 1) if sf(t.get('price')) > 0 else 0,
        } for t in data])
    except:
        return jsonify([])


@app.route('/api/hot')
def api_hot():
    return jsonify(current_hot_markets)


@app.route('/api/observations')
def api_observations():
    result = {}
    for city, obs in current_obs.items():
        result[city] = {
            'name': STATIONS.get(city, {}).get('name', city),
            'station': obs.get('station', ''),
            'temp_f': obs.get('temp_f'),
            'day_high_f': obs.get('day_high_f'),
            'day_low_f': obs.get('day_low_f'),
            'wind_mph': obs.get('wind_mph'),
            'observed_at': obs.get('observed_at', ''),
        }
    return jsonify(result)


@app.route('/dashboard')
def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Weather Scalper Terminal</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700;800&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#06080d;--bg1:#0c1017;--bg2:#111820;--bg3:#1a2130;
  --border:#1a2235;--border2:#243050;
  --text:#c8d0e0;--text2:#6a7490;--text3:#3a4260;
  --green:#00e68a;--green2:#00cc7a;--green-bg:rgba(0,230,138,.06);
  --red:#ff4466;--red2:#ee3355;--red-bg:rgba(255,68,102,.06);
  --gold:#f0b040;--gold2:#e0a030;--gold-bg:rgba(240,176,64,.08);
  --blue:#4488ff;--cyan:#40d0e0;
}
body{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.5;min-height:100vh;display:flex;flex-direction:column}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
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
.stat-card.accent-cyan::after{background:var(--cyan)}
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

.obs-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px;padding:12px}
.obs-card{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:8px 10px}
.obs-city{font-size:10px;font-weight:700;color:var(--gold)}
.obs-station{font-size:8px;color:var(--text3)}
.obs-temp{font-size:20px;font-weight:800;color:var(--red);margin:2px 0}
.obs-detail{font-size:9px;color:var(--text2)}

.status-bar{background:var(--bg1);border-top:1px solid var(--border);padding:8px 24px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;font-size:10px;color:var(--text3)}
.empty-state{color:var(--text3);text-align:center;padding:24px;font-size:11px;font-style:italic}
</style>
</head>
<body>

<div class="header-bar">
  <div class="header-left">
    <span class="brand">Weather Scalper</span>
    <span class="live-dot dot-paper" id="mode-dot"></span>
    <span class="mode-badge mode-paper" id="mode-badge">PAPER MODE</span>
  </div>
  <div class="header-right">
    <span>Buy $0.01 - $0.50</span><span>|</span>
    <span>Sell +150%</span><span>|</span>
    <span>Confidence >= 85%</span><span>|</span>
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
    <div class="hero-sub-item"><div class="hero-sub-label">Total Fees</div><div class="hero-sub-value animate-num" id="h-fees">...</div></div>
    <div class="hero-sub-item"><div class="hero-sub-label">Win Rate</div><div class="hero-sub-value animate-num" id="h-wr">...</div></div>
    <div class="hero-sub-item"><div class="hero-sub-label">Stations</div><div class="hero-sub-value animate-num cyan" id="h-stations">...</div></div>
  </div>
</div>

<div class="stats-bar full-width">
  <div class="stat-card accent-blue"><div class="stat-label">Cash</div><div class="stat-value" id="st-cash">--</div></div>
  <div class="stat-card accent-gold"><div class="stat-label">Positions Value</div><div class="stat-value" id="st-posval">--</div></div>
  <div class="stat-card accent-green"><div class="stat-label">Record</div><div class="stat-value" id="st-record">--</div></div>
  <div class="stat-card accent-green"><div class="stat-label">Avg Win</div><div class="stat-value green" id="st-avgw">--</div></div>
  <div class="stat-card accent-red"><div class="stat-label">Avg Loss</div><div class="stat-value red" id="st-avgl">--</div></div>
  <div class="stat-card accent-cyan"><div class="stat-label">Contracts</div><div class="stat-value cyan" id="st-contracts">--</div></div>
  <div class="stat-card accent-gold"><div class="stat-label">Open</div><div class="stat-value" id="st-open">--</div></div>
</div>

<div class="grid-layout">

<!-- Live Observations -->
<div class="panel full-width">
  <div class="panel-header"><h2>Live Station Observations</h2><span class="count" id="obs-count">--</span></div>
  <div class="panel-body" style="max-height:300px" id="obs-body"><div class="empty-state">Loading...</div></div>
</div>

<!-- Open Positions -->
<div class="panel full-width">
  <div class="panel-header"><h2>Open Positions</h2><span class="count" id="op-count">0</span></div>
  <div class="panel-body">
    <table><thead><tr><th>Ticker</th><th>Side</th><th>City</th><th>Temp Now</th><th>Day High</th><th>Thresh</th><th>Conf</th><th>Exp</th><th>Qty</th><th>Entry</th><th>Bid</th><th>P&L</th></tr></thead><tbody id="op-tbody"></tbody></table>
  </div>
</div>

<!-- Trade History + Hot Markets -->
<div class="panel">
  <div class="panel-header"><h2>Recent Trades</h2></div>
  <div class="panel-body">
    <table><thead><tr><th>Time</th><th>Ticker</th><th>Side</th><th>City</th><th>Qty</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead><tbody id="tr-tbody"></tbody></table>
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
  <span>Weather Scalper | NWS Real-Time Observations | 10s Cycles | Near-Expiry Only</span>
  <span>Last update: <span id="sb-time">--</span></span>
</div>

<script>
const $=s=>document.getElementById(s);
const pc=v=>v>0?'green':v<0?'red':'gray';
const fmt=v=>'$'+Math.abs(v).toFixed(2);
const pf=v=>(v>=0?'+$':'-$')+Math.abs(v).toFixed(2);

async function refresh(){
  try{
    const [st,op,tr,hot,obs]=await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/open').then(r=>r.json()),
      fetch('/api/trades').then(r=>r.json()),
      fetch('/api/hot').then(r=>r.json()),
      fetch('/api/observations').then(r=>r.json()),
    ]);

    if(st.mode==='LIVE'){$('mode-dot').className='live-dot dot-live';$('mode-badge').className='mode-badge mode-live';$('mode-badge').textContent='LIVE';}

    // Hero
    const ov=st.overall_pnl||0;const ovp=st.overall_pct||0;
    $('hero-pnl').innerHTML=`<span class="${pc(ov)}">${pf(ov)} <span style="font-size:18px">(${ovp>=0?'+':''}${ovp.toFixed(2)}%)</span></span>`;
    const ur=st.round_pnl||0;const urp=st.round_pct||0;
    $('h-unreal').innerHTML=`<span class="${pc(ur)}">${pf(ur)} (${urp>=0?'+':''}${urp}%)</span>`;
    const re=st.net_pnl||0;
    $('h-real').innerHTML=`<span class="${pc(re)}">${pf(re)}</span>`;
    $('h-fees').innerHTML=`<span class="gold">$${(st.total_fees||0).toFixed(2)}</span>`;
    $('h-wr').innerHTML=st.win_rate!=null?`<span class="${st.win_rate>50?'green':st.win_rate>0?'red':'gray'}">${st.win_rate}%</span>`:'--';
    $('h-stations').textContent=st.stations||0;
    $('hd-cycle').textContent=st.cycle||'--';

    // Stats
    $('st-cash').textContent=fmt(st.cash||0);
    $('st-posval').textContent=fmt(st.positions_value||0);
    $('st-record').innerHTML=`<span class="green">${st.wins||0}W</span> / <span class="red">${st.losses||0}L</span>`;
    $('st-avgw').textContent=st.avg_win?pf(st.avg_win):'--';
    $('st-avgl').textContent=st.avg_loss?pf(st.avg_loss):'--';
    $('st-contracts').textContent=st.total_contracts||0;
    $('st-open').textContent=st.open_count||0;

    // Observations
    const obsKeys=Object.keys(obs).sort();
    $('obs-count').textContent=obsKeys.length+' stations';
    let oH='<div class="obs-grid">';
    for(const k of obsKeys){
      const o=obs[k];
      oH+=`<div class="obs-card"><div class="obs-city">${o.name}</div><div class="obs-station">${o.station}</div><div class="obs-temp">${o.temp_f!=null?o.temp_f.toFixed(0)+'°F':'--'}</div><div class="obs-detail">High: ${o.day_high_f!=null?o.day_high_f.toFixed(0)+'°':'--'} Low: ${o.day_low_f!=null?o.day_low_f.toFixed(0)+'°':'--'}</div></div>`;
    }
    oH+='</div>';
    $('obs-body').innerHTML=oH||'<div class="empty-state">No observations</div>';

    // Positions
    $('op-count').textContent=op.length;
    $('op-tbody').innerHTML=op.map(p=>{
      const cls=pc(p.unrealized);
      return `<tr><td>${p.ticker}</td><td>${p.side}</td><td class="gold">${p.city}</td><td>${p.current_temp?.toFixed(0)||'?'}°</td><td>${p.day_high?.toFixed(0)||'?'}°</td><td>${p.threshold?.toFixed(0)}°</td><td class="cyan">${(p.confidence*100).toFixed(0)}%</td><td>${p.hours_to_expiry?.toFixed(1)}h</td><td>x${p.count}</td><td>$${p.entry?.toFixed(2)}</td><td>$${p.current_bid?.toFixed(2)}</td><td class="${cls}">${pf(p.unrealized)} (${p.gain_pct>0?'+':''}${p.gain_pct}%)</td></tr>`;
    }).join('')||'<tr><td colspan=12 class="empty-state">No positions</td></tr>';

    // Trades
    $('tr-tbody').innerHTML=tr.map(t=>{
      const cls=pc(t.pnl);
      return `<tr><td style="font-size:9px">${new Date(t.created_at).toLocaleString()}</td><td>${t.ticker}</td><td>${t.side}</td><td class="gold">${t.city}</td><td>x${t.count}</td><td>$${t.entry?.toFixed(2)}</td><td>$${t.exit?.toFixed(2)}</td><td class="${cls}">${pf(t.pnl)} (${t.gain_pct>0?'+':''}${t.gain_pct}%)</td></tr>`;
    }).join('')||'<tr><td colspan=8 class="empty-state">No trades yet</td></tr>';

    // Hot
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
    logger.info(f"=== WEATHER SCALPER [{mode}] ===")
    logger.info(f"Buy ${BUY_MIN}-${BUY_MAX}, sell +{SELL_THRESHOLD*100:.0f}%, {CONTRACTS} contracts")
    logger.info(f"Min confidence {MIN_CONFIDENCE*100:.0f}%, max {MAX_HOURS_TO_EXPIRY}h to expiry")
    logger.info(f"Tracking {len(STATIONS)} NWS stations, {CYCLE_SECONDS}s cycles")

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

"""
Real-time weather observation fetcher.
Pulls current temps from NWS/NOAA for each station so we can
compare against market prices for near-expiry contracts.
"""

import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# NWS station IDs (drop the K prefix for some APIs)
STATIONS = {
    'NYC':  {'icao': 'KNYC', 'nws_id': 'NYC', 'name': 'Central Park',       'lat': 40.7790, 'lon': -73.9692},
    'LGA':  {'icao': 'KLGA', 'nws_id': 'LGA', 'name': 'LaGuardia',          'lat': 40.7772, 'lon': -73.8726},
    'CHI':  {'icao': 'KMDW', 'nws_id': 'MDW', 'name': 'Midway',             'lat': 41.7841, 'lon': -87.7551},
    'ORD':  {'icao': 'KORD', 'nws_id': 'ORD', 'name': 'OHare',              'lat': 41.9602, 'lon': -87.9316},
    'LA':   {'icao': 'KLAX', 'nws_id': 'LAX', 'name': 'LAX Airport',        'lat': 33.9382, 'lon': -118.3870},
    'MIA':  {'icao': 'KMIA', 'nws_id': 'MIA', 'name': 'Miami Intl',         'lat': 25.7881, 'lon': -80.3169},
    'DEN':  {'icao': 'KDEN', 'nws_id': 'DEN', 'name': 'Denver Intl',        'lat': 39.8466, 'lon': -104.6560},
    'ATL':  {'icao': 'KATL', 'nws_id': 'ATL', 'name': 'Hartsfield',         'lat': 33.6304, 'lon': -84.4221},
    'DFW':  {'icao': 'KDFW', 'nws_id': 'DFW', 'name': 'DFW Intl',           'lat': 32.8998, 'lon': -97.0403},
    'DAL':  {'icao': 'KDAL', 'nws_id': 'DAL', 'name': 'Love Field',         'lat': 32.8471, 'lon': -96.8518},
    'SEA':  {'icao': 'KSEA', 'nws_id': 'SEA', 'name': 'Sea-Tac',            'lat': 47.4502, 'lon': -122.3088},
    'PHX':  {'icao': 'KPHX', 'nws_id': 'PHX', 'name': 'Sky Harbor',         'lat': 33.4373, 'lon': -112.0078},
    'DCA':  {'icao': 'KDCA', 'nws_id': 'DCA', 'name': 'Reagan National',    'lat': 38.8512, 'lon': -77.0402},
    'BOS':  {'icao': 'KBOS', 'nws_id': 'BOS', 'name': 'Logan',              'lat': 42.3656, 'lon': -71.0096},
    'CLT':  {'icao': 'KCLT', 'nws_id': 'CLT', 'name': 'Charlotte Douglas',  'lat': 35.2144, 'lon': -80.9473},
    'DTW':  {'icao': 'KDTW', 'nws_id': 'DTW', 'name': 'Detroit Metro',      'lat': 42.2124, 'lon': -83.3534},
    'HOU':  {'icao': 'KHOU', 'nws_id': 'HOU', 'name': 'Hobby',              'lat': 29.6454, 'lon': -95.2789},
    'JAX':  {'icao': 'KJAX', 'nws_id': 'JAX', 'name': 'Jacksonville Intl',  'lat': 30.4941, 'lon': -81.6879},
    'LAS':  {'icao': 'KLAS', 'nws_id': 'LAS', 'name': 'Harry Reid',         'lat': 36.0840, 'lon': -115.1537},
    'MSP':  {'icao': 'KMSP', 'nws_id': 'MSP', 'name': 'MSP Airport',        'lat': 44.8848, 'lon': -93.2223},
    'BNA':  {'icao': 'KBNA', 'nws_id': 'BNA', 'name': 'Nashville Intl',     'lat': 36.1245, 'lon': -86.6782},
    'MSY':  {'icao': 'KMSY', 'nws_id': 'MSY', 'name': 'Armstrong Intl',     'lat': 29.9934, 'lon': -90.2580},
    'OKC':  {'icao': 'KOKC', 'nws_id': 'OKC', 'name': 'Will Rogers',        'lat': 35.3931, 'lon': -97.6007},
    'PHL':  {'icao': 'KPHL', 'nws_id': 'PHL', 'name': 'Philadelphia Intl',  'lat': 39.8721, 'lon': -75.2407},
    'AUS':  {'icao': 'KAUS', 'nws_id': 'AUS', 'name': 'Bergstrom',          'lat': 30.2099, 'lon': -97.6806},
    'SAT':  {'icao': 'KSAT', 'nws_id': 'SAT', 'name': 'San Antonio Intl',   'lat': 29.5337, 'lon': -98.4698},
    'SFO':  {'icao': 'KSFO', 'nws_id': 'SFO', 'name': 'SFO Airport',        'lat': 37.6213, 'lon': -122.3790},
    'TPA':  {'icao': 'KTPA', 'nws_id': 'TPA', 'name': 'Tampa Intl',         'lat': 27.9755, 'lon': -82.5332},
    'BKF':  {'icao': 'KBKF', 'nws_id': 'BKF', 'name': 'Buckley SFB',        'lat': 39.7017, 'lon': -104.7517},
}

_session = requests.Session()
_session.headers.update({'User-Agent': 'KalshiWeatherScalper/1.0 (contact@example.com)'})


def c_to_f(c):
    return c * 9.0 / 5.0 + 32.0


def fetch_current_obs(city_key):
    """Fetch latest observation from NWS API for a station.

    Returns dict: {temp_f, temp_c, wind_mph, humidity, observed_at, day_high_f, day_low_f}
    or None on failure.
    """
    if city_key not in STATIONS:
        return None

    station = STATIONS[city_key]
    icao = station['icao']

    try:
        # NWS observations API
        resp = _session.get(
            f'https://api.weather.gov/stations/{icao}/observations/latest',
            headers={'Accept': 'application/geo+json'},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        props = data.get('properties', {})

        temp_c = props.get('temperature', {}).get('value')
        temp_f = c_to_f(temp_c) if temp_c is not None else None

        wind_ms = props.get('windSpeed', {}).get('value')
        wind_mph = wind_ms * 2.237 if wind_ms is not None else None

        humidity = props.get('relativeHumidity', {}).get('value')

        observed_at = props.get('timestamp', '')

        return {
            'temp_f': round(temp_f, 1) if temp_f is not None else None,
            'temp_c': round(temp_c, 1) if temp_c is not None else None,
            'wind_mph': round(wind_mph, 1) if wind_mph is not None else None,
            'humidity': round(humidity, 1) if humidity is not None else None,
            'observed_at': observed_at,
            'station': icao,
            'name': station['name'],
        }
    except Exception as e:
        logger.error(f"Obs fetch failed for {city_key} ({icao}): {e}")
        return None


def fetch_day_extremes(city_key):
    """Fetch today's running high and low from recent observations.

    Scans last 24h of observations to find the day's high/low so far.
    """
    if city_key not in STATIONS:
        return None, None

    station = STATIONS[city_key]
    icao = station['icao']

    try:
        resp = _session.get(
            f'https://api.weather.gov/stations/{icao}/observations',
            params={'limit': 48},  # ~24h of hourly obs
            headers={'Accept': 'application/geo+json'},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        features = data.get('features', [])

        now = datetime.now(timezone.utc)
        today_str = now.strftime('%Y-%m-%d')

        day_high = -999
        day_low = 999

        for f in features:
            props = f.get('properties', {})
            ts = props.get('timestamp', '')
            if not ts:
                continue

            # Only count observations from today (LST approximation)
            obs_date = ts[:10]
            temp_c = props.get('temperature', {}).get('value')
            if temp_c is None:
                continue

            temp_f = c_to_f(temp_c)

            # Include today and yesterday (for LST overlap)
            if temp_f > day_high:
                day_high = temp_f
            if temp_f < day_low:
                day_low = temp_f

        if day_high == -999:
            day_high = None
        else:
            day_high = round(day_high, 1)

        if day_low == 999:
            day_low = None
        else:
            day_low = round(day_low, 1)

        return day_high, day_low
    except Exception as e:
        logger.error(f"Day extremes fetch failed for {city_key}: {e}")
        return None, None


def fetch_all_observations():
    """Fetch current obs for all stations.

    Returns dict: {city_key: {temp_f, day_high_f, day_low_f, ...}}
    """
    results = {}
    for city_key in STATIONS:
        obs = fetch_current_obs(city_key)
        if obs:
            day_high, day_low = fetch_day_extremes(city_key)
            obs['day_high_f'] = day_high
            obs['day_low_f'] = day_low
            results[city_key] = obs

    logger.info(f"Fetched observations for {len(results)}/{len(STATIONS)} stations")
    return results

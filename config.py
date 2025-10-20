"""
Local configuration for order flow monitoring.

Contains InfluxDB connection details and optional runtime defaults.
"""

INFLUX_URL = "http://localhost:8086"
INFLUX_ORG = "hy"
INFLUX_BUCKET = "orderflow"
INFLUX_TOKEN = "_GrOE2zCGz_69sZrKaWvvKoHx9ixUgDR8ZhPN3qv3xPZUc1AKx7RT2xd8WlK4c61lUCtVqJmsmNfeDpTNGTwmA=="
INFLUX_MEASUREMENT = "orderflow"

# Optional runtime defaults
DEFAULT_POLL_INTERVAL = 2
DEFAULT_DELTA_WINDOW = 60
DEFAULT_CVD_WINDOW = 60
DEFAULT_DIVERGENCE_WINDOW = 300
DEFAULT_HISTORY_SECONDS = 3600
DEFAULT_IMBALANCE_SMOOTH_POINTS = 20

DEFAULT_SYMBOLS = [
    "coinbase:btc-usdt",
    "coinbase:eth-usdt",
]

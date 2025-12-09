import os
import sys
import time
import hmac
import hashlib
import requests
import subprocess
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load Environment Variables (Security Best Practice)
load_dotenv()

# Logger Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class BinanceBatchExtractor:
    def __init__(self):
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_SECRET_KEY")
        self.base_url = "https://api.binance.com"
        
        # Configure output paths relative to the script location
        # This ensures it runs on any machine, not just yours
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_file = os.path.join(self.base_dir, "data", "trade_data.txt")
        self.last_sync_file = os.path.join(self.base_dir, "data", "last_sync_time.txt")
        
        # Create 'data' folder if it doesn't exist
        os.makedirs(os.path.join(self.base_dir, "data"), exist_ok=True)

        if not self.api_key or not self.api_secret:
            logging.critical("Error: BINANCE_API_KEY or BINANCE_SECRET_KEY not found in .env")
            sys.exit(1)

    def _create_signature(self, query_string):
        return hmac.new(
            self.api_secret.encode('utf-8'), 
            query_string.encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()

    def _get_server_time(self):
        endpoint = "/api/v3/time"
        try:
            response = requests.get(self.base_url + endpoint)
            if response.status_code == 200:
                return response.json()['serverTime']
            else:
                logging.error(f"Time sync error: {response.status_code}, {response.text}")
                return int(time.time() * 1000)
        except Exception as e:
            logging.error(f"Connection error: {e}")
            return int(time.time() * 1000)

    def get_trades(self, symbol):
        endpoint = "/api/v3/myTrades"
        server_timestamp = self._get_server_time()
        
        params = {
            "symbol": symbol.upper(),
            "timestamp": server_timestamp,
            # Lookback window: 30 days
            "startTime": int((server_timestamp - 30 * 24 * 60 * 60 * 1000)),
            "recvWindow": 10000
        }
        
        query_string = '&'.join([f"{key}={params[key]}" for key in params])
        signature = self._create_signature(query_string)
        
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"{self.base_url}{endpoint}?{query_string}&signature={signature}"

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                trades = response.json()
                if trades:
                    logging.info(f"[{symbol}] Transactions found: {len(trades)}")
                else:
                    logging.info(f"[{symbol}] No transactions found.")
                return trades
            else:
                logging.error(f"[{symbol}] API Error: {response.status_code}, {response.text}")
                return []
        except Exception as e:
            logging.error(f"[{symbol}] Request failed: {e}")
            return []

    def convert_timezone(self, time_in_ms):
        """Converts Binance timestamp (UTC) to Local Time (UTC-3/Brazil)."""
        utc_time = datetime.fromtimestamp(time_in_ms / 1000, tz=timezone.utc)
        local_time = utc_time.astimezone(timezone(timedelta(hours=-3)))
        return local_time.strftime('%Y-%m-%d %H:%M:%S')

    def get_user_start_time(self):
        """Interactive input to determine the starting point of the extraction."""
        while True:
            try:
                # In a production CI/CD environment, this would be an env var argument
                user_input = input("Enter cutoff date (YYYY-MM-DD HH:MM:SS): ")
                return datetime.strptime(user_input, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print("Invalid format. Please try again (YYYY-MM-DD HH:MM:SS).")

    def save_data(self, trades, cutoff_date):
        temp_file = self.data_file + ".tmp"
        registered_txs = set()

        # Load existing temporary cache to prevent duplicates within the run
        if os.path.exists(temp_file):
            with open(temp_file, 'r') as temp:
                for line in temp:
                    registered_txs.add(line.strip())

        with open(self.data_file, 'a') as f, open(temp_file, 'a') as temp:
            for trade in trades:
                trade_time_str = self.convert_timezone(trade['time'])
                trade_dt = datetime.strptime(trade_time_str, '%Y-%m-%d %H:%M:%S')

                # Filter by user defined cutoff date
                if trade_dt <= cutoff_date:
                    continue

                # CSV Format: Symbol,Price,Qty,Time,IsBuyer
                tx_str = f"{trade['symbol']},{trade['price']},{trade['qty']},{trade_time_str},{trade['isBuyer']}"
                
                if tx_str not in registered_txs:
                    f.write(tx_str + "\n")
                    temp.write(tx_str + "\n")
                    registered_txs.add(tx_str)
                    logging.info(f"Processing transaction: {tx_str}")

    def run(self):
        # Massive list of pairs to demonstrate wide market coverage capability
        symbols = [
            'AAVEBTC', 'ALGOBTC', 'APEBTC', 'APTBTC', 'ARPABTC', 'ATOMBTC', 'AUDIOBTC',
            'BANDBTC', 'CHZBTC', 'COTIBTC', 'DOGEBTC', 'DYDXBTC', 'EGLDBTC', 'ENJBTC',
            'ETCBTC', 'FTMBTC', 'FLOWBTC', 'GLMBTC', 'ICPBTC', 'KDABTC', 'KMDBTC',
            'LITBTC', 'MASKUSDT', 'MKRBTC', 'MTLBTC', 'NEARBTC', 'NEXOBTC',
            'OPBTC', 'OXTBTC', 'POLBTC', 'POWRBTC', 'QNTBTC', 'SFPBTC', 'SNXBTC', 'SUSHIBTC',
            'SYSBTC', 'THETABTC', 'WANBTC', 'XTZBTC'
        ]

        cutoff_date = self.get_user_start_time()

        # Save cutoff date for the downstream analytics script
        with open(self.last_sync_file, 'w') as f:
            f.write(cutoff_date.strftime('%Y-%m-%d %H:%M:%S'))

        # Create header if file is new
        if not os.path.exists(self.data_file):
            with open(self.data_file, 'w') as f:
                f.write("symbol,price,qty,time,isBuyer\n")

        # Batch Processing Loop
        logging.info(f"Starting batch extraction for {len(symbols)} pairs...")
        for symbol in symbols:
            trades = self.get_trades(symbol)
            if trades:
                self.save_data(trades, cutoff_date)

        logging.info(f"Extraction complete. Data saved to {self.data_file}")

        # Trigger the Analytics/Averaging script
        # Using sys.executable ensures we use the same Python environment
        analytics_script = os.path.join(self.base_dir, "media.py")
        if os.path.exists(analytics_script):
            logging.info("Triggering analytics module (media.py)...")
            subprocess.run([sys.executable, analytics_script])
        else:
            logging.warning("media.py not found. Skipping analytics step.")

if __name__ == "__main__":
    extractor = BinanceBatchExtractor()
    try:
        extractor.run()
    except KeyboardInterrupt:
        sys.exit(0)

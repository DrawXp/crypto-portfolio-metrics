import os
import sys
import csv
import logging
from datetime import datetime

# Logger Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class PortfolioMetricsEngine:
    """
    Financial Calculation Engine.
    
    Processes raw trade data to compute:
    - Weighted Average Price (WAP)
    - Realized PnL (Profit and Loss)
    - Break-Even Point
    
    Designed to work with localized CSV formats (comma vs dot decimal separator).
    """

    def __init__(self):
        # Configure paths relative to script location for portability
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(self.base_dir, "data")
        
        # Input/Output Files
        self.raw_data_file = os.path.join(self.data_dir, "trade_data.txt")
        self.portfolio_csv = os.path.join(self.data_dir, "portfolio_metrics.csv")
        self.last_sync_file = os.path.join(self.data_dir, "last_sync_time.txt")
        self.modified_log_file = os.path.join(self.data_dir, "modified_assets.log")

    def _read_trade_data(self):
        trades = []
        if os.path.exists(self.raw_data_file):
            with open(self.raw_data_file, 'r') as f:
                next(f, None)  # Skip header if present
                for line in f:
                    try:
                        # Format: symbol,price,qty,time,isBuyer
                        parts = line.strip().split(',')
                        if len(parts) < 5: continue
                        
                        symbol, price, qty, time_trade, is_buyer_str = parts
                        
                        trades.append({
                            'symbol': symbol,
                            'price': float(price),
                            'qty': float(qty),
                            'time': time_trade,
                            'is_buyer': is_buyer_str == 'True'
                        })
                    except ValueError as e:
                        logging.warning(f"Skipping malformed line: {line.strip()} | Error: {e}")

        # Sort by date to ensure chronological processing (FIFO/Weighted Avg logic dependency)
        trades.sort(key=lambda x: datetime.strptime(x['time'], '%Y-%m-%d %H:%M:%S'))
        return trades

    def _format_decimal(self, value):
        """Formats float to 8 decimal places string with comma separator (European/BR style)."""
        return f"{value:.8f}".replace('.', ',')

    def _safe_float(self, value_str: str) -> float:
        """Robust string-to-float conversion handling localized separators."""
        value = value_str.strip()
        if not value:
            return 0.0
        return float(value.replace(',', '.'))

    def update_portfolio(self):
        cutoff_date = datetime.min
        if os.path.exists(self.last_sync_file):
            with open(self.last_sync_file, 'r') as f:
                date_str = f.read().strip()
                if date_str:
                    cutoff_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

        raw_trades = self._read_trade_data()
        modified_symbols = set()
        
        # Load existing portfolio state
        sheet_rows = []
        if os.path.exists(self.portfolio_csv):
            with open(self.portfolio_csv, 'r', newline='') as file:
                reader = csv.reader(file)
                sheet_rows = list(reader)

        # Initialize header if empty
        # Header: Symbol, Avg Price, Qty, Last Trade, Break Even, Realized PnL
        if not sheet_rows:
            sheet_rows.append(["symbol", "average_price", "quantity", "last_trade_time", "break_even", "realized_pnl"])

        # Create lookup map for O(1) access
        # Key: Symbol -> Value: Row List (Mutable)
        portfolio_map = {row[0]: row for row in sheet_rows[1:]}

        logging.info(f"Processing {len(raw_trades)} trades...")

        for trade in raw_trades:
            symbol = trade['symbol']
            tx_qty = trade['qty']
            tx_price = trade['price']
            tx_date = datetime.strptime(trade['time'], '%Y-%m-%d %H:%M:%S')
            is_buyer = trade['is_buyer']

            # Incremental processing filter
            if tx_date <= cutoff_date:
                continue

            logging.debug(f"Processing {symbol} | Qty: {tx_qty} | Price: {tx_price} | Buyer: {is_buyer}")

            if symbol in portfolio_map:
                row = portfolio_map[symbol]
                
                # Parse current state
                current_avg_price = self._safe_float(row[1])
                current_qty = self._safe_float(row[2])
                current_realized_pnl = self._safe_float(row[5])
                
                # Logic: Update metrics based on trade side
                if is_buyer:
                    # Weighted Average Price (WAP) Calculation
                    total_cost = current_avg_price * current_qty
                    new_cost = tx_price * tx_qty
                    
                    final_qty = current_qty + tx_qty
                    final_avg_price = (total_cost + new_cost) / final_qty if final_qty > 0 else 0.0
                    new_realized_pnl = current_realized_pnl
                    
                else: # Sell Side
                    if tx_qty > current_qty:
                        logging.warning(f"[{symbol}] Sell qty ({tx_qty}) > Current qty ({current_qty}). Skipping to avoid negative balance error.")
                        continue
                        
                    # FIFO PnL Logic
                    pnl_from_sale = (tx_price - current_avg_price) * tx_qty
                    new_realized_pnl = current_realized_pnl + pnl_from_sale
                    
                    final_qty = current_qty - tx_qty
                    final_avg_price = current_avg_price # Avg price doesn't change on sale in this model

                # Break-Even Calculation
                # BE = (AvgPrice * Qty - RealizedProfit) / Qty
                if final_qty > 0:
                    final_break_even = (final_avg_price * final_qty - new_realized_pnl) / final_qty
                else:
                    final_break_even = 0.0

                # Update Row Data
                row[1] = self._format_decimal(final_avg_price)
                row[2] = self._format_decimal(final_qty)
                row[3] = tx_date.strftime('%Y-%m-%d %H:%M:%S')
                row[4] = self._format_decimal(final_break_even)
                row[5] = self._format_decimal(new_realized_pnl)
                
                modified_symbols.add(symbol)

            else:
                # New Asset Entry
                if is_buyer:
                    final_avg_price = tx_price
                    final_qty = tx_qty
                    new_realized_pnl = 0.0
                else:
                    # Short Selling logic or Data mismatch (Selling what you don't have)
                    # Assuming short position for completeness, or handle as error
                    final_avg_price = tx_price
                    final_qty = -tx_qty 
                    new_realized_pnl = 0.0

                if final_qty != 0:
                    final_break_even = (final_avg_price * final_qty - new_realized_pnl) / final_qty
                else:
                    final_break_even = 0.0

                new_row = [
                    symbol,
                    self._format_decimal(final_avg_price),
                    self._format_decimal(final_qty),
                    tx_date.strftime('%Y-%m-%d %H:%M:%S'),
                    self._format_decimal(final_break_even),
                    self._format_decimal(new_realized_pnl)
                ]
                
                sheet_rows.append(new_row)
                portfolio_map[symbol] = new_row # Update map for subsequent trades in same batch
                modified_symbols.add(symbol)

        # Write updates to CSV
        try:
            with open(self.portfolio_csv, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(sheet_rows)
            logging.info(f"Portfolio metrics updated: {self.portfolio_csv}")
        except IOError as e:
            logging.error(f"Failed to write CSV: {e}")

        # Log modified assets
        try:
            with open(self.modified_log_file, 'w') as f:
                f.write(', '.join(sorted(modified_symbols)))
            logging.info(f"Change log saved: {self.modified_log_file}")
        except IOError:
            pass

if __name__ == "__main__":
    engine = PortfolioMetricsEngine()
    engine.update_portfolio()

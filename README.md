# Crypto Portfolio Metrics & ETL Pipeline 📊

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Binance API](https://img.shields.io/badge/Binance-API%20V3-yellow)
![Status](https://img.shields.io/badge/Status-Production-green)

A specialized data engineering toolkit designed to extract trading history from Binance, normalize data formats, and calculate advanced portfolio metrics including Weighted Average Price (WAP), Realized PnL, and Break-Even thresholds.

Unlike simple trackers, this solution implements a local **ETL (Extract, Transform, Load)** pipeline that supports incremental syncing, making it efficient for accounts with thousands of transactions.

## 🚀 Key Features

* **Secure Extraction:** Uses HMAC SHA256 signatures to authenticate with Binance API (Read-Only access).
* **Incremental Syncing:** Smart logic to fetch only new trades since the last execution, optimizing API usage.
* **Financial Modeling:**
    * **Weighted Average Price (WAP):** Accurate buy price calculation based on accumulation.
    * **FIFO Logic:** Handles partial sells to adjust average price and calculate Realized PnL.
    * **Break-Even Point:** Dynamic calculation of the exit price needed to cover costs.
* **Localization:** Handles CSV decimal separators automatically (Comma vs. Dot support).

## 📂 Architecture

The project consists of two coupled modules located in `/data_engineering`:

1.  **`binance_batch_extractor.py` (The Extractor):**
    * Connects to Binance API.
    * Iterates through a predefined list of assets.
    * Converts UTC timestamps to Local Time.
    * Saves raw data to `data/trade_data.txt`.

2.  **`portfolio_metrics_engine.py` (The Transformer):**
    * Triggered automatically after extraction.
    * Parses raw trade data.
    * Computes financial metrics (Avg Price, BE, PnL).
    * Exports final report to `data/portfolio_metrics.csv`.

## ⚙️ Installation & Setup

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/crypto-portfolio-metrics.git](https://github.com/YOUR_USERNAME/crypto-portfolio-metrics.git)
    cd crypto-portfolio-metrics
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Environment Configuration:**
    Create a `.env` file in the root directory and add your Binance API credentials:
    ```ini
    BINANCE_API_KEY=your_key
    BINANCE_SECRET_KEY=your_secret
    ```

## 🏃 Usage

To run the full pipeline (Extraction + Calculation), execute the extractor script:

```bash
python data_engineering/binance_batch_extractor.py

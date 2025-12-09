# Centralized Data Ingestion Service

A unified, high-performance API for ingesting, processing, and storing financial data from various sources (SEC, Market Data, Alternative Data) for trading algorithms.

## 🏗 Architecture

This project follows a **Vertical Slice Architecture**, where each data source is a self-contained feature with its own routes, schemas, and logic.

### Core Components
- **FastAPI**: Main application gateway.
- **Vertical Slices**: Self-contained modules in `src/dataingestion/features/`.
- **Data Lake (Storage)**:
    - **Bronze (Raw)**: Current implementation stores raw data (Parquet/JSON) to disk via `DataWriter`.
    - **Silver/Gold**: Validation and canonicalization layers (Processing logic exists, but primary ingestion path is currently Raw-to-Disk).

## 🚀 Features / Data Sources

The service ingests data from these providers into a structured Data Lake (Parquet/JSON):

### 1. Market Data
- **Alpaca** (`raw/market_data/alpaca_bars/`):
    - **Trading Gateway**: Proxy for Account Info, Orders (`POST /orders`), and Option Chains.
    - Historical Bars (Stocks/Crypto) and Option Bars.
    - Corporate Actions (Splits/Dividends).
- **Coinbase** (`raw/market_data/coinbase_candles/`):
    - Historical Crypto Candles (Advanced Trade API).
- **YFinance** (`raw/market_data/yfinance_bars/`):
    - Fallback OHLCV market data.

### 2. Alternative Data
- **Unusual Whales** (`raw/options_flow/uw_flow/`):
    - Options Order Flow, Dark Pool Trades.
    - Tides (Market/Sector/ETF) and Short Interest.
- **SEC** (`raw/fundamentals/sec_filings/`):
    - Real-time EDGAR Filings (JSON).
    - XBRL Company Facts.
- **NewsAPI** (`raw/news/newsapi/`):
    - Global news headlines (JSON).

### 3. Macro Economic
- **FRED** (`raw/macro/fred/`):
    - Economic series data (GDP, CPI, Unemployment).

### 4. AI & LLM
- **LLM Gateway**: Centralized router for LLM interactions.
    - **AnyLLM**: Unified interface for OpenAI, Anthropic, Gemini, Mistral.
    - **Local Support**: First-class support for `llama.cpp` (GGUF) models via `any-llm`.

## ✅ Verification

The repository includes scripts to verify that ingestion pipelines and integrations are working correctly.

### Run All Verifications
This script runs a smoke test for every data source (requires `.env` keys):
```bash
python verify_all.py
```

### Specific Verifications
- **Any-LLM / Local Models**: `python verify_any_llm.py`
- **SEC Data**: `python verify_sec.py`
- **Application App**: `python verify_app.py`



## 🤖 Local LLM Integration (Ollama)

This project uses **Ollama** for running local Large Language Models (LLMs). It automates model management and utilizes Apple Silicon (Metal) acceleration out of the box.

### 1. Install Ollama
Download and install from [ollama.com](https://ollama.com).

### 2. Quick Start (Standard Models)
Pull standard models directly from the Ollama library:

```bash
# Recommended: Llama 3.1 8B
ollama pull llama3.1

# Recommended: Qwen 2.5 14B
ollama pull qwen2.5:14b

# Recommended: Mistral Small 24B
ollama pull mistral-small
```

### 3. Advanced: Using Custom GGUF Models
If you have high-performance GGUF models (like **Qwen 3 32B**) downloaded manually, you can import them into Ollama.

1.  **Create a Modelfile** (in the same folder as your `.gguf` file):
    ```dockerfile
    FROM ./Qwen_Qwen3-32B-Q4_K_M.gguf
    ```
2.  **Create the Model**:
    ```bash
    ollama create qwen3-32b -f Modelfile
    ```
3.  **Use it**:
    ```bash
    ollama run qwen3-32b
    ```

### 4. Configure AnyLLM & Cloud Support
1.  Navigate to `any-llm/` directory and copy/create `.env`.
2.  **OpenRouter**: Add `OPENROUTER_API_KEY` to `.env`.
3.  **Ollama Cloud**: To use `ollama.com` cloud inference instead of local:
    ```bash
    OLLAMA_HOST=https://ollama.com
    OLLAMA_API_KEY=your-key
    ```
    *(Comment these out to revert to Local mode)*

```python
# Example usage
from any_llm import AnyLLM

# Local/Cloud Ollama (toggled via env)
client = AnyLLM.create("ollama")
response = client.completion(model="qwen3-32b", messages=[...])

# OpenRouter
client = AnyLLM.create("openrouter") 
response = client.completion(model="google/gemini-2.0-flash-exp:free", messages=[...])
```

## 🛠 Getting Started

### Prerequisites
- Python 3.10+
- Redis Server (running locally or accessible)

### Installation

1.  **Clone the repository** (if you haven't already).
2.  **Install dependencies**:
    ```bash
    pip install -e .
    ```

### Configuration

Create a `.env` file in the project root. You can use the `Settings` class in `src/dataingestion/shared/config.py` as a reference.

**Required Variables:**

```env
# App
APP_NAME="Data Ingestion Service"
DEBUG=True

# Storage
DATA_DIR="../data" # Relative path recommended for portability

# Redis
REDIS_URL="redis://localhost:6379/0"

# API Keys (Add as needed)
APCA_API_KEY_ID="your_alpaca_key"
APCA_API_SECRET_KEY="your_alpaca_secret"
APCA_API_BASE_URL="https://paper-api.alpaca.markets" # Optional, defaults to paper

UNUSUAL_WHALES_API_KEY="your_uw_key"
FRED_API_KEY="your_fred_key"
NEWS_API_KEY="your_news_key"
COINBASE_API_KEY="your_coinbase_key"
COINBASE_API_SECRET="your_coinbase_secret"

# SEC
SEC_USER_AGENT="Your Name your.email@example.com"
```

### Running the Server

Start the FastAPI server with hot-reloading enabled:

```bash
uvicorn src.dataingestion.api.main:app --reload
```

The API will be available at `http://127.0.0.1:8000`.
Access the interactive API docs at `http://127.0.0.1:8000/docs`.

## 📂 Project Structure

```
src/dataingestion/
├── api/                # Main API entry point and router setup
├── features/           # Vertical slices for each data source
│   ├── alpaca_data/    # Stock, Crypto, Options, Corporate Actions
│   ├── coinbase_data/  # Crypto History
│   ├── fred_data/      # Economic Data
│   ├── llm_gateway/    # AI/LLM Routing
│   ├── news_data/      # NewsAPI
│   ├── sec_data/       # EDGAR Filings & XBRL Facts
│   ├── uw_data/        # Unusual Whales (Flow, Dark Pools, Tides, etc.)
│   └── yfinance_data/  # Fallback Market Data
├── processors/         # Data processing logic (Silver Layer)
└── shared/             # Shared utilities (Config, Cache, Storage)
```

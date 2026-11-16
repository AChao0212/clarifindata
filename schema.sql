-- Calendar flags used by ingestion.trading_calendar. Load official dates before use.
CREATE TABLE IF NOT EXISTS taiwan_stock_trading_date (
    trade_date date PRIMARY KEY,
    is_trading_day boolean NOT NULL
);

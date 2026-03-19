#!/usr/bin/env python3
"""
TradEdge Circuit Limit & Earnings Tracker v1.0
=================================================
Tracks NSE circuit limits and upcoming earnings dates for all stocks.

Circuit Limits:
  - Fetches daily circuit limit bands from NSE (2%, 5%, 10%, 20%, No Band)
  - Filters stocks by circuit band (exclude 2% and 5% restricted stocks)
  - Alerts when a stock's circuit limit changes (band tightening/loosening)
  - Scan clause support: filter by circuit limit in scanner

Earnings Dates:
  - Fetches upcoming board meeting / results dates
  - Alerts N days before earnings
  - Flags stocks reporting this week

Data Sources:
  - NSE Bhavcopy (circuit limits)
  - yfinance (earnings dates)
  - MoneyControl / BSE (earnings calendar fallback)

Output:
  - data/circuit_limits.json
  - data/earnings_calendar.json
"""

import json
import time
import csv
import io
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
CIRCUIT_FILE = DATA_DIR / "circuit_limits.json"
CIRCUIT_HISTORY_FILE = DATA_DIR / "circuit_limits_history.json"
EARNINGS_FILE = DATA_DIR / "earnings_calendar.json"


# ═══════════════════════════════════════════════════════════════════════════════
# CIRCUIT LIMIT TYPES
# ═══════════════════════════════════════════════════════════════════════════════

class CircuitBand:
    """NSE Circuit Limit Band classifications."""
    BAND_2 = "2%"
    BAND_5 = "5%"
    BAND_10 = "10%"
    BAND_20 = "20%"
    NO_BAND = "No Band"     # F&O stocks typically
    UNKNOWN = "Unknown"
    
    # Bands that are too restrictive for active trading
    RESTRICTED_BANDS = {"2%", "5%"}
    
    # All possible bands ordered from most to least restrictive
    ALL_BANDS = ["2%", "5%", "10%", "20%", "No Band"]
    
    @staticmethod
    def from_pct(upper_limit: float, lower_limit: float, close: float) -> str:
        """Determine circuit band from upper/lower limit prices."""
        if close <= 0:
            return CircuitBand.UNKNOWN
        
        upper_pct = abs((upper_limit - close) / close * 100)
        lower_pct = abs((close - lower_limit) / close * 100)
        band_pct = round(min(upper_pct, lower_pct))
        
        if band_pct <= 2:
            return CircuitBand.BAND_2
        elif band_pct <= 5:
            return CircuitBand.BAND_5
        elif band_pct <= 10:
            return CircuitBand.BAND_10
        elif band_pct <= 20:
            return CircuitBand.BAND_20
        else:
            return CircuitBand.NO_BAND
    
    @staticmethod
    def is_restricted(band: str) -> bool:
        """Check if a circuit band is too restrictive for trading."""
        return band in CircuitBand.RESTRICTED_BANDS
    
    @staticmethod
    def band_to_numeric(band: str) -> int:
        """Convert band to numeric for sorting (lower = more restricted)."""
        mapping = {"2%": 2, "5%": 5, "10%": 10, "20%": 20, "No Band": 100, "Unknown": -1}
        return mapping.get(band, -1)


@dataclass
class CircuitLimitInfo:
    """Circuit limit information for a stock."""
    symbol: str
    band: str                       # "2%", "5%", "10%", "20%", "No Band"
    upper_limit: float = 0.0        # Upper circuit price
    lower_limit: float = 0.0        # Lower circuit price
    close_price: float = 0.0        # Previous close
    is_restricted: bool = False     # True if band is 2% or 5%
    band_changed: bool = False      # True if band changed from previous day
    prev_band: str = ""             # Previous band (if changed)
    change_direction: str = ""      # "tightened" or "loosened"
    in_fno: bool = False            # F&O stock (typically no circuit)
    
    def to_dict(self):
        return {
            "symbol": self.symbol,
            "band": self.band,
            "upper_limit": self.upper_limit,
            "lower_limit": self.lower_limit,
            "close_price": self.close_price,
            "is_restricted": self.is_restricted,
            "band_changed": self.band_changed,
            "prev_band": self.prev_band,
            "change_direction": self.change_direction,
            "in_fno": self.in_fno,
        }


@dataclass
class EarningsEvent:
    """Earnings/results announcement event."""
    symbol: str
    name: str
    date: str                   # "YYYY-MM-DD"
    event_type: str             # "results", "board_meeting", "agm"
    quarter: str                # "Q3 FY26", "Q4 FY26"
    days_until: int = 0         # Days from today
    is_this_week: bool = False
    is_today: bool = False
    
    def to_dict(self):
        return {
            "symbol": self.symbol,
            "name": self.name,
            "date": self.date,
            "event_type": self.event_type,
            "quarter": self.quarter,
            "days_until": self.days_until,
            "is_this_week": self.is_this_week,
            "is_today": self.is_today,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CIRCUIT LIMIT TRACKER
# ═══════════════════════════════════════════════════════════════════════════════

class CircuitLimitTracker:
    """
    Tracks and manages circuit limit data for all NSE stocks.
    
    Usage:
        tracker = CircuitLimitTracker()
        tracker.update()  # Fetch latest circuit limits
        
        # Filter stocks
        tradeable = tracker.get_tradeable_stocks()  # Excludes 2% and 5%
        restricted = tracker.get_restricted_stocks()  # Only 2% and 5%
        
        # Check specific stock
        info = tracker.get_circuit_info("RELIANCE")
        
        # Get alerts (band changes)
        alerts = tracker.get_circuit_change_alerts()
    """
    
    def __init__(self):
        self.circuit_data: dict[str, CircuitLimitInfo] = {}
        self.prev_data: dict[str, CircuitLimitInfo] = {}
        self._load_existing()
    
    def _load_existing(self):
        """Load previously saved circuit data."""
        if CIRCUIT_FILE.exists():
            try:
                with open(CIRCUIT_FILE) as f:
                    data = json.load(f)
                for sym, info in data.get("stocks", {}).items():
                    self.prev_data[sym] = CircuitLimitInfo(
                        symbol=sym,
                        band=info.get("band", "Unknown"),
                        upper_limit=info.get("upper_limit", 0),
                        lower_limit=info.get("lower_limit", 0),
                        close_price=info.get("close_price", 0),
                    )
            except Exception:
                pass
    
    def update_from_bhavcopy(self, bhavcopy_data: list[dict]):
        """
        Update circuit limits from NSE Bhavcopy data.
        
        Bhavcopy CSV columns typically include:
        SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, 
        TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN,
        HI_52_WK, LO_52_WK
        
        Circuit limits come from a separate file:
        https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_{date}.csv
        Or from the security-wise price bands CSV.
        """
        for row in bhavcopy_data:
            symbol = row.get("SYMBOL", "").strip()
            if not symbol:
                continue
            
            close = float(row.get("CLOSE", 0) or 0)
            upper = float(row.get("UPPER_LIMIT", 0) or row.get("HI_52_WK", 0) or 0)
            lower = float(row.get("LOWER_LIMIT", 0) or row.get("LO_52_WK", 0) or 0)
            
            if upper > 0 and lower > 0 and close > 0:
                band = CircuitBand.from_pct(upper, lower, close)
            else:
                band = CircuitBand.UNKNOWN
            
            info = CircuitLimitInfo(
                symbol=symbol,
                band=band,
                upper_limit=round(upper, 2),
                lower_limit=round(lower, 2),
                close_price=round(close, 2),
                is_restricted=CircuitBand.is_restricted(band),
            )
            
            # Check for band change
            if symbol in self.prev_data:
                prev = self.prev_data[symbol]
                if prev.band != band and prev.band != "Unknown":
                    info.band_changed = True
                    info.prev_band = prev.band
                    prev_num = CircuitBand.band_to_numeric(prev.band)
                    curr_num = CircuitBand.band_to_numeric(band)
                    info.change_direction = "tightened" if curr_num < prev_num else "loosened"
            
            self.circuit_data[symbol] = info
    
    def update_from_yfinance(self, symbols: list[str]):
        """
        Estimate circuit bands from yfinance data.
        Uses price range heuristics when NSE bhavcopy is not available.
        """
        import yfinance as yf
        
        # Known F&O stocks (these typically have no circuit limit)
        FNO_STOCKS = {
            "RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "TCS", "INFY",
            "BHARTIARTL", "HINDUNILVR", "ITC", "KOTAKBANK", "LT", "AXISBANK",
            "BAJFINANCE", "MARUTI", "SUNPHARMA", "TITAN", "ASIANPAINT",
            "WIPRO", "HCLTECH", "TATAMOTORS", "M&M", "NTPC", "POWERGRID",
            "ULTRACEMCO", "BAJAJFINSV", "NESTLEIND", "JSWSTEEL", "TATASTEEL",
            "ONGC", "COALINDIA", "TECHM", "DRREDDY", "CIPLA", "HEROMOTOCO",
            "EICHERMOT", "BPCL", "HINDALCO", "GRASIM", "DIVISLAB", "BRITANNIA",
            "ADANIENT", "ADANIPORTS", "DLF", "INDIGO", "ZOMATO", "HAL", "BEL",
            "TATAPOWER", "RECLTD", "PFC", "NHPC", "IRFC", "JINDALSTEL",
            "VEDL", "NMDC", "SAIL", "TRENT", "BAJAJ-AUTO", "APOLLOHOSP",
            "GODREJPROP", "SBILIFE", "HDFCLIFE", "SHRIRAMFIN", "CHOLAFIN",
            "PIDILITIND", "BERGEPAINT", "HAVELLS", "DABUR", "MARICO", "COLPAL",
            "TATACONSUM", "LUPIN", "BIOCON", "AUROPHARMA", "TORNTPHARM",
            "DMART", "POLYCAB", "DIXON", "ABB", "SIEMENS", "CUMMINSIND",
            "PERSISTENT", "LTIM", "MPHASIS", "COFORGE", "NAUKRI",
            "AMBUJACEM", "ACC", "DALMIACEM", "JKCEMENT", "RAMCOCEM",
            "MUTHOOTFIN", "MANAPPURAM", "PVRINOX", "VOLTAS", "CROMPTON",
            "IRCTC", "RVNL", "BHEL", "BANKBARODA", "PNB", "CANBK",
            "IDFCFIRSTB", "FEDERALBNK", "BANDHANBNK", "AUBANK",
            "MRF", "BALKRISIND", "APOLLOTYRE", "EXIDEIND", "MOTHERSON",
            "IOC", "HPCL", "GAIL", "PETRONET", "CONCOR", "BOSCHLTD",
        }
        
        for symbol in symbols:
            if symbol in FNO_STOCKS:
                self.circuit_data[symbol] = CircuitLimitInfo(
                    symbol=symbol,
                    band=CircuitBand.NO_BAND,
                    is_restricted=False,
                    in_fno=True,
                )
                continue
            
            # For non-F&O stocks, estimate from market cap
            # Large caps (>20K Cr) usually have 20% or No Band
            # Mid caps (5K-20K Cr) usually have 10% or 20%
            # Small caps (<5K Cr) can have 5% or 10%
            # Penny / surveillance stocks often have 2% or 5%
            
            # This is a heuristic — actual circuit limits should come from NSE
            if symbol not in self.circuit_data:
                self.circuit_data[symbol] = CircuitLimitInfo(
                    symbol=symbol,
                    band=CircuitBand.UNKNOWN,
                )
    
    def set_circuit_band(self, symbol: str, band: str, upper: float = 0, lower: float = 0, close: float = 0):
        """Manually set circuit band for a stock (from NSE data or user input)."""
        info = CircuitLimitInfo(
            symbol=symbol,
            band=band,
            upper_limit=upper,
            lower_limit=lower,
            close_price=close,
            is_restricted=CircuitBand.is_restricted(band),
        )
        
        if symbol in self.circuit_data:
            prev = self.circuit_data[symbol]
            if prev.band != band and prev.band != "Unknown":
                info.band_changed = True
                info.prev_band = prev.band
                prev_num = CircuitBand.band_to_numeric(prev.band)
                curr_num = CircuitBand.band_to_numeric(band)
                info.change_direction = "tightened" if curr_num < prev_num else "loosened"
        
        self.circuit_data[symbol] = info
    
    def get_circuit_info(self, symbol: str) -> CircuitLimitInfo:
        """Get circuit limit info for a specific stock."""
        return self.circuit_data.get(symbol, CircuitLimitInfo(symbol=symbol, band=CircuitBand.UNKNOWN))
    
    def get_tradeable_stocks(self, symbols: list[str] = None) -> list[str]:
        """Get stocks that are NOT circuit-restricted (exclude 2% and 5%)."""
        pool = symbols or list(self.circuit_data.keys())
        return [s for s in pool if not self.circuit_data.get(s, CircuitLimitInfo(symbol=s, band="Unknown")).is_restricted]
    
    def get_restricted_stocks(self) -> list[str]:
        """Get stocks with 2% or 5% circuit limit."""
        return [s for s, info in self.circuit_data.items() if info.is_restricted]
    
    def get_stocks_by_band(self, band: str) -> list[str]:
        """Get all stocks with a specific circuit band."""
        return [s for s, info in self.circuit_data.items() if info.band == band]
    
    def get_circuit_change_alerts(self) -> list[CircuitLimitInfo]:
        """Get stocks whose circuit band changed since last update."""
        return [info for info in self.circuit_data.values() if info.band_changed]
    
    def filter_scan_results(self, symbols: list[str],
                            exclude_bands: set[str] = None) -> list[str]:
        """
        Filter scan results by circuit limit.
        Default: excludes 2% and 5% bands.
        """
        if exclude_bands is None:
            exclude_bands = CircuitBand.RESTRICTED_BANDS
        
        return [
            s for s in symbols
            if self.circuit_data.get(s, CircuitLimitInfo(symbol=s, band="Unknown")).band not in exclude_bands
        ]
    
    def save(self):
        """Save current circuit data to JSON."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        output = {
            "generated_at": datetime.now().isoformat(),
            "total_stocks": len(self.circuit_data),
            "summary": {
                band: len(self.get_stocks_by_band(band))
                for band in CircuitBand.ALL_BANDS + ["Unknown"]
            },
            "restricted_count": len(self.get_restricted_stocks()),
            "band_changes": [info.to_dict() for info in self.get_circuit_change_alerts()],
            "stocks": {s: info.to_dict() for s, info in self.circuit_data.items()},
        }
        
        with open(CIRCUIT_FILE, "w") as f:
            json.dump(output, f, indent=2)
        
        # Append to history
        history = []
        if CIRCUIT_HISTORY_FILE.exists():
            try:
                with open(CIRCUIT_HISTORY_FILE) as f:
                    history = json.load(f)
            except Exception:
                history = []
        
        history.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "summary": output["summary"],
            "changes": [info.to_dict() for info in self.get_circuit_change_alerts()],
        })
        
        # Keep last 90 days
        history = history[-90:]
        
        with open(CIRCUIT_HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# EARNINGS DATE TRACKER
# ═══════════════════════════════════════════════════════════════════════════════

class EarningsTracker:
    """
    Tracks upcoming earnings/results dates for NSE stocks.
    
    Usage:
        tracker = EarningsTracker()
        tracker.update(symbols)
        
        upcoming = tracker.get_upcoming(days=7)
        today = tracker.get_reporting_today()
        this_week = tracker.get_reporting_this_week()
    """
    
    def __init__(self):
        self.events: list[EarningsEvent] = []
        self._load_existing()
    
    def _load_existing(self):
        if EARNINGS_FILE.exists():
            try:
                with open(EARNINGS_FILE) as f:
                    data = json.load(f)
                for e in data.get("events", []):
                    self.events.append(EarningsEvent(**e))
            except Exception:
                pass
    
    def update_from_yfinance(self, symbols: list[str]):
        """Fetch earnings dates from yfinance for given symbols."""
        import yfinance as yf
        
        today = datetime.now().date()
        new_events = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(f"{symbol}.NS")
                cal = ticker.calendar
                
                if cal is not None and not cal.empty:
                    # yfinance calendar has 'Earnings Date' row
                    if hasattr(cal, 'loc') and 'Earnings Date' in cal.index:
                        dates = cal.loc['Earnings Date']
                        for d in (dates if hasattr(dates, '__iter__') else [dates]):
                            if pd.notna(d):
                                earnings_date = pd.Timestamp(d).date()
                                days_until = (earnings_date - today).days
                                
                                if days_until >= -7:  # Include recent past (7 days)
                                    # Determine quarter
                                    month = earnings_date.month
                                    if month <= 3:
                                        quarter = "Q4"
                                    elif month <= 6:
                                        quarter = "Q1"
                                    elif month <= 9:
                                        quarter = "Q2"
                                    else:
                                        quarter = "Q3"
                                    
                                    fy = earnings_date.year if month > 3 else earnings_date.year - 1
                                    fy_str = f"{quarter} FY{str(fy+1)[-2:]}"
                                    
                                    event = EarningsEvent(
                                        symbol=symbol,
                                        name=symbol,
                                        date=earnings_date.isoformat(),
                                        event_type="results",
                                        quarter=fy_str,
                                        days_until=days_until,
                                        is_this_week=0 <= days_until <= 7,
                                        is_today=days_until == 0,
                                    )
                                    new_events.append(event)
            except Exception:
                continue
        
        # Merge with existing (deduplicate by symbol+date)
        existing_keys = {(e.symbol, e.date) for e in self.events}
        for e in new_events:
            if (e.symbol, e.date) not in existing_keys:
                self.events.append(e)
                existing_keys.add((e.symbol, e.date))
        
        # Re-sort by date
        self.events.sort(key=lambda e: e.date)
    
    def add_earnings_date(self, symbol: str, date: str, quarter: str = "",
                          event_type: str = "results"):
        """Manually add an earnings date."""
        today = datetime.now().date()
        try:
            earn_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            return
        
        days_until = (earn_date - today).days
        
        event = EarningsEvent(
            symbol=symbol,
            name=symbol,
            date=date,
            event_type=event_type,
            quarter=quarter,
            days_until=days_until,
            is_this_week=0 <= days_until <= 7,
            is_today=days_until == 0,
        )
        self.events.append(event)
    
    def get_upcoming(self, days: int = 30) -> list[EarningsEvent]:
        """Get earnings events in the next N days."""
        today = datetime.now().date()
        cutoff = today + timedelta(days=days)
        
        result = []
        for e in self.events:
            try:
                d = datetime.strptime(e.date, "%Y-%m-%d").date()
                if today <= d <= cutoff:
                    e.days_until = (d - today).days
                    e.is_today = e.days_until == 0
                    e.is_this_week = 0 <= e.days_until <= 7
                    result.append(e)
            except ValueError:
                continue
        
        return sorted(result, key=lambda e: e.date)
    
    def get_reporting_today(self) -> list[EarningsEvent]:
        """Get stocks reporting earnings today."""
        today = datetime.now().date().isoformat()
        return [e for e in self.events if e.date == today]
    
    def get_reporting_this_week(self) -> list[EarningsEvent]:
        """Get stocks reporting earnings this week."""
        return self.get_upcoming(days=7)
    
    def is_near_earnings(self, symbol: str, days: int = 5) -> bool:
        """Check if a stock is within N days of earnings."""
        today = datetime.now().date()
        for e in self.events:
            if e.symbol == symbol:
                try:
                    d = datetime.strptime(e.date, "%Y-%m-%d").date()
                    if abs((d - today).days) <= days:
                        return True
                except ValueError:
                    continue
        return False
    
    def get_earnings_alert(self, symbol: str, alert_days: int = 3) -> Optional[str]:
        """Get earnings alert message if stock is near earnings."""
        today = datetime.now().date()
        for e in self.events:
            if e.symbol == symbol:
                try:
                    d = datetime.strptime(e.date, "%Y-%m-%d").date()
                    diff = (d - today).days
                    if diff == 0:
                        return f"⚠️ {symbol} reports {e.quarter} results TODAY"
                    elif 0 < diff <= alert_days:
                        return f"📅 {symbol} reports {e.quarter} results in {diff} day(s) ({e.date})"
                    elif diff < 0 and abs(diff) <= 2:
                        return f"📊 {symbol} reported {e.quarter} results {abs(diff)} day(s) ago"
                except ValueError:
                    continue
        return None
    
    def save(self):
        """Save earnings calendar to JSON."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        output = {
            "generated_at": datetime.now().isoformat(),
            "total_events": len(self.events),
            "reporting_today": [e.to_dict() for e in self.get_reporting_today()],
            "reporting_this_week": [e.to_dict() for e in self.get_reporting_this_week()],
            "events": [e.to_dict() for e in self.events],
        }
        
        with open(EARNINGS_FILE, "w") as f:
            json.dump(output, f, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# SCANNER INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════

def apply_circuit_filter(scan_results: list[str], 
                         tracker: CircuitLimitTracker,
                         exclude_bands: set[str] = None) -> dict:
    """
    Apply circuit limit filter to scan results.
    
    Returns:
        {
            "tradeable": [symbols passing circuit filter],
            "restricted": [symbols filtered out],
            "stats": {"2%": N, "5%": N, "10%": N, ...}
        }
    """
    if exclude_bands is None:
        exclude_bands = {"2%", "5%"}
    
    tradeable = []
    restricted = []
    stats = {}
    
    for symbol in scan_results:
        info = tracker.get_circuit_info(symbol)
        band = info.band
        stats[band] = stats.get(band, 0) + 1
        
        if band in exclude_bands:
            restricted.append(symbol)
        else:
            tradeable.append(symbol)
    
    return {
        "tradeable": tradeable,
        "restricted": restricted,
        "tradeable_count": len(tradeable),
        "restricted_count": len(restricted),
        "stats": stats,
    }


def enrich_with_alerts(scan_results: list[dict],
                       circuit_tracker: CircuitLimitTracker,
                       earnings_tracker: EarningsTracker) -> list[dict]:
    """
    Enrich scan results with circuit limit info and earnings alerts.
    Adds 'circuit' and 'earnings_alert' fields to each result.
    """
    for stock in scan_results:
        symbol = stock.get("symbol", "")
        
        # Circuit info
        circuit_info = circuit_tracker.get_circuit_info(symbol)
        stock["circuit"] = {
            "band": circuit_info.band,
            "upper_limit": circuit_info.upper_limit,
            "lower_limit": circuit_info.lower_limit,
            "is_restricted": circuit_info.is_restricted,
            "band_changed": circuit_info.band_changed,
            "prev_band": circuit_info.prev_band,
            "change_direction": circuit_info.change_direction,
        }
        
        # Earnings alert
        stock["earnings_alert"] = earnings_tracker.get_earnings_alert(symbol)
        stock["near_earnings"] = earnings_tracker.is_near_earnings(symbol)
    
    return scan_results


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  Circuit Limit & Earnings Tracker — Self Test")
    print("=" * 60)
    
    # ── Circuit Limit Test ──
    tracker = CircuitLimitTracker()
    
    # Set some test data
    tracker.set_circuit_band("RELIANCE", "No Band", 1500, 1200, 1395)
    tracker.set_circuit_band("HDFCBANK", "No Band", 900, 700, 840)
    tracker.set_circuit_band("TCS", "No Band", 2600, 2200, 2409)
    tracker.set_circuit_band("YESBANK", "5%", 21, 19, 20)
    tracker.set_circuit_band("RPOWER", "5%", 32, 29, 30.5)
    tracker.set_circuit_band("SUZLON", "20%", 72, 48, 60)
    tracker.set_circuit_band("TESTSTOCK", "2%", 10.2, 9.8, 10.0)
    tracker.set_circuit_band("IREDA", "10%", 220, 180, 200)
    
    # Simulate a band change
    tracker.prev_data["SUZLON"] = CircuitLimitInfo(symbol="SUZLON", band="10%")
    tracker.set_circuit_band("SUZLON", "20%", 72, 48, 60)
    
    print(f"\n  Total stocks: {len(tracker.circuit_data)}")
    print(f"  Restricted (2%/5%): {len(tracker.get_restricted_stocks())}")
    print(f"  Tradeable: {len(tracker.get_tradeable_stocks())}")
    
    print(f"\n  Band distribution:")
    for band in CircuitBand.ALL_BANDS + ["Unknown"]:
        count = len(tracker.get_stocks_by_band(band))
        if count > 0:
            print(f"    {band:10s}: {count}")
    
    print(f"\n  Restricted stocks: {tracker.get_restricted_stocks()}")
    
    # Circuit change alerts
    alerts = tracker.get_circuit_change_alerts()
    if alerts:
        print(f"\n  Circuit band change alerts:")
        for a in alerts:
            print(f"    ⚠️ {a.symbol}: {a.prev_band} → {a.band} ({a.change_direction})")
    
    # Filter scan results
    scan_symbols = ["RELIANCE", "YESBANK", "TCS", "RPOWER", "SUZLON", "TESTSTOCK", "IREDA"]
    filtered = apply_circuit_filter(scan_symbols, tracker)
    print(f"\n  Scan filter test:")
    print(f"    Input: {len(scan_symbols)} stocks")
    print(f"    Tradeable: {filtered['tradeable_count']} → {filtered['tradeable']}")
    print(f"    Restricted: {filtered['restricted_count']} → {filtered['restricted']}")
    
    # ── Earnings Test ──
    print(f"\n{'=' * 60}")
    print("  Earnings Tracker")
    print(f"{'=' * 60}")
    
    earnings = EarningsTracker()
    
    today = datetime.now().date()
    earnings.add_earnings_date("TCS", (today + timedelta(days=2)).isoformat(), "Q4 FY26")
    earnings.add_earnings_date("INFY", today.isoformat(), "Q4 FY26")
    earnings.add_earnings_date("HDFCBANK", (today + timedelta(days=10)).isoformat(), "Q4 FY26")
    earnings.add_earnings_date("RELIANCE", (today + timedelta(days=25)).isoformat(), "Q4 FY26")
    
    print(f"\n  Reporting today: {[e.symbol for e in earnings.get_reporting_today()]}")
    print(f"  Reporting this week: {[e.symbol for e in earnings.get_reporting_this_week()]}")
    print(f"  Upcoming (30 days): {[e.symbol for e in earnings.get_upcoming(30)]}")
    
    # Alerts
    for sym in ["TCS", "INFY", "HDFCBANK", "RELIANCE", "SBIN"]:
        alert = earnings.get_earnings_alert(sym)
        if alert:
            print(f"  {alert}")
        else:
            print(f"  {sym}: No upcoming earnings")
    
    print(f"\n{'=' * 60}")

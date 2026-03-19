#!/usr/bin/env python3
"""
TradEdge ChartInk Clause Parser v1.0
=====================================
Parses ChartInk scanner clause syntax and evaluates against stock OHLCV data.

Supports:
  - Time references: latest, N days/weeks/months ago
  - OHLCV fields: open, high, low, close, volume
  - Indicators: sma, ema, rsi, macd, supertrend, bollinger bands, atr, adx, cci, vwap
  - Aggregate functions: max, min, count, countstreak
  - Arithmetic: +, -, *, /
  - Comparisons: >, <, >=, <=, =
  - Logical: and, or
  - Grouping: ( )
  - Segment wrappers: cash, futures (treated as pass-through)
  - Watchlist IDs: {12345} (ignored)

Usage:
  from chartink_parser import ChartInkParser
  
  parser = ChartInkParser()
  clause = "( cash ( latest close > latest sma( latest close , 200 ) and latest rsi( 14 ) > 50 ) )"
  
  # Evaluate against a stock's DataFrame (columns: Open, High, Low, Close, Volume)
  result = parser.evaluate(clause, df)  # Returns True/False
"""

import re
import math
import numpy as np
import pandas as pd
from enum import Enum, auto
from dataclasses import dataclass
from typing import Any


# ═══════════════════════════════════════════════════════════════════════════════
# TOKEN TYPES
# ═══════════════════════════════════════════════════════════════════════════════

class TokenType(Enum):
    # Literals
    NUMBER = auto()
    
    # Time references
    LATEST = auto()          # "latest"
    DAYS_AGO = auto()        # "N day(s) ago" or "N days ago"
    WEEKS_AGO = auto()       # "N week(s) ago"
    MONTHS_AGO = auto()      # "N month(s) ago"
    
    # OHLCV fields
    OPEN = auto()
    HIGH = auto()
    LOW = auto()
    CLOSE = auto()
    VOLUME = auto()
    
    # Indicator functions
    SMA = auto()
    EMA = auto()
    RSI = auto()
    MACD = auto()
    MACD_SIGNAL = auto()
    MACD_HISTOGRAM = auto()
    SUPERTREND = auto()
    UPPER_BOLLINGER = auto()
    LOWER_BOLLINGER = auto()
    ATR = auto()
    ADX = auto()
    CCI = auto()
    VWAP = auto()
    MFI = auto()
    OBV = auto()
    WILLIAMS_R = auto()
    STOCH_K = auto()
    STOCH_D = auto()
    ROC = auto()
    
    # Aggregate functions
    MAX = auto()             # max(period, series)
    MIN = auto()             # min(period, series)
    COUNT = auto()           # count(N, 1 where condition)
    COUNTSTREAK = auto()     # countstreak(N, 1 where condition)
    
    # Operators
    PLUS = auto()
    MINUS = auto()
    MULTIPLY = auto()
    DIVIDE = auto()
    
    # Comparisons
    GT = auto()              # >
    LT = auto()              # <
    GTE = auto()             # >=
    LTE = auto()             # <=
    EQ = auto()              # =
    
    # Logical
    AND = auto()
    OR = auto()
    
    # Grouping
    LPAREN = auto()
    RPAREN = auto()
    COMMA = auto()
    
    # Segment markers (pass-through)
    CASH = auto()
    FUTURES = auto()
    
    # Special
    WHERE = auto()           # used in count()
    WATCHLIST = auto()       # {12345}
    EOF = auto()


@dataclass
class Token:
    type: TokenType
    value: Any
    pos: int = 0


# ═══════════════════════════════════════════════════════════════════════════════
# TOKENIZER
# ═══════════════════════════════════════════════════════════════════════════════

class Tokenizer:
    """Converts a ChartInk clause string into a stream of tokens."""
    
    # Multi-word keywords (order matters — longest first)
    MULTI_WORD_KEYWORDS = [
        ("upper bollinger band", TokenType.UPPER_BOLLINGER),
        ("lower bollinger band", TokenType.LOWER_BOLLINGER),
        ("macd signal line", TokenType.MACD_SIGNAL),
        ("macd signal", TokenType.MACD_SIGNAL),
        ("macd histogram", TokenType.MACD_HISTOGRAM),
        ("macd line", TokenType.MACD),
        ("stochastic %k", TokenType.STOCH_K),
        ("stochastic %d", TokenType.STOCH_D),
        ("stochastic k", TokenType.STOCH_K),
        ("stochastic d", TokenType.STOCH_D),
        ("williams %r", TokenType.WILLIAMS_R),
        ("williams r", TokenType.WILLIAMS_R),
        ("days ago", None),   # handled specially
        ("day ago", None),
        ("weeks ago", None),
        ("week ago", None),
        ("months ago", None),
        ("month ago", None),
        ("greater than equal to", TokenType.GTE),
        ("greater than", TokenType.GT),
        ("less than equal to", TokenType.LTE),
        ("less than", TokenType.LT),
    ]
    
    SINGLE_KEYWORDS = {
        "latest": TokenType.LATEST,
        "open": TokenType.OPEN,
        "high": TokenType.HIGH,
        "low": TokenType.LOW,
        "close": TokenType.CLOSE,
        "volume": TokenType.VOLUME,
        "sma": TokenType.SMA,
        "ema": TokenType.EMA,
        "rsi": TokenType.RSI,
        "macd": TokenType.MACD,
        "supertrend": TokenType.SUPERTREND,
        "atr": TokenType.ATR,
        "adx": TokenType.ADX,
        "cci": TokenType.CCI,
        "vwap": TokenType.VWAP,
        "mfi": TokenType.MFI,
        "obv": TokenType.OBV,
        "roc": TokenType.ROC,
        "max": TokenType.MAX,
        "min": TokenType.MIN,
        "count": TokenType.COUNT,
        "countstreak": TokenType.COUNTSTREAK,
        "and": TokenType.AND,
        "or": TokenType.OR,
        "where": TokenType.WHERE,
        "cash": TokenType.CASH,
        "futures": TokenType.FUTURES,
    }
    
    def tokenize(self, clause: str) -> list[Token]:
        tokens = []
        # Normalize whitespace
        text = clause.strip().lower()
        i = 0
        
        while i < len(text):
            c = text[i]
            
            # Skip whitespace
            if c in ' \t\n\r':
                i += 1
                continue
            
            # Watchlist IDs like {57960}
            if c == '{':
                end = text.find('}', i)
                if end != -1:
                    tokens.append(Token(TokenType.WATCHLIST, text[i+1:end], i))
                    i = end + 1
                    continue
                i += 1
                continue
            
            # Parentheses
            if c == '(':
                tokens.append(Token(TokenType.LPAREN, '(', i))
                i += 1
                continue
            if c == ')':
                tokens.append(Token(TokenType.RPAREN, ')', i))
                i += 1
                continue
            
            # Comma
            if c == ',':
                tokens.append(Token(TokenType.COMMA, ',', i))
                i += 1
                continue
            
            # Comparison operators
            if c == '>' and i + 1 < len(text) and text[i+1] == '=':
                tokens.append(Token(TokenType.GTE, '>=', i))
                i += 2
                continue
            if c == '>':
                tokens.append(Token(TokenType.GT, '>', i))
                i += 1
                continue
            if c == '<' and i + 1 < len(text) and text[i+1] == '=':
                tokens.append(Token(TokenType.LTE, '<=', i))
                i += 2
                continue
            if c == '<':
                tokens.append(Token(TokenType.LT, '<', i))
                i += 1
                continue
            if c == '=':
                tokens.append(Token(TokenType.EQ, '=', i))
                i += 1
                continue
            
            # Arithmetic operators
            if c == '+':
                tokens.append(Token(TokenType.PLUS, '+', i))
                i += 1
                continue
            if c == '-':
                # Could be negative number or minus operator
                # If previous token is a number/close/field, treat as minus
                if tokens and tokens[-1].type in (
                    TokenType.NUMBER, TokenType.RPAREN,
                    TokenType.CLOSE, TokenType.OPEN, TokenType.HIGH,
                    TokenType.LOW, TokenType.VOLUME
                ):
                    tokens.append(Token(TokenType.MINUS, '-', i))
                    i += 1
                    continue
                # Otherwise might be negative number - check next char
                if i + 1 < len(text) and (text[i+1].isdigit() or text[i+1] == '.'):
                    num, end = self._read_number(text, i)
                    tokens.append(Token(TokenType.NUMBER, num, i))
                    i = end
                    continue
                tokens.append(Token(TokenType.MINUS, '-', i))
                i += 1
                continue
            if c == '*':
                tokens.append(Token(TokenType.MULTIPLY, '*', i))
                i += 1
                continue
            if c == '/':
                tokens.append(Token(TokenType.DIVIDE, '/', i))
                i += 1
                continue
            
            # Numbers
            if c.isdigit() or (c == '.' and i + 1 < len(text) and text[i+1].isdigit()):
                num, end = self._read_number(text, i)
                
                # Check if this number is followed by "day(s) ago", "week(s) ago", "month(s) ago"
                rest = text[end:].lstrip()
                for suffix, period_type in [
                    ("days ago", "days"), ("day ago", "days"),
                    ("weeks ago", "weeks"), ("week ago", "weeks"),
                    ("months ago", "months"), ("month ago", "months"),
                ]:
                    if rest.startswith(suffix):
                        type_map = {
                            "days": TokenType.DAYS_AGO,
                            "weeks": TokenType.WEEKS_AGO,
                            "months": TokenType.MONTHS_AGO,
                        }
                        tokens.append(Token(type_map[period_type], int(num), i))
                        end = end + (len(text[end:]) - len(text[end:].lstrip())) + len(suffix)
                        break
                else:
                    tokens.append(Token(TokenType.NUMBER, num, i))
                
                i = end
                continue
            
            # Words / keywords
            if c.isalpha() or c == '%':
                word, end = self._read_word(text, i)
                
                # Check multi-word keywords first
                found = False
                remaining = text[i:]
                for mw_text, mw_type in self.MULTI_WORD_KEYWORDS:
                    if remaining.startswith(mw_text):
                        # Make sure it's a word boundary
                        next_pos = i + len(mw_text)
                        if next_pos >= len(text) or not text[next_pos].isalpha():
                            if mw_type:
                                tokens.append(Token(mw_type, mw_text, i))
                            i = next_pos
                            found = True
                            break
                
                if found:
                    continue
                
                # Single word keywords
                if word in self.SINGLE_KEYWORDS:
                    tokens.append(Token(self.SINGLE_KEYWORDS[word], word, i))
                else:
                    # Unknown word — skip (might be noise like "the", "is", etc.)
                    pass
                
                i = end
                continue
            
            # Skip unknown characters
            i += 1
        
        tokens.append(Token(TokenType.EOF, None, len(text)))
        return tokens
    
    def _read_number(self, text: str, start: int) -> tuple[float, int]:
        i = start
        if i < len(text) and text[i] == '-':
            i += 1
        while i < len(text) and (text[i].isdigit() or text[i] == '.'):
            i += 1
        try:
            val = float(text[start:i])
        except ValueError:
            val = 0.0
        return val, i
    
    def _read_word(self, text: str, start: int) -> tuple[str, int]:
        i = start
        while i < len(text) and (text[i].isalpha() or text[i] in '_%'):
            i += 1
        return text[start:i], i


# ═══════════════════════════════════════════════════════════════════════════════
# INDICATOR ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class IndicatorEngine:
    """Computes technical indicators from OHLCV DataFrames."""
    
    @staticmethod
    def sma(series: pd.Series, period: int) -> pd.Series:
        return series.rolling(period).mean()
    
    @staticmethod
    def ema(series: pd.Series, period: int) -> pd.Series:
        return series.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def rsi(close: pd.Series, period: int = 14) -> pd.Series:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
    
    @staticmethod
    def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return {"line": macd_line, "signal": signal_line, "histogram": histogram}
    
    @staticmethod
    def bollinger_bands(close: pd.Series, period: int = 20, std: float = 2.0) -> dict:
        sma = close.rolling(period).mean()
        std_dev = close.rolling(period).std()
        return {
            "upper": sma + (std * std_dev),
            "lower": sma - (std * std_dev),
            "middle": sma,
        }
    
    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(period).mean()
    
    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        plus_dm = high.diff()
        minus_dm = -low.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        
        tr = IndicatorEngine.atr(high, low, close, 1)  # single period TR
        atr_val = tr.rolling(period).mean()
        
        plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val)
        minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val)
        
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
        return dx.rolling(period).mean()
    
    @staticmethod
    def cci(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
        tp = (high + low + close) / 3
        sma_tp = tp.rolling(period).mean()
        mad = tp.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
        return (tp - sma_tp) / (0.015 * mad)
    
    @staticmethod
    def supertrend(high: pd.Series, low: pd.Series, close: pd.Series,
                   period: int = 7, multiplier: float = 3.0) -> pd.Series:
        atr_val = IndicatorEngine.atr(high, low, close, period)
        hl2 = (high + low) / 2
        upper_band = hl2 + (multiplier * atr_val)
        lower_band = hl2 - (multiplier * atr_val)
        
        supertrend = pd.Series(index=close.index, dtype=float)
        direction = pd.Series(index=close.index, dtype=int)
        
        supertrend.iloc[0] = upper_band.iloc[0]
        direction.iloc[0] = 1
        
        for i in range(1, len(close)):
            if close.iloc[i] > upper_band.iloc[i-1]:
                direction.iloc[i] = 1
            elif close.iloc[i] < lower_band.iloc[i-1]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[i-1]
            
            if direction.iloc[i] == 1:
                supertrend.iloc[i] = lower_band.iloc[i]
            else:
                supertrend.iloc[i] = upper_band.iloc[i]
        
        return supertrend
    
    @staticmethod
    def mfi(high: pd.Series, low: pd.Series, close: pd.Series,
            volume: pd.Series, period: int = 14) -> pd.Series:
        tp = (high + low + close) / 3
        mf = tp * volume
        
        pos_mf = pd.Series(0.0, index=close.index)
        neg_mf = pd.Series(0.0, index=close.index)
        
        tp_diff = tp.diff()
        pos_mf[tp_diff > 0] = mf[tp_diff > 0]
        neg_mf[tp_diff < 0] = mf[tp_diff < 0]
        
        pos_sum = pos_mf.rolling(period).sum()
        neg_sum = neg_mf.rolling(period).sum()
        
        mfr = pos_sum / neg_sum
        return 100 - (100 / (1 + mfr))
    
    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        direction = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        return (volume * direction).cumsum()
    
    @staticmethod
    def williams_r(high: pd.Series, low: pd.Series, close: pd.Series,
                   period: int = 14) -> pd.Series:
        hh = high.rolling(period).max()
        ll = low.rolling(period).min()
        return -100 * (hh - close) / (hh - ll)
    
    @staticmethod
    def stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                   k_period: int = 14, d_period: int = 3) -> dict:
        hh = high.rolling(k_period).max()
        ll = low.rolling(k_period).min()
        k = 100 * (close - ll) / (hh - ll)
        d = k.rolling(d_period).mean()
        return {"k": k, "d": d}
    
    @staticmethod
    def roc(close: pd.Series, period: int = 12) -> pd.Series:
        return (close - close.shift(period)) / close.shift(period) * 100
    
    @staticmethod
    def vwap(high: pd.Series, low: pd.Series, close: pd.Series,
             volume: pd.Series) -> pd.Series:
        tp = (high + low + close) / 3
        return (tp * volume).cumsum() / volume.cumsum()


# ═══════════════════════════════════════════════════════════════════════════════
# PARSER & EVALUATOR
# ═══════════════════════════════════════════════════════════════════════════════

class ChartInkParser:
    """
    Parses and evaluates ChartInk scan clauses against stock OHLCV data.
    
    Usage:
        parser = ChartInkParser()
        result = parser.evaluate(clause_string, ohlcv_dataframe)
    """
    
    def __init__(self):
        self.tokenizer = Tokenizer()
        self.indicators = IndicatorEngine()
        self.tokens: list[Token] = []
        self.pos: int = 0
        self.df: pd.DataFrame = None
    
    def evaluate(self, clause: str, df: pd.DataFrame) -> bool:
        """
        Evaluate a ChartInk scan clause against a stock's OHLCV DataFrame.
        
        Args:
            clause: ChartInk scan clause string
            df: DataFrame with columns Open, High, Low, Close, Volume (DatetimeIndex)
        
        Returns:
            True if the stock passes the scan, False otherwise
        """
        self.df = df
        self.tokens = self.tokenizer.tokenize(clause)
        self.pos = 0
        
        try:
            result = self._parse_expression()
            return bool(result) if isinstance(result, (bool, np.bool_)) else False
        except Exception as e:
            # If parsing fails, stock doesn't match
            return False
    
    def evaluate_detailed(self, clause: str, df: pd.DataFrame) -> dict:
        """
        Like evaluate() but returns detailed results for debugging.
        """
        self.df = df
        self.tokens = self.tokenizer.tokenize(clause)
        self.pos = 0
        
        try:
            result = self._parse_expression()
            return {
                "match": bool(result) if isinstance(result, (bool, np.bool_)) else False,
                "value": result,
                "error": None,
                "tokens": len(self.tokens),
            }
        except Exception as e:
            return {
                "match": False,
                "value": None,
                "error": str(e),
                "tokens": len(self.tokens),
            }
    
    # ── Token navigation ──
    
    def _current(self) -> Token:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return Token(TokenType.EOF, None)
    
    def _peek(self, offset: int = 0) -> Token:
        idx = self.pos + offset
        if idx < len(self.tokens):
            return self.tokens[idx]
        return Token(TokenType.EOF, None)
    
    def _advance(self) -> Token:
        token = self._current()
        self.pos += 1
        return token
    
    def _expect(self, expected_type: TokenType) -> Token:
        token = self._current()
        if token.type != expected_type:
            raise SyntaxError(
                f"Expected {expected_type.name} but got {token.type.name} "
                f"('{token.value}') at position {token.pos}"
            )
        return self._advance()
    
    def _skip_if(self, *types: TokenType) -> bool:
        if self._current().type in types:
            self._advance()
            return True
        return False
    
    # ── Recursive descent parser ──
    
    def _parse_expression(self) -> Any:
        """Top-level: handles segment wrappers, watchlists, and logical expressions."""
        # Skip outer parentheses and segment markers
        while self._current().type in (TokenType.LPAREN, TokenType.CASH,
                                        TokenType.FUTURES, TokenType.WATCHLIST):
            self._advance()
        
        result = self._parse_logical_or()
        
        # Skip trailing parens
        while self._current().type == TokenType.RPAREN:
            self._advance()
        
        return result
    
    def _parse_logical_or(self) -> Any:
        """Handle OR expressions."""
        left = self._parse_logical_and()
        
        while self._current().type == TokenType.OR:
            self._advance()
            right = self._parse_logical_and()
            left = left or right
        
        return left
    
    def _parse_logical_and(self) -> Any:
        """Handle AND expressions."""
        left = self._parse_comparison()
        
        while self._current().type == TokenType.AND:
            self._advance()
            right = self._parse_comparison()
            left = left and right
        
        return left
    
    def _parse_comparison(self) -> Any:
        """Handle comparison expressions: >, <, >=, <=, ="""
        left = self._parse_additive()
        
        comp_types = {
            TokenType.GT: lambda a, b: a > b,
            TokenType.LT: lambda a, b: a < b,
            TokenType.GTE: lambda a, b: a >= b,
            TokenType.LTE: lambda a, b: a <= b,
            TokenType.EQ: lambda a, b: abs(a - b) < 1e-6 if isinstance(a, float) else a == b,
        }
        
        if self._current().type in comp_types:
            op = comp_types[self._current().type]
            self._advance()
            right = self._parse_additive()
            
            try:
                return op(float(left), float(right))
            except (TypeError, ValueError):
                return False
        
        return left
    
    def _parse_additive(self) -> Any:
        """Handle + and - expressions."""
        left = self._parse_multiplicative()
        
        while self._current().type in (TokenType.PLUS, TokenType.MINUS):
            if self._current().type == TokenType.PLUS:
                self._advance()
                right = self._parse_multiplicative()
                left = float(left) + float(right)
            else:
                self._advance()
                right = self._parse_multiplicative()
                left = float(left) - float(right)
        
        return left
    
    def _parse_multiplicative(self) -> Any:
        """Handle * and / expressions."""
        left = self._parse_unary()
        
        while self._current().type in (TokenType.MULTIPLY, TokenType.DIVIDE):
            if self._current().type == TokenType.MULTIPLY:
                self._advance()
                right = self._parse_unary()
                left = float(left) * float(right)
            else:
                self._advance()
                right = self._parse_unary()
                r = float(right)
                left = float(left) / r if r != 0 else 0
        
        return left
    
    def _parse_unary(self) -> Any:
        """Handle unary minus and primary values."""
        if self._current().type == TokenType.MINUS:
            self._advance()
            return -float(self._parse_primary())
        return self._parse_primary()
    
    def _parse_primary(self) -> Any:
        """
        Parse primary values: numbers, OHLCV references, indicator calls,
        aggregate functions, and grouped expressions.
        """
        token = self._current()
        
        # Number literal
        if token.type == TokenType.NUMBER:
            self._advance()
            return token.value
        
        # Grouped expression
        if token.type == TokenType.LPAREN:
            self._advance()
            # Skip any segment markers inside parens
            while self._current().type in (TokenType.CASH, TokenType.FUTURES, TokenType.WATCHLIST):
                self._advance()
            result = self._parse_logical_or()
            self._skip_if(TokenType.RPAREN)
            return result
        
        # Time-referenced OHLCV: "latest close", "1 day ago high", etc.
        if token.type in (TokenType.LATEST, TokenType.DAYS_AGO,
                          TokenType.WEEKS_AGO, TokenType.MONTHS_AGO):
            return self._parse_time_reference()
        
        # Standalone field (without time prefix — treat as latest)
        if token.type in (TokenType.CLOSE, TokenType.OPEN, TokenType.HIGH,
                          TokenType.LOW, TokenType.VOLUME):
            self._advance()
            return self._get_field_value(token.type, 0)
        
        # Indicator functions that don't need a time prefix
        if token.type in (TokenType.SMA, TokenType.EMA, TokenType.RSI,
                          TokenType.MACD, TokenType.MACD_SIGNAL, TokenType.MACD_HISTOGRAM,
                          TokenType.SUPERTREND, TokenType.UPPER_BOLLINGER,
                          TokenType.LOWER_BOLLINGER, TokenType.ATR, TokenType.ADX,
                          TokenType.CCI, TokenType.VWAP, TokenType.MFI, TokenType.OBV,
                          TokenType.WILLIAMS_R, TokenType.STOCH_K, TokenType.STOCH_D,
                          TokenType.ROC):
            return self._parse_indicator(0)
        
        # Aggregate functions
        if token.type in (TokenType.MAX, TokenType.MIN):
            return self._parse_aggregate()
        
        if token.type in (TokenType.COUNT, TokenType.COUNTSTREAK):
            return self._parse_count()
        
        # Skip unknown tokens
        self._advance()
        return 0
    
    def _parse_time_reference(self) -> Any:
        """Parse a time-prefixed value: 'latest close', '2 days ago sma(...)', etc."""
        token = self._advance()
        
        # Determine offset in trading days
        if token.type == TokenType.LATEST:
            offset = 0
        elif token.type == TokenType.DAYS_AGO:
            offset = int(token.value)
        elif token.type == TokenType.WEEKS_AGO:
            offset = int(token.value) * 5  # approx trading days
        elif token.type == TokenType.MONTHS_AGO:
            offset = int(token.value) * 21  # approx trading days
        else:
            offset = 0
        
        next_token = self._current()
        
        # OHLCV field
        if next_token.type in (TokenType.CLOSE, TokenType.OPEN, TokenType.HIGH,
                                TokenType.LOW, TokenType.VOLUME):
            self._advance()
            return self._get_field_value(next_token.type, offset)
        
        # Indicator function
        if next_token.type in (TokenType.SMA, TokenType.EMA, TokenType.RSI,
                                TokenType.MACD, TokenType.MACD_SIGNAL,
                                TokenType.MACD_HISTOGRAM, TokenType.SUPERTREND,
                                TokenType.UPPER_BOLLINGER, TokenType.LOWER_BOLLINGER,
                                TokenType.ATR, TokenType.ADX, TokenType.CCI,
                                TokenType.VWAP, TokenType.MFI, TokenType.OBV,
                                TokenType.WILLIAMS_R, TokenType.STOCH_K,
                                TokenType.STOCH_D, TokenType.ROC):
            return self._parse_indicator(offset)
        
        # Aggregate functions
        if next_token.type in (TokenType.MAX, TokenType.MIN):
            return self._parse_aggregate(offset)
        
        if next_token.type in (TokenType.COUNT, TokenType.COUNTSTREAK):
            return self._parse_count()
        
        return 0
    
    def _get_field_value(self, field_type: TokenType, offset: int) -> float:
        """Get an OHLCV field value at a given offset from latest."""
        field_map = {
            TokenType.OPEN: "Open",
            TokenType.HIGH: "High",
            TokenType.LOW: "Low",
            TokenType.CLOSE: "Close",
            TokenType.VOLUME: "Volume",
        }
        col = field_map.get(field_type, "Close")
        idx = -(1 + offset)
        
        if col in self.df.columns and abs(idx) <= len(self.df):
            val = self.df[col].iloc[idx]
            return float(val) if not pd.isna(val) else 0.0
        return 0.0
    
    def _parse_indicator(self, offset: int = 0) -> float:
        """Parse and compute an indicator function call."""
        token = self._advance()
        ind_type = token.type
        
        # Read function arguments: ( arg1, arg2, ... )
        args = self._parse_func_args()
        
        try:
            return self._compute_indicator(ind_type, args, offset)
        except Exception:
            return 0.0
    
    def _parse_func_args(self) -> list:
        """Parse function arguments inside parentheses."""
        args = []
        
        if self._current().type != TokenType.LPAREN:
            return args
        
        self._advance()  # skip (
        
        while self._current().type != TokenType.RPAREN and self._current().type != TokenType.EOF:
            # Each arg could be a sub-expression
            arg = self._parse_additive()
            args.append(arg)
            
            if self._current().type == TokenType.COMMA:
                self._advance()
        
        self._skip_if(TokenType.RPAREN)
        return args
    
    def _resolve_series(self, arg) -> pd.Series:
        """
        Resolve an argument to a pd.Series.
        If it's a scalar (from evaluating 'latest close'), use the Close series.
        """
        if isinstance(arg, pd.Series):
            return arg
        # If the arg is a float that matches a column's latest value, likely it's close
        # Default to Close series
        return self.df["Close"]
    
    def _compute_indicator(self, ind_type: TokenType, args: list, offset: int) -> float:
        """Compute an indicator value and return the value at the given offset."""
        close = self.df["Close"]
        high = self.df["High"]
        low = self.df["Low"]
        volume = self.df["Volume"]
        idx = -(1 + offset)
        
        def _safe_get(series: pd.Series) -> float:
            if abs(idx) <= len(series):
                val = series.iloc[idx]
                return float(val) if not pd.isna(val) else 0.0
            return 0.0
        
        if ind_type == TokenType.SMA:
            period = int(args[1]) if len(args) > 1 else int(args[0]) if args else 20
            result = self.indicators.sma(close, period)
            return _safe_get(result)
        
        elif ind_type == TokenType.EMA:
            period = int(args[1]) if len(args) > 1 else int(args[0]) if args else 20
            result = self.indicators.ema(close, period)
            return _safe_get(result)
        
        elif ind_type == TokenType.RSI:
            period = int(args[0]) if args else 14
            result = self.indicators.rsi(close, period)
            return _safe_get(result)
        
        elif ind_type == TokenType.MACD:
            fast = int(args[0]) if len(args) > 0 else 12
            slow = int(args[1]) if len(args) > 1 else 26
            sig = int(args[2]) if len(args) > 2 else 9
            result = self.indicators.macd(close, fast, slow, sig)
            return _safe_get(result["line"])
        
        elif ind_type == TokenType.MACD_SIGNAL:
            fast = int(args[0]) if len(args) > 0 else 12
            slow = int(args[1]) if len(args) > 1 else 26
            sig = int(args[2]) if len(args) > 2 else 9
            result = self.indicators.macd(close, fast, slow, sig)
            return _safe_get(result["signal"])
        
        elif ind_type == TokenType.MACD_HISTOGRAM:
            fast = int(args[0]) if len(args) > 0 else 12
            slow = int(args[1]) if len(args) > 1 else 26
            sig = int(args[2]) if len(args) > 2 else 9
            result = self.indicators.macd(close, fast, slow, sig)
            return _safe_get(result["histogram"])
        
        elif ind_type == TokenType.UPPER_BOLLINGER:
            period = int(args[0]) if len(args) > 0 else 20
            std = float(args[1]) if len(args) > 1 else 2.0
            result = self.indicators.bollinger_bands(close, period, std)
            return _safe_get(result["upper"])
        
        elif ind_type == TokenType.LOWER_BOLLINGER:
            period = int(args[0]) if len(args) > 0 else 20
            std = float(args[1]) if len(args) > 1 else 2.0
            result = self.indicators.bollinger_bands(close, period, std)
            return _safe_get(result["lower"])
        
        elif ind_type == TokenType.ATR:
            period = int(args[0]) if args else 14
            result = self.indicators.atr(high, low, close, period)
            return _safe_get(result)
        
        elif ind_type == TokenType.ADX:
            period = int(args[0]) if args else 14
            result = self.indicators.adx(high, low, close, period)
            return _safe_get(result)
        
        elif ind_type == TokenType.CCI:
            period = int(args[0]) if args else 20
            result = self.indicators.cci(high, low, close, period)
            return _safe_get(result)
        
        elif ind_type == TokenType.SUPERTREND:
            period = int(args[0]) if len(args) > 0 else 7
            mult = float(args[1]) if len(args) > 1 else 3.0
            result = self.indicators.supertrend(high, low, close, period, mult)
            return _safe_get(result)
        
        elif ind_type == TokenType.VWAP:
            result = self.indicators.vwap(high, low, close, volume)
            return _safe_get(result)
        
        elif ind_type == TokenType.MFI:
            period = int(args[0]) if args else 14
            result = self.indicators.mfi(high, low, close, volume, period)
            return _safe_get(result)
        
        elif ind_type == TokenType.OBV:
            result = self.indicators.obv(close, volume)
            return _safe_get(result)
        
        elif ind_type == TokenType.WILLIAMS_R:
            period = int(args[0]) if args else 14
            result = self.indicators.williams_r(high, low, close, period)
            return _safe_get(result)
        
        elif ind_type == TokenType.STOCH_K:
            k_period = int(args[0]) if len(args) > 0 else 14
            d_period = int(args[1]) if len(args) > 1 else 3
            result = self.indicators.stochastic(high, low, close, k_period, d_period)
            return _safe_get(result["k"])
        
        elif ind_type == TokenType.STOCH_D:
            k_period = int(args[0]) if len(args) > 0 else 14
            d_period = int(args[1]) if len(args) > 1 else 3
            result = self.indicators.stochastic(high, low, close, k_period, d_period)
            return _safe_get(result["d"])
        
        elif ind_type == TokenType.ROC:
            period = int(args[0]) if args else 12
            result = self.indicators.roc(close, period)
            return _safe_get(result)
        
        return 0.0
    
    def _parse_aggregate(self, offset: int = 0) -> float:
        """Parse max() or min() aggregate functions."""
        token = self._advance()
        is_max = token.type == TokenType.MAX
        
        args = self._parse_func_args()
        
        if len(args) < 1:
            return 0.0
        
        period = int(args[0])
        
        # The second arg determines which series (default: close for max, low for min)
        # In ChartInk: max(50, latest high) = highest high in last 50 bars
        close = self.df["Close"]
        high = self.df["High"]
        low = self.df["Low"]
        
        # Determine the series - usually the 2nd arg resolves to a field value
        # which tells us which column to use
        series = high if is_max else low  # default
        
        end_idx = len(self.df) - offset
        start_idx = max(0, end_idx - period)
        
        if is_max:
            return float(series.iloc[start_idx:end_idx].max())
        else:
            return float(series.iloc[start_idx:end_idx].min())
    
    def _parse_count(self) -> float:
        """Parse count() function — counts how many times a condition is true in N bars."""
        token = self._advance()
        is_streak = token.type == TokenType.COUNTSTREAK
        
        # count( N, 1 where <condition> )
        if self._current().type != TokenType.LPAREN:
            return 0
        
        self._advance()  # skip (
        
        # Read N
        n_bars = int(self._parse_additive())
        self._skip_if(TokenType.COMMA)
        
        # Read the "1" (threshold — usually 1)
        threshold = self._parse_additive()
        
        # Skip "where"
        self._skip_if(TokenType.WHERE)
        
        # Now we need to evaluate the condition for each of the last N bars
        # Save parser state
        condition_start = self.pos
        count = 0
        streak = 0
        max_streak = 0
        
        for bar_offset in range(n_bars):
            # Reset parser to condition start
            self.pos = condition_start
            # TODO: full per-bar evaluation would need shifting the DataFrame
            # For now, evaluate at current bar only (simplified)
            try:
                result = self._parse_logical_or()
                if result:
                    count += 1
                    streak += 1
                    max_streak = max(max_streak, streak)
                else:
                    streak = 0
            except Exception:
                streak = 0
        
        # Skip to closing paren
        depth = 1
        while depth > 0 and self._current().type != TokenType.EOF:
            if self._current().type == TokenType.LPAREN:
                depth += 1
            elif self._current().type == TokenType.RPAREN:
                depth -= 1
            if depth > 0:
                self._advance()
        self._skip_if(TokenType.RPAREN)
        
        return max_streak if is_streak else count


# ═══════════════════════════════════════════════════════════════════════════════
# PRE-BUILT SCAN PRESETS
# ═══════════════════════════════════════════════════════════════════════════════

PRESET_SCANS = {
    "rsi_oversold": "( cash ( latest rsi( 14 ) < 30 ) )",
    "rsi_overbought": "( cash ( latest rsi( 14 ) > 70 ) )",
    "golden_cross_50_200": "( cash ( latest ema( close, 50 ) > latest ema( close, 200 ) and 1 day ago ema( close, 50 ) <= 1 day ago ema( close, 200 ) ) )",
    "death_cross_50_200": "( cash ( latest ema( close, 50 ) < latest ema( close, 200 ) and 1 day ago ema( close, 50 ) >= 1 day ago ema( close, 200 ) ) )",
    "macd_bullish_crossover": "( cash ( latest macd( 12, 26, 9 ) > latest macd signal( 12, 26, 9 ) and 1 day ago macd( 12, 26, 9 ) <= 1 day ago macd signal( 12, 26, 9 ) ) )",
    "macd_bearish_crossover": "( cash ( latest macd( 12, 26, 9 ) < latest macd signal( 12, 26, 9 ) and 1 day ago macd( 12, 26, 9 ) >= 1 day ago macd signal( 12, 26, 9 ) ) )",
    "volume_3x_spike": "( cash ( latest volume >= 1 day ago sma( volume, 20 ) * 3 ) )",
    "volume_2x_spike": "( cash ( latest volume >= 1 day ago sma( volume, 20 ) * 2 ) )",
    "bollinger_squeeze": "( cash ( latest upper bollinger band( 20, 2 ) - latest lower bollinger band( 20, 2 ) < 1 day ago upper bollinger band( 20, 2 ) - 1 day ago lower bollinger band( 20, 2 ) ) )",
    "above_200_ema": "( cash ( latest close > latest ema( close, 200 ) ) )",
    "below_200_ema": "( cash ( latest close < latest ema( close, 200 ) ) )",
    "52w_high_breakout": "( cash ( latest close >= latest max( 252, latest high ) * 0.98 ) )",
    "52w_low_breakdown": "( cash ( latest close <= latest min( 252, latest low ) * 1.02 ) )",
    "trend_template_minervini": "( cash ( latest close > 30 and latest ema( close, 200 ) > 1 month ago ema( close, 200 ) and latest close > latest ema( close, 200 ) and latest close > latest ema( close, 150 ) and latest close > latest ema( close, 50 ) and latest ema( close, 50 ) > latest ema( close, 150 ) and latest ema( close, 150 ) > latest ema( close, 200 ) ) )",
    "pocket_pivot_volume": "( cash ( latest volume > latest max( 10, latest volume ) and latest close > 1 day ago close ) )",
    "inside_bar": "( cash ( latest high < 1 day ago high and latest low > 1 day ago low ) )",
    "outside_bar": "( cash ( latest high > 1 day ago high and latest low < 1 day ago low ) )",
    "bullish_engulfing": "( cash ( 1 day ago close < 1 day ago open and latest close > latest open and latest close > 1 day ago open and latest open < 1 day ago close ) )",
    "bearish_engulfing": "( cash ( 1 day ago close > 1 day ago open and latest close < latest open and latest close < 1 day ago open and latest open > 1 day ago close ) )",
    "supertrend_positive": "( cash ( latest close > latest supertrend( 7, 3 ) ) )",
    "supertrend_negative": "( cash ( latest close < latest supertrend( 7, 3 ) ) )",
}


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def scan_stock(clause: str, df: pd.DataFrame) -> bool:
    """Quick scan a single stock against a clause."""
    parser = ChartInkParser()
    return parser.evaluate(clause, df)


def scan_universe(clause: str, stock_data: dict[str, pd.DataFrame]) -> list[str]:
    """
    Scan all stocks in the universe against a clause.
    
    Args:
        clause: ChartInk scan clause
        stock_data: dict of {symbol: DataFrame}
    
    Returns:
        List of symbols that match the scan
    """
    parser = ChartInkParser()
    matches = []
    
    for symbol, df in stock_data.items():
        if df is not None and len(df) >= 50:
            if parser.evaluate(clause, df):
                matches.append(symbol)
    
    return matches


def get_preset_names() -> list[str]:
    """Return list of available preset scan names."""
    return list(PRESET_SCANS.keys())


def get_preset_clause(name: str) -> str | None:
    """Get the clause for a preset scan."""
    return PRESET_SCANS.get(name)


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  TradEdge ChartInk Parser v1.0 — Self Test")
    print("=" * 60)
    
    # Create synthetic test data
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=300, freq="B")
    price = 100 + np.cumsum(np.random.randn(300) * 1.5)
    
    df = pd.DataFrame({
        "Open": price + np.random.randn(300) * 0.5,
        "High": price + abs(np.random.randn(300) * 1.0),
        "Low": price - abs(np.random.randn(300) * 1.0),
        "Close": price,
        "Volume": np.random.randint(100000, 5000000, 300).astype(float),
    }, index=dates)
    
    parser = ChartInkParser()
    
    # Test cases
    test_clauses = [
        ("Simple: close > 50", "( cash ( latest close > 50 ) )"),
        ("SMA crossover", "( cash ( latest sma( close, 50 ) > latest sma( close, 200 ) ) )"),
        ("RSI oversold", "( cash ( latest rsi( 14 ) < 30 ) )"),
        ("RSI overbought", "( cash ( latest rsi( 14 ) > 70 ) )"),
        ("Volume spike 2x", "( cash ( latest volume >= latest sma( volume, 20 ) * 2 ) )"),
        ("Above 200 EMA", "( cash ( latest close > latest ema( close, 200 ) ) )"),
        ("MACD positive", "( cash ( latest macd( 12, 26, 9 ) > 0 ) )"),
        ("Bollinger upper break", "( cash ( latest close > latest upper bollinger band( 20, 2 ) ) )"),
        ("Compound AND", "( cash ( latest close > latest ema( close, 200 ) and latest rsi( 14 ) > 50 ) )"),
        ("Time reference", "( cash ( latest close > 5 days ago close ) )"),
        ("Minervini template", PRESET_SCANS["trend_template_minervini"]),
        ("Inside bar", PRESET_SCANS["inside_bar"]),
        ("SuperTrend positive", PRESET_SCANS["supertrend_positive"]),
    ]
    
    print(f"\nTesting against synthetic data (300 bars, latest close: {price[-1]:.2f})")
    print(f"Latest RSI(14): {IndicatorEngine.rsi(df['Close'], 14).iloc[-1]:.2f}")
    print(f"Latest SMA(200): {IndicatorEngine.sma(df['Close'], 200).iloc[-1]:.2f}")
    print()
    
    for name, clause in test_clauses:
        result = parser.evaluate_detailed(clause, df)
        status = "✓ MATCH" if result["match"] else "✗ no match"
        err = f" [ERR: {result['error']}]" if result["error"] else ""
        print(f"  {status}  {name}{err}")
    
    # Test tokenizer
    print(f"\n{'=' * 60}")
    print("  Tokenizer test")
    print(f"{'=' * 60}")
    
    sample = "( cash ( latest sma( latest close , 7 ) / latest sma( latest close , 65 ) >= 1.0 ) )"
    tokens = Tokenizer().tokenize(sample)
    print(f"\n  Input: {sample}")
    print(f"  Tokens ({len(tokens)}):")
    for t in tokens:
        if t.type != TokenType.EOF:
            print(f"    {t.type.name:20s} = {t.value}")
    
    print(f"\n{'=' * 60}")
    print(f"  Available presets: {len(PRESET_SCANS)}")
    for name in PRESET_SCANS:
        print(f"    • {name}")
    print(f"{'=' * 60}")

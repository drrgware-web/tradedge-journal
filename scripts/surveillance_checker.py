#!/usr/bin/env python3
"""
TradEdge Surveillance Checker v1.0
====================================
Checks stocks against safety/surveillance criteria to flag red flags.

Checks:
  1. NSE ASM (Additional Surveillance Measure) list
  2. NSE GSM (Graded Surveillance Measure) list
  3. High Debt Company (Debt/Equity > 200%)
  4. No/Limited Institutional Holding (< 5%)
  5. Penny Stock (Price < ₹10 or MCap < ₹100 Cr)
  6. High Promoter Pledging (> 50%)
  7. Low Liquidity (Avg volume < 10,000)
  8. Decline in Institutional Sponsors (QoQ)
  9. Decline in Shares held by Institutions (QoQ)

Output:
  - Surveillance checklist with pass/fail flags
  - Red flag count
  - Overall safety assessment
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SurveillanceFlag:
    """A single surveillance check result."""
    name: str
    description: str
    is_flagged: bool    # True = RED FLAG (bad)
    severity: str       # "high", "medium", "low"
    detail: str = ""
    
    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "flagged": self.is_flagged,
            "severity": self.severity,
            "detail": self.detail,
        }


@dataclass 
class SurveillanceResult:
    """Complete surveillance checklist result."""
    flags: list = field(default_factory=list)
    red_flag_count: int = 0
    total_checks: int = 0
    assessment: str = ""
    assessment_detail: str = ""
    
    def to_dict(self):
        return {
            "flags": [f.to_dict() for f in self.flags],
            "red_flag_count": self.red_flag_count,
            "total_checks": self.total_checks,
            "assessment": self.assessment,
            "assessment_detail": self.assessment_detail,
        }


class SurveillanceChecker:
    """Runs all surveillance checks on a stock."""
    
    # Known ASM/GSM stocks (this list should be updated periodically)
    # In production, fetch from NSE: https://www.nseindia.com/regulations/asm-gsm
    ASM_STOCKS = set()  # Populated at runtime from NSE data
    GSM_STOCKS = set()
    
    def check(self, symbol: str, df: pd.DataFrame, info: dict,
              prev_info: dict = None) -> SurveillanceResult:
        """
        Run all surveillance checks.
        
        Args:
            symbol: NSE symbol
            df: OHLCV DataFrame
            info: Current yfinance ticker.info
            prev_info: Previous quarter's info (for QoQ comparison)
        """
        flags = []
        
        # 1. ASM List
        flags.append(self._check_asm(symbol))
        
        # 2. GSM List
        flags.append(self._check_gsm(symbol))
        
        # 3. High Debt
        flags.append(self._check_high_debt(info))
        
        # 4. No/Limited Institutional Holding
        flags.append(self._check_institutional_holding(info))
        
        # 5. Penny Stock
        flags.append(self._check_penny_stock(df, info))
        
        # 6. High Promoter Pledging
        flags.append(self._check_promoter_pledging(info))
        
        # 7. Low Liquidity
        flags.append(self._check_low_liquidity(df))
        
        # 8. Decline in Institutional Sponsors
        flags.append(self._check_institutional_decline(info, prev_info))
        
        # 9. Decline in Shares held by Institutions
        flags.append(self._check_institutional_shares_decline(info, prev_info))
        
        # Count flags
        red_count = sum(1 for f in flags if f.is_flagged)
        total = len(flags)
        
        # Assessment
        if red_count == 0:
            assessment = "No Red Flags"
            detail = ("No obvious concerns with the stock. However, make sure you do "
                     "the required fundamental & technical analysis before investing.")
        elif red_count <= 2:
            assessment = f"{red_count} Caution Flag(s)"
            detail = ("Some concerns detected. Review the flagged items carefully before "
                     "making any investment decisions.")
        else:
            assessment = f"{red_count} Red Flags"
            detail = ("Multiple concerns detected. Exercise extreme caution. This stock "
                     "has significant risk factors that need thorough investigation.")
        
        return SurveillanceResult(
            flags=flags,
            red_flag_count=red_count,
            total_checks=total,
            assessment=assessment,
            assessment_detail=detail,
        )
    
    def _check_asm(self, symbol: str) -> SurveillanceFlag:
        """Check if stock is on NSE ASM list."""
        is_asm = symbol.upper() in self.ASM_STOCKS
        return SurveillanceFlag(
            name="NSE Additional Surveillance Measure (ASM) list",
            description="Stock is under additional surveillance by NSE",
            is_flagged=is_asm,
            severity="high",
            detail="Stock is on ASM list" if is_asm else "Not on ASM list",
        )
    
    def _check_gsm(self, symbol: str) -> SurveillanceFlag:
        """Check if stock is on NSE GSM list."""
        is_gsm = symbol.upper() in self.GSM_STOCKS
        return SurveillanceFlag(
            name="NSE Graded Surveillance Measure (GSM) list",
            description="Stock is under graded surveillance by NSE",
            is_flagged=is_gsm,
            severity="high",
            detail="Stock is on GSM list" if is_gsm else "Not on GSM list",
        )
    
    def _check_high_debt(self, info: dict) -> SurveillanceFlag:
        """Check for dangerously high debt levels."""
        de = info.get("debtToEquity")
        
        if de is None:
            return SurveillanceFlag(
                name="High Debt Company",
                description="Debt/Equity ratio > 200%",
                is_flagged=False,
                severity="medium",
                detail="Debt/Equity data not available",
            )
        
        is_high = de > 200
        return SurveillanceFlag(
            name="High Debt Company",
            description="Debt/Equity ratio > 200%",
            is_flagged=is_high,
            severity="high" if de > 300 else "medium",
            detail=f"Debt/Equity: {de:.0f}%",
        )
    
    def _check_institutional_holding(self, info: dict) -> SurveillanceFlag:
        """Check for low/no institutional holding."""
        inst = info.get("heldPercentInstitutions")
        
        if inst is None:
            return SurveillanceFlag(
                name="No OR limited institutional holding",
                description="Institutional holding < 5%",
                is_flagged=True,
                severity="medium",
                detail="Institutional holding data not available",
            )
        
        pct = inst * 100
        is_low = pct < 5
        return SurveillanceFlag(
            name="No OR limited institutional holding",
            description="Institutional holding < 5%",
            is_flagged=is_low,
            severity="medium",
            detail=f"Institutional holding: {pct:.1f}%",
        )
    
    def _check_penny_stock(self, df: pd.DataFrame, info: dict) -> SurveillanceFlag:
        """Check if stock qualifies as penny stock."""
        price = float(df["Close"].iloc[-1]) if df is not None and len(df) > 0 else 0
        mcap = info.get("marketCap", 0) or 0
        mcap_cr = mcap / 1e7
        
        is_penny = price < 10 or mcap_cr < 100
        
        detail_parts = []
        if price < 10:
            detail_parts.append(f"Price ₹{price:.2f} (< ₹10)")
        if mcap_cr < 100:
            detail_parts.append(f"MCap ₹{mcap_cr:.0f} Cr (< ₹100 Cr)")
        
        return SurveillanceFlag(
            name="Penny Stock",
            description="Price < ₹10 or Market Cap < ₹100 Cr",
            is_flagged=is_penny,
            severity="high",
            detail="; ".join(detail_parts) if detail_parts else f"Price ₹{price:.2f}, MCap ₹{mcap_cr:.0f} Cr",
        )
    
    def _check_promoter_pledging(self, info: dict) -> SurveillanceFlag:
        """Check for high promoter pledging."""
        # yfinance doesn't directly provide pledging data
        # This would need NSE corporate filings data
        # For now, use insider holding as a proxy
        insider = info.get("heldPercentInsiders")
        
        if insider is None:
            return SurveillanceFlag(
                name="High Promoter Pledging",
                description="Promoter pledging > 50% of holdings",
                is_flagged=False,
                severity="high",
                detail="Promoter pledging data not available via yfinance (check NSE filings)",
            )
        
        # Low promoter holding can indicate pledging or dilution
        pct = insider * 100
        is_concerning = pct < 20  # Very low promoter holding
        
        return SurveillanceFlag(
            name="High Promoter Pledging",
            description="Promoter holding very low (< 20%) — may indicate pledging",
            is_flagged=is_concerning,
            severity="medium",
            detail=f"Promoter/Insider holding: {pct:.1f}%",
        )
    
    def _check_low_liquidity(self, df: pd.DataFrame) -> SurveillanceFlag:
        """Check for low trading liquidity."""
        if df is None or len(df) < 20:
            return SurveillanceFlag(
                name="Low liquidity",
                description="Average daily volume < 10,000 shares",
                is_flagged=True,
                severity="medium",
                detail="Insufficient data",
            )
        
        avg_vol = float(df["Volume"].tail(20).mean())
        is_low = avg_vol < 10000
        
        if avg_vol >= 1000000:
            vol_str = f"{avg_vol/1e6:.1f}M"
        elif avg_vol >= 1000:
            vol_str = f"{avg_vol/1e3:.0f}K"
        else:
            vol_str = f"{avg_vol:.0f}"
        
        return SurveillanceFlag(
            name="Low liquidity",
            description="Average daily volume < 10,000 shares",
            is_flagged=is_low,
            severity="medium" if avg_vol > 5000 else "high",
            detail=f"Avg 20-day volume: {vol_str}",
        )
    
    def _check_institutional_decline(self, info: dict, prev_info: dict = None) -> SurveillanceFlag:
        """Check if number of institutional sponsors is declining."""
        if prev_info is None:
            return SurveillanceFlag(
                name="Decline in number of Institutional Sponsors",
                description="QoQ decline in institutional fund count",
                is_flagged=False,
                severity="medium",
                detail="Previous quarter data not available for comparison",
            )
        
        current_inst = info.get("heldPercentInstitutions", 0) or 0
        prev_inst = prev_info.get("heldPercentInstitutions", 0) or 0
        
        is_declining = current_inst < prev_inst and prev_inst > 0
        change_pct = ((current_inst - prev_inst) / prev_inst * 100) if prev_inst > 0 else 0
        
        return SurveillanceFlag(
            name="Decline in number of Institutional Sponsors",
            description="QoQ decline in institutional participation",
            is_flagged=is_declining,
            severity="medium",
            detail=f"Change: {change_pct:+.1f}% (Current: {current_inst*100:.1f}%, Prev: {prev_inst*100:.1f}%)",
        )
    
    def _check_institutional_shares_decline(self, info: dict, prev_info: dict = None) -> SurveillanceFlag:
        """Check if shares held by institutions is declining."""
        if prev_info is None:
            return SurveillanceFlag(
                name="Decline in shares held by Institutional Sponsors",
                description="QoQ decline in shares held by institutions",
                is_flagged=False,
                severity="medium",
                detail="Previous quarter data not available for comparison",
            )
        
        current = info.get("heldPercentInstitutions", 0) or 0
        prev = prev_info.get("heldPercentInstitutions", 0) or 0
        
        is_declining = current < prev and prev > 0
        change_pct = ((current - prev) / prev * 100) if prev > 0 else 0
        
        return SurveillanceFlag(
            name="Decline in shares held by Institutional Sponsors",
            description="QoQ decline in institutional share holdings",
            is_flagged=is_declining,
            severity="medium",
            detail=f"Change: {change_pct:+.1f}%",
        )
    
    @classmethod
    def load_asm_gsm_lists(cls, asm_symbols: list[str] = None, gsm_symbols: list[str] = None):
        """
        Load ASM/GSM lists. In production, fetch from:
        https://www.nseindia.com/regulations/additional-surveillance-measures-asm
        https://www.nseindia.com/regulations/graded-surveillance-measure-gsm
        """
        if asm_symbols:
            cls.ASM_STOCKS = set(s.upper() for s in asm_symbols)
        if gsm_symbols:
            cls.GSM_STOCKS = set(s.upper() for s in gsm_symbols)


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  Surveillance Checker — Self Test")
    print("=" * 60)
    
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=300, freq="B")
    price = 150 + np.cumsum(np.random.randn(300) * 2)
    
    # Test 1: Healthy stock
    df_good = pd.DataFrame({
        "Open": price, "High": price + 2, "Low": price - 2,
        "Close": price,
        "Volume": np.random.randint(500000, 5000000, 300).astype(float),
    }, index=dates)
    
    info_good = {
        "marketCap": 50000000000,
        "debtToEquity": 45,
        "heldPercentInstitutions": 0.35,
        "heldPercentInsiders": 0.45,
    }
    
    checker = SurveillanceChecker()
    result = checker.check("RELIANCE", df_good, info_good)
    
    print(f"\n  Stock: RELIANCE (healthy)")
    print(f"  Assessment: {result.assessment}")
    print(f"  Red Flags: {result.red_flag_count}/{result.total_checks}")
    for f in result.flags:
        icon = "🚩" if f.is_flagged else "✅"
        print(f"    {icon} {f.name}: {f.detail}")
    
    # Test 2: Risky stock
    price_penny = np.full(300, 5.0) + np.random.randn(300) * 0.5
    df_bad = pd.DataFrame({
        "Open": price_penny, "High": price_penny + 0.5, "Low": price_penny - 0.5,
        "Close": price_penny,
        "Volume": np.random.randint(100, 5000, 300).astype(float),
    }, index=dates)
    
    info_bad = {
        "marketCap": 50000000,   # ₹5 Cr
        "debtToEquity": 350,
        "heldPercentInstitutions": 0.02,
        "heldPercentInsiders": 0.15,
    }
    
    SurveillanceChecker.load_asm_gsm_lists(asm_symbols=["TESTBAD"])
    result2 = checker.check("TESTBAD", df_bad, info_bad)
    
    print(f"\n  Stock: TESTBAD (risky)")
    print(f"  Assessment: {result2.assessment}")
    print(f"  Red Flags: {result2.red_flag_count}/{result2.total_checks}")
    for f in result2.flags:
        icon = "🚩" if f.is_flagged else "✅"
        print(f"    {icon} {f.name}: {f.detail}")
    
    print(f"\n{'=' * 60}")

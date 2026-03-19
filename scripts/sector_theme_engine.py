#!/usr/bin/env python3
"""
TradEdge Hot Sector / Theme Detection Engine v1.0
====================================================
Detects leading/lagging sectors, tracks rotation, generates RS heatmaps,
integrates with RRM v4.0, and provides AI-powered insights via Groq.

Features:
  - Sectoral RS Rating across 3 timeframes (3M, 1M, 1W)
  - Sector rotation tracking (which sectors are accelerating/decelerating)
  - Theme momentum scoring (Defence, Power, PSU Banks, etc.)
  - RRM quadrant integration (Leading, Weakening, Improving, Lagging)
  - Groq AI-powered market narrative + sector insights
  - Treemap data output for frontend visualization

Data Sources:
  - scanner_results.json (stock-level RS and sector tags)
  - rrm_data.json (existing RRM data if available)
  - Groq API for AI insights

Output:
  - data/sector_heatmap.json
  - data/sector_analysis.json (with AI insights)

Setup:
  GROQ_API_KEY=your_groq_key (env var or .env file)
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from collections import defaultdict

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
HEATMAP_FILE = DATA_DIR / "sector_heatmap.json"
ANALYSIS_FILE = DATA_DIR / "sector_analysis.json"
RRM_DATA_FILE = DATA_DIR / "rrm_data.json"
SCANNER_FILE = DATA_DIR / "scanner_results.json"

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.3-70b-versatile"


# ═══════════════════════════════════════════════════════════════
# SECTOR DEFINITIONS
# ═══════════════════════════════════════════════════════════════

# Map ChartInk/yfinance industries to unified sector names
SECTOR_MAP = {
    # Banking
    "Banking": "Banks - PSU", "Banks - PSU": "Banks - PSU",
    "Banks - Pvt.": "Banks - Pvt.", "Private Banks": "Banks - Pvt.",
    "NBFC": "Housing Finance", "Housing Finance": "Housing Finance",
    "Finance - NBFC": "Housing Finance", "Financial Services": "Financial Servi...",
    "Finance - Stock Broking": "Broking", "Broking": "Broking",
    "Finance - Investment": "Financial Servi...",
    "Insurance": "Insurance",

    # IT
    "IT": "IT", "IT - Software": "IT", "Technology": "IT",

    # Pharma / Healthcare
    "Pharma": "Pharma", "Pharmaceuticals & Drugs": "Pharma",
    "Healthcare": "Healthcare...", "Healthcare Services": "Healthcare...",

    # Auto
    "Auto": "Automobiles", "Automobile": "Automobiles", "Auto Ancillary": "Automobiles",

    # Consumer
    "FMCG": "Consumer...", "Consumer Goods": "Consumer...", "Consumer Food": "Consumer...",
    "Retailing": "Consumer...", "Household & Personal Products": "Consumer...",

    # Metals / Mining
    "Metals": "Metals & Mining", "Mining": "Metals & Mining",
    "Steel & Iron Products": "Metals & Mining", "Mining & Minerals": "Metals & Mining",

    # Power / Energy
    "Power": "Power", "Energy": "Power",
    "Oil & Gas": "Power", "Petroleum Products": "Power",

    # Industrial / Infra
    "Infrastructure": "Industrials", "Engineering - Construction": "Industrials",
    "Engineering - Industrial Equipments": "Industrials",
    "Cement": "Cement", "Cement & Construction Materials": "Cement",

    # Real Estate
    "Real Estate": "Realty", "Construction - Real Estate": "Realty",

    # Telecom
    "Telecom": "Telecom", "Telecommunication": "Telecom",

    # Defence
    "Defence": "Defence", "Aerospace & Defense": "Defence",

    # Chemicals
    "Chemicals": "Chemicals", "Pesticides & Agrochemicals": "Chemicals",

    # Textiles
    "Textiles": "Textile", "Textile": "Textile",

    # Paper
    "Paper": "Paper", "Paper & Paper Products": "Paper",

    # Logistics
    "Logistics": "Logistics",

    # Media
    "Media": "Media", "Media & Entertainment": "Media",

    # Hotels
    "Hotel  Resort & Restaurants": "Hotels", "Hotels": "Hotels",

    # Sugar
    "Sugar": "Sugar",

    # Fertilizer
    "Fertilizers": "Fertilize...",

    # Packaging
    "Packaging": "Packaging",

    # Misc
    "Conglomerate": "HoldCo*", "Diversified": "HoldCo*",
    "Misc.": "Misc.",
    "Gems & Jewellery": "Gems & Jewellery",
    "Pipes": "Pipes",
    "Railways": "Railways",
    "Online": "Online",
    "Retail": "Retail",
    "Electric Equipment": "Industrials",
    "Plastic Products": "Industrials",
    "Trading": "Misc.",
    "Environmental Services": "Industrials",
}

# Theme groupings (multiple sectors → theme)
THEMES = {
    "PSU Pack": ["Banks - PSU", "Power", "Defence", "Railways", "Fertilize..."],
    "Digital India": ["IT", "Online", "Telecom"],
    "Infra Boom": ["Industrials", "Cement", "Metals & Mining", "Realty"],
    "Green Energy": ["Power"],  # Would include specific renewable stocks
    "Consumer Revival": ["Consumer...", "Hotels", "Retail", "Automobiles"],
    "Financialization": ["Banks - Pvt.", "Banks - PSU", "Housing Finance", "Insurance", "Broking"],
    "China+1 / Make in India": ["Chemicals", "Textile", "Industrials", "Defence"],
}


# ═══════════════════════════════════════════════════════════════
# SECTOR RS CALCULATOR
# ═══════════════════════════════════════════════════════════════

@dataclass
class SectorRS:
    """Relative Strength data for a sector."""
    name: str
    stock_count: int = 0
    # RS ratings (0-100, higher = stronger)
    rs_3m: float = 50
    rs_1m: float = 50
    rs_1w: float = 50
    # Avg returns
    avg_return_3m: float = 0
    avg_return_1m: float = 0
    avg_return_1w: float = 0
    # Momentum
    momentum_score: float = 0  # Weighted composite
    momentum_direction: str = "neutral"  # accelerating, decelerating, neutral
    # RRM quadrant
    rrm_quadrant: str = "unknown"  # leading, weakening, improving, lagging
    # Breadth
    pct_above_50ema: float = 0
    pct_bullish: float = 0  # % of stocks with composite_score > 0
    # Top stocks
    top_stocks: list = field(default_factory=list)
    bottom_stocks: list = field(default_factory=list)

    def to_dict(self):
        return {
            "name": self.name, "stock_count": self.stock_count,
            "rs_3m": round(self.rs_3m, 1), "rs_1m": round(self.rs_1m, 1), "rs_1w": round(self.rs_1w, 1),
            "avg_return_3m": round(self.avg_return_3m, 2), "avg_return_1m": round(self.avg_return_1m, 2),
            "avg_return_1w": round(self.avg_return_1w, 2),
            "momentum_score": round(self.momentum_score, 2), "momentum_direction": self.momentum_direction,
            "rrm_quadrant": self.rrm_quadrant,
            "pct_above_50ema": round(self.pct_above_50ema, 1), "pct_bullish": round(self.pct_bullish, 1),
            "top_stocks": self.top_stocks[:5], "bottom_stocks": self.bottom_stocks[:5],
        }


class SectorAnalyzer:
    """Analyzes sector performance, rotation, and themes."""

    def __init__(self):
        self.sectors: dict[str, SectorRS] = {}
        self.themes: dict[str, dict] = {}

    def analyze(self, stocks: list[dict]) -> dict:
        """Run full sector analysis on scanner results."""

        # Group stocks by sector
        sector_stocks = defaultdict(list)
        for stock in stocks:
            raw_sector = stock.get("sector", "Unknown")
            sector = SECTOR_MAP.get(raw_sector, raw_sector)
            if sector == "Unknown" or not sector:
                sector = "Misc."
            sector_stocks[sector].append(stock)

        # Calculate RS for each sector
        all_returns_3m = [s.get("returns", {}).get("3m", 0) or 0 for s in stocks]
        all_returns_1m = [s.get("returns", {}).get("1m", 0) or 0 for s in stocks]
        all_returns_1w = [s.get("returns", {}).get("1w", 0) or 0 for s in stocks]

        for sector_name, sector_stock_list in sector_stocks.items():
            if len(sector_stock_list) < 2:
                continue

            sr = SectorRS(name=sector_name, stock_count=len(sector_stock_list))

            # Avg returns
            returns_3m = [s.get("returns", {}).get("3m", 0) or 0 for s in sector_stock_list]
            returns_1m = [s.get("returns", {}).get("1m", 0) or 0 for s in sector_stock_list]
            returns_1w = [s.get("returns", {}).get("1w", 0) or 0 for s in sector_stock_list]

            sr.avg_return_3m = np.mean(returns_3m) if returns_3m else 0
            sr.avg_return_1m = np.mean(returns_1m) if returns_1m else 0
            sr.avg_return_1w = np.mean(returns_1w) if returns_1w else 0

            # RS Rating (percentile rank vs all sectors)
            sr.rs_3m = self._percentile_rank(sr.avg_return_3m, all_returns_3m)
            sr.rs_1m = self._percentile_rank(sr.avg_return_1m, all_returns_1m)
            sr.rs_1w = self._percentile_rank(sr.avg_return_1w, all_returns_1w)

            # Momentum score (weighted: 40% 1W + 30% 1M + 30% 3M)
            sr.momentum_score = sr.rs_1w * 0.4 + sr.rs_1m * 0.3 + sr.rs_3m * 0.3

            # Momentum direction
            if sr.rs_1w > sr.rs_1m > sr.rs_3m:
                sr.momentum_direction = "accelerating"
            elif sr.rs_1w < sr.rs_1m < sr.rs_3m:
                sr.momentum_direction = "decelerating"
            elif sr.rs_1w > sr.rs_3m:
                sr.momentum_direction = "improving"
            else:
                sr.momentum_direction = "neutral"

            # RRM Quadrant mapping
            # Leading: strong RS + accelerating
            # Weakening: strong RS + decelerating
            # Improving: weak RS + accelerating
            # Lagging: weak RS + decelerating
            rs_mid = 50
            if sr.rs_1m >= rs_mid and sr.momentum_direction in ("accelerating", "improving"):
                sr.rrm_quadrant = "leading"
            elif sr.rs_1m >= rs_mid and sr.momentum_direction in ("decelerating", "neutral"):
                sr.rrm_quadrant = "weakening"
            elif sr.rs_1m < rs_mid and sr.momentum_direction in ("accelerating", "improving"):
                sr.rrm_quadrant = "improving"
            else:
                sr.rrm_quadrant = "lagging"

            # Breadth
            above_50ema = sum(1 for s in sector_stock_list
                             if s.get("ema", {}).get("ema50") and s.get("price", 0) > s["ema"]["ema50"])
            sr.pct_above_50ema = (above_50ema / len(sector_stock_list) * 100) if sector_stock_list else 0

            bullish = sum(1 for s in sector_stock_list if s.get("composite_score", 0) > 0)
            sr.pct_bullish = (bullish / len(sector_stock_list) * 100) if sector_stock_list else 0

            # Top / bottom stocks
            sorted_stocks = sorted(sector_stock_list, key=lambda s: s.get("composite_score", 0), reverse=True)
            sr.top_stocks = [{"symbol": s["symbol"], "score": s.get("composite_score", 0),
                              "change": s.get("change_pct", 0)} for s in sorted_stocks[:5]]
            sr.bottom_stocks = [{"symbol": s["symbol"], "score": s.get("composite_score", 0),
                                 "change": s.get("change_pct", 0)} for s in sorted_stocks[-5:]]

            self.sectors[sector_name] = sr

        # Analyze themes
        self._analyze_themes()

        return self._build_output()

    def _percentile_rank(self, value: float, all_values: list[float]) -> float:
        """Calculate percentile rank (0-100)."""
        if not all_values:
            return 50
        below = sum(1 for v in all_values if v < value)
        return (below / len(all_values)) * 100

    def _analyze_themes(self):
        """Score themes based on constituent sector performance."""
        for theme_name, theme_sectors in THEMES.items():
            scores = []
            for sec_name in theme_sectors:
                if sec_name in self.sectors:
                    scores.append(self.sectors[sec_name].momentum_score)

            if scores:
                self.themes[theme_name] = {
                    "name": theme_name,
                    "avg_momentum": round(np.mean(scores), 2),
                    "sectors": theme_sectors,
                    "sector_count": len(scores),
                    "is_hot": np.mean(scores) > 60,
                }

    def _build_output(self) -> dict:
        """Build the final output dictionary."""
        # Sort sectors by momentum
        sorted_sectors = sorted(self.sectors.values(), key=lambda s: s.momentum_score, reverse=True)

        # RRM matrix
        matrix = {"leading": [], "weakening": [], "improving": [], "lagging": []}
        for sr in sorted_sectors:
            matrix[sr.rrm_quadrant].append(sr.name)

        # Treemap data (for frontend)
        treemap_3m = [{"name": sr.name, "value": sr.stock_count, "rs": sr.rs_3m,
                        "return": sr.avg_return_3m, "quadrant": sr.rrm_quadrant}
                       for sr in sorted_sectors]
        treemap_1m = [{"name": sr.name, "value": sr.stock_count, "rs": sr.rs_1m,
                        "return": sr.avg_return_1m, "quadrant": sr.rrm_quadrant}
                       for sr in sorted_sectors]
        treemap_1w = [{"name": sr.name, "value": sr.stock_count, "rs": sr.rs_1w,
                        "return": sr.avg_return_1w, "quadrant": sr.rrm_quadrant}
                       for sr in sorted_sectors]

        # Hot themes
        hot_themes = sorted(self.themes.values(), key=lambda t: t["avg_momentum"], reverse=True)

        # Rotation signals
        rotation_signals = []
        for sr in sorted_sectors:
            if sr.momentum_direction == "accelerating" and sr.rs_1m > 60:
                rotation_signals.append({
                    "sector": sr.name, "signal": "ROTATING IN",
                    "detail": f"RS accelerating: 3M={sr.rs_3m:.0f} → 1M={sr.rs_1m:.0f} → 1W={sr.rs_1w:.0f}",
                })
            elif sr.momentum_direction == "decelerating" and sr.rs_3m > 60:
                rotation_signals.append({
                    "sector": sr.name, "signal": "ROTATING OUT",
                    "detail": f"RS decelerating: 3M={sr.rs_3m:.0f} → 1M={sr.rs_1m:.0f} → 1W={sr.rs_1w:.0f}",
                })

        return {
            "generated_at": datetime.now().isoformat(),
            "total_sectors": len(self.sectors),
            "sectors": {sr.name: sr.to_dict() for sr in sorted_sectors},
            "matrix": matrix,
            "treemaps": {"3m": treemap_3m, "1m": treemap_1m, "1w": treemap_1w},
            "hot_themes": hot_themes,
            "rotation_signals": rotation_signals,
            "top_sectors": [sr.name for sr in sorted_sectors[:5]],
            "bottom_sectors": [sr.name for sr in sorted_sectors[-5:]],
        }


# ═══════════════════════════════════════════════════════════════
# GROQ AI INSIGHTS
# ═══════════════════════════════════════════════════════════════

def generate_ai_insights(sector_data: dict, scanner_data: dict) -> dict:
    """Generate AI-powered market narrative and sector insights using Groq."""

    if not GROQ_API_KEY:
        print("  ⚠ GROQ_API_KEY not set — skipping AI insights")
        return {"narrative": "AI insights unavailable. Set GROQ_API_KEY to enable.", "sector_calls": [], "themes": []}

    # Build context for the AI
    top_sectors = sector_data.get("top_sectors", [])[:5]
    bottom_sectors = sector_data.get("bottom_sectors", [])[:5]
    hot_themes = [t["name"] for t in sector_data.get("hot_themes", []) if t.get("is_hot")]
    rotation = sector_data.get("rotation_signals", [])[:5]
    matrix = sector_data.get("matrix", {})

    # Top catalyst stocks
    all_stocks = scanner_data.get("stocks", [])
    top_catalysts = []
    for s in all_stocks:
        for c in s.get("catalysts", []):
            if c.get("priority", 0) >= 9:
                top_catalysts.append(f"{s['symbol']}: {c['title']}")
    top_catalysts = top_catalysts[:10]

    # Bullish/bearish count
    bullish = sum(1 for s in all_stocks if s.get("composite_score", 0) >= 3)
    bearish = sum(1 for s in all_stocks if s.get("composite_score", 0) <= -3)

    prompt = f"""You are TradEdge AI — a sharp, concise Indian stock market analyst. 
Analyze today's NSE market data and provide actionable insights.

DATA:
- Stocks scanned: {len(all_stocks)}
- Bullish stocks (score≥3): {bullish}
- Bearish stocks (score≤-3): {bearish}
- Top 5 sectors by RS momentum: {', '.join(top_sectors)}
- Bottom 5 sectors: {', '.join(bottom_sectors)}
- Hot themes: {', '.join(hot_themes) or 'None dominant'}
- Leading sectors (RRM): {', '.join(matrix.get('leading', [])[:5])}
- Weakening sectors: {', '.join(matrix.get('weakening', [])[:5])}
- Improving sectors: {', '.join(matrix.get('improving', [])[:5])}
- Lagging sectors: {', '.join(matrix.get('lagging', [])[:5])}
- Rotation signals: {json.dumps(rotation[:3])}
- Top catalysts: {chr(10).join(top_catalysts[:5]) or 'None significant'}

Respond in this exact JSON format only (no markdown, no extra text):
{{
  "market_mood": "bullish/bearish/neutral/cautious",
  "mood_score": 65,
  "narrative": "2-3 sentence market overview for today. Be specific about sectors and catalysts.",
  "sector_calls": [
    {{"sector": "name", "call": "BUY/HOLD/AVOID", "reason": "1 sentence"}},
    {{"sector": "name", "call": "BUY/HOLD/AVOID", "reason": "1 sentence"}}
  ],
  "theme_insight": "1-2 sentences about the dominant theme/rotation happening",
  "risk_flag": "1 sentence about key risk to watch",
  "action_items": ["concise action 1", "concise action 2", "concise action 3"]
}}"""

    try:
        data = json.dumps({
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 800,
        }).encode()

        req = urllib.request.Request(
            "https://api.groq.com/openai/v1/chat/completions",
            data=data,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())

        content = result["choices"][0]["message"]["content"].strip()
        # Clean any markdown fences
        if content.startswith("```"):
            content = content.split("\n", 1)[1]
        if content.endswith("```"):
            content = content.rsplit("```", 1)[0]
        content = content.strip()

        insights = json.loads(content)
        insights["generated_at"] = datetime.now().isoformat()
        insights["model"] = GROQ_MODEL
        return insights

    except Exception as e:
        print(f"  ⚠ Groq AI error: {e}")
        return {
            "market_mood": "unknown",
            "narrative": f"AI insight generation failed: {str(e)[:100]}",
            "sector_calls": [],
            "theme_insight": "",
            "risk_flag": "",
            "action_items": [],
        }


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def run_sector_analysis():
    print("=" * 55)
    print("  Hot Sector / Theme Detection Engine")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # Load scanner data
    if not SCANNER_FILE.exists():
        print("  ❌ scanner_results.json not found. Run generate_data.py first.")
        return

    with open(SCANNER_FILE) as f:
        scanner_data = json.load(f)

    stocks = scanner_data.get("stocks", [])
    print(f"\n  📊 Analyzing {len(stocks)} stocks across sectors...")

    # Run sector analysis
    analyzer = SectorAnalyzer()
    sector_output = analyzer.analyze(stocks)

    print(f"  📁 Found {sector_output['total_sectors']} sectors")
    print(f"\n  🏆 Top sectors: {', '.join(sector_output['top_sectors'])}")
    print(f"  ⬇ Bottom sectors: {', '.join(sector_output['bottom_sectors'])}")

    # RRM Matrix
    matrix = sector_output["matrix"]
    print(f"\n  📊 RRM Sector Matrix:")
    for quad in ["leading", "weakening", "improving", "lagging"]:
        secs = matrix.get(quad, [])[:4]
        print(f"    {quad.upper():12s}: {', '.join(secs) or '—'}")

    # Rotation signals
    if sector_output["rotation_signals"]:
        print(f"\n  🔄 Rotation Signals:")
        for sig in sector_output["rotation_signals"][:5]:
            print(f"    {'🟢' if sig['signal']=='ROTATING IN' else '🔴'} {sig['sector']}: {sig['signal']}")

    # Hot themes
    hot = [t for t in sector_output.get("hot_themes", []) if t.get("is_hot")]
    if hot:
        print(f"\n  🔥 Hot Themes: {', '.join(t['name'] for t in hot)}")

    # Save heatmap data
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(HEATMAP_FILE, "w") as f:
        json.dump(sector_output, f, indent=2)
    print(f"\n  💾 Saved: {HEATMAP_FILE}")

    # Generate AI insights
    print(f"\n  🤖 Generating AI insights via Groq...")
    ai_insights = generate_ai_insights(sector_output, scanner_data)

    analysis_output = {
        "generated_at": datetime.now().isoformat(),
        "sector_data": sector_output,
        "ai_insights": ai_insights,
    }

    with open(ANALYSIS_FILE, "w") as f:
        json.dump(analysis_output, f, indent=2)
    print(f"  💾 Saved: {ANALYSIS_FILE}")

    if ai_insights.get("narrative"):
        print(f"\n  🤖 AI Narrative:")
        print(f"    {ai_insights['narrative'][:200]}...")

    if ai_insights.get("action_items"):
        print(f"\n  📋 Action Items:")
        for item in ai_insights["action_items"][:3]:
            print(f"    • {item}")

    print(f"\n{'=' * 55}")


if __name__ == "__main__":
    run_sector_analysis()

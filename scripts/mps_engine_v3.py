"""
================================================================================
    MPS v3.1.2 — Market Pulse Score Engine
    By Dr. Rahul Ware
    v3.1.2: FII Flow Warning now shows last 5 days history
================================================================================
"""

import json, sys, time
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, List

def print_banner(animate=True):
    C="\033[96m";G="\033[92m";Y="\033[93m";R="\033[91m";M="\033[95m";B="\033[1m";D="\033[2m";X="\033[0m"
    print(f"{C}{B}\n    ╔╦╗╦═╗╔═╗╔╦╗╔═╗╔╦╗╔═╗╔═╗\n     ║ ╠╦╝╠═╣ ║║║╣  ║║║ ╦║╣\n     ╩ ╩╚═╩ ╩═╩╝╚═╝═╩╝╚═╝╚═╝{X}")
    print(f"    {C}{'━'*38}{X}")
    s="✦  T R A D E   T H E   P U L S E  ✦"
    if animate and sys.stdout.isatty():
        sys.stdout.write("    ");cs=[R,Y,G,C,M]
        for i,ch in enumerate(s):sys.stdout.write(f"{B}{cs[i%len(cs)]}{ch}{X}");sys.stdout.flush();time.sleep(0.03)
        print();sys.stdout.write("    ")
        for ch in "By Dr. Rahul Ware":sys.stdout.write(f"{G}{B}{ch}{X}");sys.stdout.flush();time.sleep(0.04)
        print()
    else:
        print(f"    {B}{G}{s}{X}");print(f"    {B}{M}By Dr. Rahul Ware{X}")
    print(f"    {C}{'━'*38}{X}");print(f"    {D}MPS v3.1.2 — Market Pulse Score Engine{X}\n")

@dataclass
class RawMarketData:
    date:str;stocks_above_200sma:int=0;total_universe:int=500;stocks_above_50sma:int=0
    advances:int=0;declines:int=0;unchanged:int=0;stocks_up_4pct:int=0
    burst_gainers_4_5pct:int=0;burst_losers_4_5pct:int=0;new_52w_highs:int=0;new_52w_lows:int=0
    india_vix:float=0.0;pcr:float=0.0;stocks_rsi_above_50:int=0;stocks_atr_pct_above_4:int=0
    stocks_rsi_above_70:int=0;nifty_at_52w_high:bool=False;fii_net_buy_crores:float=0.0
    brent_crude:float=0.0;us10y_yield:float=0.0;usd_inr:float=0.0

@dataclass
class PillarScore:
    name:str;raw_value:float;raw_score:float;weight:float;weighted_score:float
    description:str="";sub_components:dict=field(default_factory=dict)

@dataclass
class ModifierResult:
    name:str;triggered:bool;adjustment:float;reason:str=""

@dataclass
class MPSResult:
    date:str;version:str;pillar_scores:list;base_score:float;modifiers:list
    total_modifier:float;final_score:float;zone:str;zone_emoji:str;zone_action:str
    risk_per_trade:str;state:str;state_message:str;burst_ratio:float;burst_label:str
    rsi_breadth_pct:float;atr_breadth_pct:float;atr_regime:str;macro_summary:str=""

def _interpolate(value,breakpoints):
    if value<=breakpoints[0][0]:return float(breakpoints[0][1])
    if value>=breakpoints[-1][0]:return float(breakpoints[-1][1])
    for i in range(len(breakpoints)-1):
        x0,y0=breakpoints[i];x1,y1=breakpoints[i+1]
        if x0<=value<=x1:return y0+(value-x0)/(x1-x0)*(y1-y0)
    return float(breakpoints[-1][1])

def score_structural(pct):return _interpolate(pct,[(0,0),(30,25),(50,50),(70,75),(85,100)])

def score_breadth_composite(pct_50sma,ad_ratio):
    p=_interpolate(pct_50sma,[(0,0),(25,20),(50,50),(65,75),(80,100)])
    a=_interpolate(ad_ratio,[(0.4,0),(0.7,25),(1.0,50),(1.5,75),(2.0,90),(3.0,100)])
    return 0.6*p+0.4*a,p,a

def score_spark_composite(up4,bg,bl):
    ss=_interpolate(float(up4),[(0,0),(5,15),(15,40),(25,65),(40,85),(60,100)])
    r=(bg/max(bl,1))*100
    bs=100 if r>400 else 75 if r>200 else 50 if r>100 else 25 if r>50 else 0
    return ss*0.6+bs*0.4,ss,bs,r

def score_quality(h,l):return _interpolate(float(h-l),[(-50,0),(-20,20),(0,45),(20,65),(50,80),(100,100)])

def score_sentiment(vix,pcr):
    vs=_interpolate(vix,[(12,100),(14,85),(18,60),(22,35),(28,15),(35,0)])
    ps=_interpolate(pcr,[(0.5,20),(0.7,60),(0.8,80),(1.0,100)] if pcr<=1.0 else [(1.0,100),(1.2,80),(1.5,50),(2.0,15)])
    return 0.5*vs+0.5*ps,vs,ps

def score_momentum(pct):return min(max(pct,0),100)

def score_volatility(pct):
    if pct<10:return 100,"Squeeze"
    elif pct<25:return 75,"Healthy"
    elif pct<35:return 40,"Danger"
    else:return 10,"Panic"

def get_burst_label(r):
    if r>400:return "Super Trend"
    elif r>200:return "Strong Trend"
    elif r>100:return "Moderate"
    elif r>50:return "Weak"
    else:return "Distribution"

# ═══ MODIFIERS ═══

def check_exhaustion_penalty(rsi70,total=500):
    pct=(rsi70/total)*100 if total>0 else 0
    if pct>=95:return ModifierResult("Exhaustion Penalty",True,-15.0,f"{pct:.1f}% stocks with RSI>70 — extreme overheating")
    elif pct>=90:return ModifierResult("Exhaustion Penalty",True,-10.0,f"{pct:.1f}% stocks with RSI>70 — significant overheating")
    elif pct>=80:return ModifierResult("Exhaustion Penalty",True,-5.0,f"{pct:.1f}% stocks with RSI>70 — market stretched")
    else:return ModifierResult("Exhaustion Penalty",False,0.0,f"{pct:.1f}% stocks with RSI>70 — within normal range")

def check_persistence_boost(streak):
    if streak>=63:return ModifierResult("Persistence Boost",True,+7.0,f"{streak} days of structural bull — very stable foundation")
    elif streak>=42:return ModifierResult("Persistence Boost",True,+5.0,f"{streak} days of structural bull — strong stability")
    elif streak>=21:return ModifierResult("Persistence Boost",True,+3.0,f"{streak} days of structural bull — stability bonus active")
    else:return ModifierResult("Persistence Boost",False,0.0,f"{streak} days — streak too short for bonus (need 21+)")

def check_divergence_warning(at_high,pct50,prev50):
    if at_high and pct50<prev50:
        return ModifierResult("Divergence Warning",True,-8.0,f"Zombie Rally: Nifty at 52W high but breadth falling ({prev50:.1f}% → {pct50:.1f}%)")
    return ModifierResult("Divergence Warning",False,0.0,"No divergence detected")

def check_fii_flow_warning(fii_sell_days, fii_5day_net, fii_5day_history=None):
    """
    Modifier 4: FII Flow Warning.
    Now shows last 5 days of FII net values in the description.
    """
    # Build last 5 days display string
    if fii_5day_history and len(fii_5day_history) > 0:
        hist_vals = fii_5day_history[-5:]
        hist_str = " | ".join(f"{v:+,.0f}" for v in hist_vals)
        hist_line = f" [Last {len(hist_vals)}d: {hist_str}]"
    else:
        hist_line = ""

    if fii_sell_days < 5:
        return ModifierResult("FII Flow Warning", False, 0.0,
            f"FII sell streak: {fii_sell_days} days — below threshold (need 5+){hist_line}")

    if fii_sell_days >= 11:
        bp, sev = -8.0, "heavy institutional exit"
    elif fii_sell_days >= 8:
        bp, sev = -5.0, "significant FII pressure"
    else:
        bp, sev = -3.0, "FII caution zone"

    mp, mn = 0.0, ""
    if fii_5day_net < -10000:
        mp = -2.0
        mn = f" + heavy outflow (₹{abs(fii_5day_net):,.0f} Cr in 5 days)"

    return ModifierResult("FII Flow Warning", True, bp + mp,
        f"FII net sellers for {fii_sell_days} consecutive days — {sev}{mn}{hist_line}")

def check_warning_day(pillars):
    mx={"Structural":18,"Breadth":18,"Spark":13,"Quality":13,"Sentiment":13,"Momentum":13,"Volatility":12}
    weak=[p.name for p in pillars if p.weighted_score<mx.get(p.name,10)*0.4]
    if len(weak)>=3:return ModifierResult("Warning Day",True,-10.0,f"{len(weak)} pillars weak (<40%): {', '.join(weak)}")
    return ModifierResult("Warning Day",False,0.0,f"{len(weak)} pillars weak — below threshold (need 3+)")

def check_volatility_regime(atr_pct):
    if atr_pct>50:return ModifierResult("Volatility Regime",True,-15.0,f"ATR Breadth {atr_pct:.1f}% — panic/capitulation territory")
    elif atr_pct>30:return ModifierResult("Volatility Regime",True,-10.0,f"ATR Breadth {atr_pct:.1f}% — erratic, reduce position sizes")
    elif atr_pct<20:return ModifierResult("Volatility Regime",True,+5.0,f"ATR Breadth {atr_pct:.1f}% — calm market, high confidence")
    return ModifierResult("Volatility Regime",False,0.0,f"ATR Breadth {atr_pct:.1f}% — normal volatility")

def check_crude_oil_stress(bp):
    if bp<=0:return ModifierResult("Crude Oil Stress",False,0.0,"Crude data unavailable")
    if bp>110:return ModifierResult("Crude Oil Stress",True,-8.0,f"Brent ${bp:.1f} — emergency level, exit cyclicals")
    elif bp>95:return ModifierResult("Crude Oil Stress",True,-5.0,f"Brent ${bp:.1f} — high stress, imported inflation risk")
    elif bp<65:return ModifierResult("Crude Oil Stress",True,+3.0,f"Brent ${bp:.1f} — tailwind for India, lower input costs")
    elif bp<75:return ModifierResult("Crude Oil Stress",True,+2.0,f"Brent ${bp:.1f} — comfortable zone, margin expansion")
    return ModifierResult("Crude Oil Stress",False,0.0,f"Brent ${bp:.1f} — neutral range ($75-$95)")

def check_global_yield_pressure(y):
    if y<=0:return ModifierResult("Global Yield Pressure",False,0.0,"US 10Y data unavailable")
    if y>5.0:return ModifierResult("Global Yield Pressure",True,-5.0,f"US 10Y {y:.2f}% — extreme EM pressure, FII exodus likely")
    elif y>4.25:return ModifierResult("Global Yield Pressure",True,-3.0,f"US 10Y {y:.2f}% — gravity pulling capital to US")
    elif y<3.5:return ModifierResult("Global Yield Pressure",True,+3.0,f"US 10Y {y:.2f}% — risk-on, FII inflows to EM")
    elif y<3.8:return ModifierResult("Global Yield Pressure",True,+1.0,f"US 10Y {y:.2f}% — mild EM tailwind")
    return ModifierResult("Global Yield Pressure",False,0.0,f"US 10Y {y:.2f}% — neutral range (3.8-4.25%)")

def check_rupee_stress(usd_inr,usd_inr_20d=0.0):
    if usd_inr<=0:return ModifierResult("Rupee Stress",False,0.0,"USD/INR data unavailable")
    dp=((usd_inr-usd_inr_20d)/usd_inr_20d)*100 if usd_inr_20d>0 else 0.0
    if dp>3.0:return ModifierResult("Rupee Stress",True,-5.0,f"INR ₹{usd_inr:.2f} — vertical spike ({dp:+.1f}% in 20d), panic selling likely")
    elif dp>2.0:return ModifierResult("Rupee Stress",True,-3.0,f"INR ₹{usd_inr:.2f} — rapid weakening ({dp:+.1f}% in 20d), FII headwind")
    elif dp<-1.0:return ModifierResult("Rupee Stress",True,+2.0,f"INR ₹{usd_inr:.2f} — appreciating ({dp:+.1f}% in 20d), FII confidence boost")
    elif dp<0:return ModifierResult("Rupee Stress",True,+1.0,f"INR ₹{usd_inr:.2f} — stable/strengthening ({dp:+.1f}% in 20d)")
    return ModifierResult("Rupee Stress",False,0.0,f"INR ₹{usd_inr:.2f} — stable ({dp:+.1f}% in 20d)")

def determine_state(mods,rsi_pct,struct_pct):
    for m in mods:
        if m.name=="Warning Day" and m.triggered:return "WARNING","3+ pillars weak. Stop new entries. Tighten existing stops."
    if rsi_pct>80 and struct_pct>70:return "OVEREXTENDED","Sell into strength. Book partial profits."
    if rsi_pct<20 or struct_pct<15:return "EXHAUSTED","Bottom fishing zone. Watch for reversal candles."
    return "NORMAL","Trend is healthy — trade your plan."

def classify_zone(s):
    if s>=75:return("Easy Money Zone","🟢","Full position sizing. Breakouts have high success rate. Be aggressive on quality setups.","1.5–2% risk per trade")
    elif s>=60:return("Smart Money Zone","🔵","Standard sizing. Market is selective — focus on sector leaders. Don't chase laggards.","1% risk per trade")
    elif s>=40:return("Hard Money Zone","🟡","Selective mean-reversion setups. Backtested edge exists at 0.5% risk. Tight stops, quick exits.","0.5% risk per trade")
    else:return("No Money Zone","🔴","Cash is King. Even good setups will fail due to market drag. Protect capital, wait for conditions to improve.","0% exposure")

def build_macro_summary(mods):
    mm=[m for m in mods if m.name in("Crude Oil Stress","Global Yield Pressure","Rupee Stress")]
    active=[m for m in mm if m.triggered]
    if not active:return "Macro: All neutral — no global headwinds or tailwinds."
    t=sum(m.adjustment for m in active)
    parts=[f"{m.name} ({'+' if m.adjustment>0 else ''}{m.adjustment:.0f})" for m in active]
    return f"Macro {'tailwind' if t>0 else 'headwind'} ({'+' if t>0 else ''}{t:.0f}): {', '.join(parts)}"

# ═══ MAIN ENGINE ═══

def calculate_mps(data, structural_bull_streak_days=0, prev_pct_above_50sma=0.0,
                  fii_net_consecutive_sell_days=0, fii_5day_net_crores=0.0,
                  fii_5day_history=None, usd_inr_20d_ago=0.0):

    p200=(data.stocks_above_200sma/data.total_universe)*100
    p50=(data.stocks_above_50sma/data.total_universe)*100
    rsi_pct=(data.stocks_rsi_above_50/data.total_universe)*100
    atr_pct=(data.stocks_atr_pct_above_4/data.total_universe)*100
    tt=data.advances+data.declines
    ad=data.advances/data.declines if data.declines>0 else 3.0 if tt>0 else 1.0

    pillars=[]
    s1=score_structural(p200);pillars.append(PillarScore("Structural",p200,s1,0.18,s1*0.18,f"{p200:.1f}% of Nifty 500 above 200 SMA"))
    bc,ss,ads=score_breadth_composite(p50,ad);pillars.append(PillarScore("Breadth",p50,bc,0.18,bc*0.18,f"Composite: {p50:.1f}% > 50 SMA (score {ss:.1f}) + A/D {ad:.2f} (score {ads:.1f})",sub_components={"positional_pct":round(p50,1),"positional_score":round(ss,1),"ad_ratio":round(ad,2),"ad_score":round(ads,1),"advances":data.advances,"declines":data.declines}))
    sc,sbs,bs,br=score_spark_composite(data.stocks_up_4pct,data.burst_gainers_4_5pct,data.burst_losers_4_5pct);bl=get_burst_label(br);pillars.append(PillarScore("Spark",float(data.stocks_up_4pct),sc,0.13,sc*0.13,f"Stockbee 4%+: {data.stocks_up_4pct} (score {sbs:.1f}) + Burst {br:.0f} [{bl}] (score {bs:.1f})",sub_components={"stockbee_count":data.stocks_up_4pct,"stockbee_score":round(sbs,1),"burst_gainers":data.burst_gainers_4_5pct,"burst_losers":data.burst_losers_4_5pct,"burst_ratio":round(br,1),"burst_label":bl,"burst_score":round(bs,1)}))
    nnh=data.new_52w_highs-data.new_52w_lows;s4=score_quality(data.new_52w_highs,data.new_52w_lows);pillars.append(PillarScore("Quality",float(nnh),s4,0.13,s4*0.13,f"Net New Highs: {nnh} ({data.new_52w_highs}H - {data.new_52w_lows}L)"))
    s5,vs,ps=score_sentiment(data.india_vix,data.pcr);pillars.append(PillarScore("Sentiment",data.india_vix,s5,0.13,s5*0.13,f"VIX: {data.india_vix:.2f} (score {vs:.1f}), PCR: {data.pcr:.2f} (score {ps:.1f})",sub_components={"vix_score":round(vs,1),"pcr_score":round(ps,1)}))
    s6=score_momentum(rsi_pct);pillars.append(PillarScore("Momentum",rsi_pct,s6,0.13,s6*0.13,f"{rsi_pct:.1f}% of Nifty 500 with RSI(14) > 50"))
    s7,ar=score_volatility(atr_pct);pillars.append(PillarScore("Volatility",atr_pct,s7,0.12,s7*0.12,f"ATR Breadth: {atr_pct:.1f}% of stocks with ATR% > 4% — [{ar}]"))

    base=sum(p.weighted_score for p in pillars)
    mods=[]
    mods.append(check_exhaustion_penalty(data.stocks_rsi_above_70,data.total_universe))
    mods.append(check_persistence_boost(structural_bull_streak_days))
    mods.append(check_divergence_warning(data.nifty_at_52w_high,p50,prev_pct_above_50sma))
    mods.append(check_fii_flow_warning(fii_net_consecutive_sell_days,fii_5day_net_crores,fii_5day_history))
    mods.append(check_warning_day(pillars))
    mods.append(check_volatility_regime(atr_pct))
    mods.append(check_crude_oil_stress(data.brent_crude))
    mods.append(check_global_yield_pressure(data.us10y_yield))
    mods.append(check_rupee_stress(data.usd_inr,usd_inr_20d_ago))

    tm=sum(m.adjustment for m in mods)
    fs=max(0.0,min(100.0,base+tm))
    zn,ze,za,rk=classify_zone(fs)
    st,sm=determine_state(mods,rsi_pct,p200)
    ms=build_macro_summary(mods)

    return MPSResult(date=data.date,version="3.1.2",pillar_scores=[asdict(p) for p in pillars],
        base_score=round(base,2),modifiers=[asdict(m) for m in mods],total_modifier=round(tm,2),
        final_score=round(fs,2),zone=zn,zone_emoji=ze,zone_action=za,risk_per_trade=rk,
        state=st,state_message=sm,burst_ratio=round(br,1),burst_label=bl,
        rsi_breadth_pct=round(rsi_pct,1),atr_breadth_pct=round(atr_pct,1),atr_regime=ar,macro_summary=ms)

def format_mps_report(result):
    lines=[]
    lines.append("="*75)
    lines.append(f"  MPS COMMAND CENTER v{result.version} — {result.date}")
    lines.append(f"  ✦ Trade the Pulse ✦  |  By Dr. Rahul Ware")
    lines.append("="*75);lines.append("")
    lines.append("  PILLAR SCORES (7 Pillars)");lines.append("  "+"─"*65)
    for p in result.pillar_scores:
        bar="█"*int(p['raw_score']/5)+"░"*(20-int(p['raw_score']/5))
        lines.append(f"  {p['name']:<12} {bar} {p['raw_score']:5.1f} × {p['weight']:.0%} = {p['weighted_score']:5.2f}")
        lines.append(f"               {p['description']}")
    lines.append("  "+"─"*65);lines.append(f"  {'Base Score':<12} {'':20s} {result.base_score:>22.2f}");lines.append("")
    lines.append("  SMART MODIFIERS (9 Modifiers)");lines.append("  "+"─"*65)
    for m in result.modifiers:
        s="✅ ACTIVE" if m['triggered'] else "⬜ inactive";a=f"{m['adjustment']:+.1f}" if m['triggered'] else "  0.0"
        lines.append(f"  {m['name']:<22} {s}  {a:>6}");lines.append(f"               {m['reason']}")
    lines.append("  "+"─"*65);lines.append(f"  {'Modifier Total':<22} {result.total_modifier:>36.2f}");lines.append("")
    lines.append(f"  {result.macro_summary}");lines.append("")
    si={"NORMAL":"✦","WARNING":"⚠️","OVEREXTENDED":"🔥","EXHAUSTED":"💀"}
    lines.append(f"  ┌{'─'*63}┐");lines.append(f"  │  STATE: {si.get(result.state,'?')} {result.state:<15} {result.state_message:<32}│")
    lines.append(f"  ├{'─'*63}┤");lines.append(f"  │  FINAL MPS: {result.final_score:6.2f}  {result.zone_emoji} {result.zone:<28s}│")
    lines.append(f"  │  Risk: {result.risk_per_trade:<51s}│")
    bl=f"  Burst: {result.burst_ratio:.0f} [{result.burst_label}]  RSI: {result.rsi_breadth_pct:.1f}%  ATR: {result.atr_breadth_pct:.1f}% [{result.atr_regime}]"
    lines.append(f"  │{bl}{' '*max(0,63-len(bl))}│");lines.append(f"  └{'─'*63}┘")
    lines.append(f"  Action: {result.zone_action}");lines.append("")
    return "\n".join(lines)

def to_json(result):return json.dumps(asdict(result),indent=2,ensure_ascii=False)

if __name__=="__main__":
    print_banner(animate=True)
    d=RawMarketData(date="2026-03-17",stocks_above_200sma=116,stocks_above_50sma=77,advances=305,declines=195,stocks_up_4pct=5,burst_gainers_4_5pct=11,burst_losers_4_5pct=2,new_52w_highs=0,new_52w_lows=75,india_vix=19.79,pcr=1.0,stocks_rsi_above_50=61,stocks_atr_pct_above_4=199,stocks_rsi_above_70=3,fii_net_buy_crores=-9365.52,brent_crude=101.87,us10y_yield=4.20,usd_inr=92.34)
    r=calculate_mps(d,fii_net_consecutive_sell_days=1,fii_5day_net_crores=-9365.52,fii_5day_history=[-9365.52],usd_inr_20d_ago=90.78)
    print(format_mps_report(r))

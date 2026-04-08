"""
CFCAP — Commodity Forward Curve Analytics Platform
====================================================
Complete single-file application combining:
  1. Core engine     : forward curve download, Nelson-Siegel, convenience yield,
                       calendar spreads, matplotlib 4-panel dashboard
  2. Persistence     : CSV snapshots, run history, J-7/J-14 real comparisons
  3. Scheduler       : daily automated batch runs at market open
  4. EIA Fundamentals: US crude/gas inventories, production, spot prices
  5. Streamlit UI    : interactive browser dashboard with Plotly charts

Run modes:
  python  cfcap.py                              # tkinter dialog → matplotlib dashboard
  python  cfcap.py --schedule                   # daily scheduler (keeps process alive)
  python  cfcap.py --commodity "WTI Crude Oil" --family "Energy"
  python  cfcap.py --list                       # list saved CSV snapshots
  streamlit run cfcap.py                        # browser dashboard (Streamlit)

Install:
  pip install yfinance pandas numpy scipy matplotlib streamlit plotly requests schedule
  pip install git+https://github.com/StreamAlpha/tvdatafeed.git  # for TV-only contracts

Free EIA API key: https://www.eia.gov/opendata/  (30 seconds to register)
TradingView credentials: set TV_USERNAME / TV_PASSWORD below (optional)

™ by AEG
"""

# -*- coding: utf-8 -*-
# ══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

import argparse
import json
import os
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from scipy.interpolate import CubicSpline

import requests
import tkinter as tk
from tkinter import ttk, messagebox

warnings.filterwarnings("ignore")

# Force UTF-8 output on Windows (avoids CP1252 UnicodeEncodeError)
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
#  GLOBAL CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

TV_USERNAME = ""    # TradingView username (optional — anonymous session if empty)
TV_PASSWORD = ""    # TradingView password
EIA_API_KEY = ""    # EIA API key — get free at https://www.eia.gov/opendata/

MONTH_CODES = list("FGHJKMNQUVXZ")
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]

# ── Storage directories ────────────────────────────────────────────────────────
# Anchor all paths to the directory where cfcap.py lives,
# regardless of where Python/Streamlit is launched from.
_BASE_DIR      = Path(__file__).resolve().parent
DATA_DIR       = _BASE_DIR / "data"
CURVES_DIR     = DATA_DIR / "curves"
DASHBOARDS_DIR = DATA_DIR / "dashboards"
LOGS_DIR       = DATA_DIR / "logs"
EIA_CACHE_DIR  = DATA_DIR / "eia_cache"

for _d in [CURVES_DIR, DASHBOARDS_DIR, LOGS_DIR, EIA_CACHE_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

COMMODITY_REGISTRY = {

    # ── Energy ─────────────────────────────────────────────────────────────────
    # Yahoo returns: $/bbl, $/MMBtu, $/gallon, $/mt (as specified per contract)
    "Energy": {

        "WTI Crude Oil": dict(
            name="WTI Crude Oil", unit="$/bbl",
            source="yahoo", yf_fmt="CL{M}{YY}.NYM",
            active_months="FGHJKMNQUVXZ", liquid_months=18,
            storage_cost=0.096, synthetic_spot=70.0,
            ns_bounds=([10, -150, -150, 0.5], [300, 150, 150, 60]),
        ),
        "Brent Crude Oil": dict(
            name="Brent Crude Oil", unit="$/bbl",
            source="yahoo", yf_fmt="BZ{M}{YY}.NYB",
            active_months="FGHJKMNQUVXZ", liquid_months=18,
            storage_cost=0.096, synthetic_spot=74.0,
            ns_bounds=([10, -150, -150, 0.5], [300, 150, 150, 60]),
        ),
        "Natural Gas (Henry Hub)": dict(
            name="Natural Gas (Henry Hub)", unit="$/MMBtu",
            source="yahoo", yf_fmt="NG{M}{YY}.NYM",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.120, synthetic_spot=3.5,
            ns_bounds=([0.1, -20, -20, 0.5], [30, 20, 20, 60]),
        ),
        "RBOB Gasoline": dict(
            # Yahoo returns $/gallon (NOT ¢/gallon). Current ~2.2-2.8 $/gal.
            name="RBOB Gasoline", unit="$/gallon",
            source="yahoo", yf_fmt="RB{M}{YY}.NYM",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.084, synthetic_spot=2.5,
            ns_bounds=([0.3, -5, -5, 0.5], [15, 5, 5, 60]),
        ),
        "Heating Oil (ULSD)": dict(
            # Yahoo returns $/gallon. Current ~2.5-3.0 $/gal.
            name="Heating Oil (ULSD)", unit="$/gallon",
            source="yahoo", yf_fmt="HO{M}{YY}.NYM",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.084, synthetic_spot=2.7,
            ns_bounds=([0.3, -5, -5, 0.5], [15, 5, 5, 60]),
        ),
        "Gasoil ICE": dict(
            # Yahoo returns $/mt. ICE London. Current ~700-900 $/mt.
            name="Gasoil ICE", unit="$/mt",
            source="yahoo", yf_fmt="LGO{M}{YY}.NYB",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.072, synthetic_spot=780.0,
            ns_bounds=([100, -800, -800, 0.5], [3000, 800, 800, 60]),
        ),
        "Jet Kerosene CIF NWE (Platts)": dict(
            # TradingView returns $/mt. Current ~800-1000 $/mt.
            name="Jet Kerosene CIF NWE (Platts)", unit="$/mt",
            source="tradingview", tv_prefix="AUJ", tv_exchange="NYMEX",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.072, synthetic_spot=850.0,
            ns_bounds=([100, -800, -800, 0.5], [3000, 800, 800, 60]),
        ),
    },

    # ── Metals ─────────────────────────────────────────────────────────────────
    # Yahoo normalises all metals to $/troy oz or $/lb (NOT cents).
    "Metals": {

        "Gold": dict(
            # Yahoo returns $/troy oz. Current ~3000-3500 $/oz.
            name="Gold", unit="$/troy oz",
            source="yahoo", yf_fmt="GC{M}{YY}.CMX",
            active_months="GJMQVZ", liquid_months=8,
            storage_cost=0.024, synthetic_spot=3200.0,
            ns_bounds=([500, -2000, -2000, 0.5], [8000, 2000, 2000, 60]),
        ),
        "Silver": dict(
            # Yahoo returns $/troy oz. Current ~28-35 $/oz.
            name="Silver", unit="$/troy oz",
            source="yahoo", yf_fmt="SI{M}{YY}.CMX",
            active_months="HKNUZ", liquid_months=6,
            storage_cost=0.036, synthetic_spot=32.0,
            ns_bounds=([3, -100, -100, 0.5], [500, 100, 100, 60]),
        ),
        "Copper": dict(
            # Yahoo returns $/lb (CME quotes in ¢/lb but Yahoo normalises).
            # Current ~4.0-5.5 $/lb. NOT ~100 $/lb — that was synthetic artefact.
            name="Copper", unit="$/lb",
            source="yahoo", yf_fmt="HG{M}{YY}.CMX",
            active_months="HKNUZ", liquid_months=8,
            storage_cost=0.048, synthetic_spot=4.8,
            ns_bounds=([0.5, -10, -10, 0.5], [20, 10, 10, 60]),
        ),
        "Platinum": dict(
            # Yahoo returns $/troy oz. Current ~950-1100 $/oz.
            name="Platinum", unit="$/troy oz",
            source="yahoo", yf_fmt="PL{M}{YY}.NYM",
            active_months="FJNV", liquid_months=6,
            storage_cost=0.030, synthetic_spot=1000.0,
            ns_bounds=([100, -1500, -1500, 0.5], [5000, 1500, 1500, 60]),
        ),
        "Palladium": dict(
            # Yahoo returns $/troy oz. Current ~1000-2500 $/oz.
            name="Palladium", unit="$/troy oz",
            source="yahoo", yf_fmt="PA{M}{YY}.NYM",
            active_months="HMUZ", liquid_months=6,
            storage_cost=0.030, synthetic_spot=1100.0,
            ns_bounds=([100, -3000, -3000, 0.5], [8000, 3000, 3000, 60]),
        ),
    },

    # ── Agriculture ────────────────────────────────────────────────────────────
    # Yahoo returns cents (¢) for grains and softs, $/mt for Cocoa.
    "Agriculture": {

        "Corn": dict(
            # Yahoo returns ¢/bushel. Current ~420-480 ¢/bu.
            name="Corn", unit="¢/bushel",
            source="yahoo", yf_fmt="ZC{M}{YY}.CBT",
            active_months="HKNUZ", liquid_months=8,
            storage_cost=0.060, synthetic_spot=450.0,
            ns_bounds=([100, -600, -600, 0.5], [2000, 600, 600, 60]),
        ),
        "Wheat (CBOT)": dict(
            # Yahoo returns ¢/bushel. Current ~520-620 ¢/bu.
            name="Wheat (CBOT)", unit="¢/bushel",
            source="yahoo", yf_fmt="ZW{M}{YY}.CBT",
            active_months="HKNUZ", liquid_months=8,
            storage_cost=0.060, synthetic_spot=560.0,
            ns_bounds=([100, -600, -600, 0.5], [3000, 600, 600, 60]),
        ),
        "Soybeans": dict(
            # Yahoo returns ¢/bushel. Current ~980-1050 ¢/bu.
            name="Soybeans", unit="¢/bushel",
            source="yahoo", yf_fmt="ZS{M}{YY}.CBT",
            active_months="FHKNQUX", liquid_months=8,
            storage_cost=0.060, synthetic_spot=1000.0,
            ns_bounds=([300, -800, -800, 0.5], [3000, 800, 800, 60]),
        ),
        "Sugar #11": dict(
            # Yahoo returns ¢/lb. Current ~18-22 ¢/lb.
            name="Sugar #11", unit="¢/lb",
            source="yahoo", yf_fmt="SB{M}{YY}.NYB",
            active_months="HKNV", liquid_months=6,
            storage_cost=0.048, synthetic_spot=19.0,
            ns_bounds=([3, -30, -30, 0.5], [100, 30, 30, 60]),
        ),
        "Coffee (Arabica)": dict(
            # Yahoo returns ¢/lb. Current ~320-400 ¢/lb.
            name="Coffee (Arabica)", unit="¢/lb",
            source="yahoo", yf_fmt="KC{M}{YY}.NYB",
            active_months="HKNUZ", liquid_months=6,
            storage_cost=0.048, synthetic_spot=350.0,
            ns_bounds=([30, -500, -500, 0.5], [2000, 500, 500, 60]),
        ),
        "Cocoa": dict(
            # Yahoo returns $/mt. Current ~7000-9000 $/mt.
            name="Cocoa", unit="$/mt",
            source="yahoo", yf_fmt="CC{M}{YY}.NYB",
            active_months="HKNUZ", liquid_months=6,
            storage_cost=0.048, synthetic_spot=8000.0,
            ns_bounds=([500, -8000, -8000, 0.5], [20000, 8000, 8000, 60]),
        ),
        "Cotton #2": dict(
            name="Cotton #2", unit="¢/lb",
            source="yahoo", yf_fmt="CT{M}{YY}.NYB",
            active_months="HKNVZ", liquid_months=6,
            storage_cost=0.060, synthetic_spot=72.0,
            ns_bounds=([10, -150, -150, 0.5], [300, 150, 150, 60]),
        ),
        # ── Grains (additional) ──────────────────────────────────────
        "Soybean Oil": dict(
            name="Soybean Oil", unit="¢/lb",
            source="yahoo", yf_fmt="ZL{M}{YY}.CBT",
            active_months="FHKNQUVZ", liquid_months=8,
            storage_cost=0.060, synthetic_spot=45.0,
            ns_bounds=([5, -60, -60, 0.5], [200, 60, 60, 60]),
        ),
        "Soybean Meal": dict(
            name="Soybean Meal", unit="$/short ton",
            source="yahoo", yf_fmt="ZM{M}{YY}.CBT",
            active_months="FHKNQUVZ", liquid_months=8,
            storage_cost=0.060, synthetic_spot=310.0,
            ns_bounds=([50, -400, -400, 0.5], [1500, 400, 400, 60]),
        ),
        "Oats": dict(
            name="Oats", unit="¢/bushel",
            source="yahoo", yf_fmt="ZO{M}{YY}.CBT",
            active_months="HKNUZ", liquid_months=6,
            storage_cost=0.060, synthetic_spot=360.0,
            ns_bounds=([50, -500, -500, 0.5], [1500, 500, 500, 60]),
        ),
        "Rough Rice": dict(
            name="Rough Rice", unit="¢/cwt",
            source="yahoo", yf_fmt="ZR{M}{YY}.CBT",
            active_months="FHKNUX", liquid_months=6,
            storage_cost=0.060, synthetic_spot=1500.0,
            ns_bounds=([300, -1000, -1000, 0.5], [5000, 1000, 1000, 60]),
        ),
        "Wheat (KC HRW)": dict(
            name="Wheat (KC HRW)", unit="¢/bushel",
            source="yahoo", yf_fmt="KE{M}{YY}.CBT",
            active_months="HKNUZ", liquid_months=8,
            storage_cost=0.060, synthetic_spot=580.0,
            ns_bounds=([100, -600, -600, 0.5], [3000, 600, 600, 60]),
        ),
        "Wheat (Spring MGEX)": dict(
            name="Wheat (Spring MGEX)", unit="¢/bushel",
            source="yahoo", yf_fmt="MWE{M}{YY}.MGE",
            active_months="HKNUZ", liquid_months=6,
            storage_cost=0.060, synthetic_spot=620.0,
            ns_bounds=([100, -600, -600, 0.5], [3000, 600, 600, 60]),
        ),
        # ── Softs (additional) ───────────────────────────────────────
        "Orange Juice": dict(
            name="Orange Juice", unit="¢/lb",
            source="yahoo", yf_fmt="OJ{M}{YY}.NYB",
            active_months="FHKNUX", liquid_months=6,
            storage_cost=0.048, synthetic_spot=250.0,
            ns_bounds=([20, -300, -300, 0.5], [800, 300, 300, 60]),
        ),
        "Robusta Coffee": dict(
            name="Robusta Coffee", unit="$/mt",
            source="tradingview", tv_prefix="RC", tv_exchange="ICE",
            active_months="FHKNUX", liquid_months=6,
            storage_cost=0.048, synthetic_spot=3800.0,
            ns_bounds=([500, -3000, -3000, 0.5], [12000, 3000, 3000, 60]),
        ),
        "White Sugar #5": dict(
            name="White Sugar #5", unit="$/mt",
            source="tradingview", tv_prefix="QW", tv_exchange="ICE",
            active_months="HKNVZ", liquid_months=6,
            storage_cost=0.048, synthetic_spot=530.0,
            ns_bounds=([100, -400, -400, 0.5], [2000, 400, 400, 60]),
        ),
        # ── Livestock ────────────────────────────────────────────────
        "Live Cattle": dict(
            name="Live Cattle", unit="¢/lb",
            source="yahoo", yf_fmt="LE{M}{YY}.CME",
            active_months="GJMQVZ", liquid_months=8,
            storage_cost=0.036, synthetic_spot=185.0,
            ns_bounds=([50, -100, -100, 0.5], [400, 100, 100, 60]),
        ),
        "Lean Hogs": dict(
            name="Lean Hogs", unit="¢/lb",
            source="yahoo", yf_fmt="HE{M}{YY}.CME",
            active_months="GJKMNQVZ", liquid_months=6,
            storage_cost=0.036, synthetic_spot=92.0,
            ns_bounds=([20, -80, -80, 0.5], [250, 80, 80, 60]),
        ),
        "Feeder Cattle": dict(
            name="Feeder Cattle", unit="¢/lb",
            source="yahoo", yf_fmt="GF{M}{YY}.CME",
            active_months="FHJKQUV", liquid_months=6,
            storage_cost=0.036, synthetic_spot=255.0,
            ns_bounds=([50, -120, -120, 0.5], [500, 120, 120, 60]),
        ),
        # ── Dairy ────────────────────────────────────────────────────
        "Milk (Class III)": dict(
            name="Milk (Class III)", unit="$/cwt",
            source="yahoo", yf_fmt="DC{M}{YY}.CME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.036, synthetic_spot=18.0,
            ns_bounds=([3, -20, -20, 0.5], [60, 20, 20, 60]),
        ),
        "Butter": dict(
            name="Butter", unit="¢/lb",
            source="yahoo", yf_fmt="CB{M}{YY}.CME",
            active_months="FGHJKMNQUVXZ", liquid_months=6,
            storage_cost=0.048, synthetic_spot=230.0,
            ns_bounds=([50, -150, -150, 0.5], [600, 150, 150, 60]),
        ),
        # ── Timber ───────────────────────────────────────────────────
        "Lumber": dict(
            name="Lumber", unit="$/mbf",
            source="yahoo", yf_fmt="LBS{M}{YY}.CME",
            active_months="FHKNUX", liquid_months=6,
            storage_cost=0.060, synthetic_spot=520.0,
            ns_bounds=([50, -500, -500, 0.5], [2000, 500, 500, 60]),
        ),
        # ── Tropical (TradingView) ────────────────────────────────────
        "Palm Oil (Bursa)": dict(
            name="Palm Oil (Bursa)", unit="MYR/mt",
            source="tradingview", tv_prefix="FCPO", tv_exchange="BMDB",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.060, synthetic_spot=4200.0,
            ns_bounds=([500, -3000, -3000, 0.5], [15000, 3000, 3000, 60]),
        ),
        "Canola (ICE Canada)": dict(
            name="Canola (ICE Canada)", unit="CAD/mt",
            source="tradingview", tv_prefix="RS", tv_exchange="ICE",
            active_months="FHKNX", liquid_months=6,
            storage_cost=0.060, synthetic_spot=620.0,
            ns_bounds=([100, -500, -500, 0.5], [2500, 500, 500, 60]),
        ),
    },

    # ── Metals (LME & additional) ─────────────────────────────────────────────
    "Base Metals": {
        "LME Copper": dict(
            name="LME Copper", unit="$/mt",
            source="tradingview", tv_prefix="COPPER", tv_exchange="LME",
            active_months="FGHJKMNQUVXZ", liquid_months=15,
            storage_cost=0.048, synthetic_spot=9800.0,
            ns_bounds=([2000, -5000, -5000, 0.5], [25000, 5000, 5000, 60]),
        ),
        "LME Aluminum": dict(
            name="LME Aluminum", unit="$/mt",
            source="tradingview", tv_prefix="ALUMINUM", tv_exchange="LME",
            active_months="FGHJKMNQUVXZ", liquid_months=15,
            storage_cost=0.048, synthetic_spot=2400.0,
            ns_bounds=([500, -2000, -2000, 0.5], [8000, 2000, 2000, 60]),
        ),
        "LME Zinc": dict(
            name="LME Zinc", unit="$/mt",
            source="tradingview", tv_prefix="ZINC", tv_exchange="LME",
            active_months="FGHJKMNQUVXZ", liquid_months=15,
            storage_cost=0.048, synthetic_spot=2800.0,
            ns_bounds=([500, -2000, -2000, 0.5], [8000, 2000, 2000, 60]),
        ),
        "LME Nickel": dict(
            name="LME Nickel", unit="$/mt",
            source="tradingview", tv_prefix="NICKEL", tv_exchange="LME",
            active_months="FGHJKMNQUVXZ", liquid_months=15,
            storage_cost=0.048, synthetic_spot=16000.0,
            ns_bounds=([3000, -15000, -15000, 0.5], [60000, 15000, 15000, 60]),
        ),
        "LME Lead": dict(
            name="LME Lead", unit="$/mt",
            source="tradingview", tv_prefix="LEAD", tv_exchange="LME",
            active_months="FGHJKMNQUVXZ", liquid_months=15,
            storage_cost=0.048, synthetic_spot=2000.0,
            ns_bounds=([300, -1500, -1500, 0.5], [6000, 1500, 1500, 60]),
        ),
        "LME Tin": dict(
            name="LME Tin", unit="$/mt",
            source="tradingview", tv_prefix="TIN", tv_exchange="LME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.048, synthetic_spot=32000.0,
            ns_bounds=([5000, -20000, -20000, 0.5], [100000, 20000, 20000, 60]),
        ),
        "LME Cobalt": dict(
            name="LME Cobalt", unit="$/mt",
            source="tradingview", tv_prefix="COBALT", tv_exchange="LME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.036, synthetic_spot=28000.0,
            ns_bounds=([5000, -20000, -20000, 0.5], [100000, 20000, 20000, 60]),
        ),
        "Steel HRC (USA)": dict(
            name="Steel HRC (USA)", unit="$/short ton",
            source="tradingview", tv_prefix="HRC1", tv_exchange="CME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.060, synthetic_spot=760.0,
            ns_bounds=([100, -600, -600, 0.5], [3000, 600, 600, 60]),
        ),
        "Iron Ore (CME 62%)": dict(
            name="Iron Ore (CME 62%)", unit="$/mt",
            source="tradingview", tv_prefix="TIO1", tv_exchange="CME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.060, synthetic_spot=105.0,
            ns_bounds=([20, -100, -100, 0.5], [350, 100, 100, 60]),
        ),
        "Aluminum (COMEX)": dict(
            name="Aluminum (COMEX)", unit="$/mt",
            source="yahoo", yf_fmt="ALI{M}{YY}.CMX",
            active_months="HKNUZ", liquid_months=6,
            storage_cost=0.048, synthetic_spot=2400.0,
            ns_bounds=([500, -2000, -2000, 0.5], [8000, 2000, 2000, 60]),
        ),
        "Lithium Carbonate": dict(
            name="Lithium Carbonate", unit="$/mt",
            source="tradingview", tv_prefix="LC1", tv_exchange="CME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.036, synthetic_spot=9500.0,
            ns_bounds=([1000, -15000, -15000, 0.5], [80000, 15000, 15000, 60]),
        ),
    },

    # ── Energy (additional) ───────────────────────────────────────────────────
    "Energy (Additional)": {
        "Dutch TTF Natural Gas": dict(
            name="Dutch TTF Natural Gas", unit="€/MWh",
            source="tradingview", tv_prefix="TTFG", tv_exchange="ICEENDEX",
            active_months="FGHJKMNQUVXZ", liquid_months=24,
            storage_cost=0.120, synthetic_spot=38.0,
            ns_bounds=([2, -60, -60, 0.5], [200, 60, 60, 60]),
        ),
        "UK NBP Natural Gas": dict(
            name="UK NBP Natural Gas", unit="p/therm",
            source="tradingview", tv_prefix="NBPG", tv_exchange="ICE",
            active_months="FGHJKMNQUVXZ", liquid_months=18,
            storage_cost=0.120, synthetic_spot=90.0,
            ns_bounds=([10, -100, -100, 0.5], [500, 100, 100, 60]),
        ),
        "Coal (API2 Rotterdam)": dict(
            name="Coal (API2 Rotterdam)", unit="$/mt",
            source="tradingview", tv_prefix="MTF", tv_exchange="ICE",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.048, synthetic_spot=110.0,
            ns_bounds=([20, -150, -150, 0.5], [500, 150, 150, 60]),
        ),
        "Coal (Newcastle API4)": dict(
            name="Coal (Newcastle API4)", unit="$/mt",
            source="tradingview", tv_prefix="NCF", tv_exchange="ICE",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.048, synthetic_spot=130.0,
            ns_bounds=([20, -150, -150, 0.5], [600, 150, 150, 60]),
        ),
        "Uranium (UxC)": dict(
            name="Uranium (UxC)", unit="$/lb U3O8",
            source="tradingview", tv_prefix="UX1", tv_exchange="CME",
            active_months="HKNUZ", liquid_months=8,
            storage_cost=0.024, synthetic_spot=78.0,
            ns_bounds=([10, -80, -80, 0.5], [300, 80, 80, 60]),
        ),
        "European Carbon (EUA)": dict(
            name="European Carbon (EUA)", unit="€/tCO2",
            source="tradingview", tv_prefix="EUAD", tv_exchange="ICE",
            active_months="HMNUZ", liquid_months=12,
            storage_cost=0.024, synthetic_spot=65.0,
            ns_bounds=([5, -80, -80, 0.5], [200, 80, 80, 60]),
        ),
        "Singapore Fuel Oil 380cst": dict(
            name="Singapore Fuel Oil 380cst", unit="$/mt",
            source="tradingview", tv_prefix="AY", tv_exchange="NYMEX",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.072, synthetic_spot=420.0,
            ns_bounds=([50, -400, -400, 0.5], [1500, 400, 400, 60]),
        ),
        "Naphtha CIF NWE (Platts)": dict(
            name="Naphtha CIF NWE (Platts)", unit="$/mt",
            source="tradingview", tv_prefix="UN", tv_exchange="NYMEX",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.072, synthetic_spot=650.0,
            ns_bounds=([50, -600, -600, 0.5], [2000, 600, 600, 60]),
        ),
        "Propane (Mont Belvieu)": dict(
            name="Propane (Mont Belvieu)", unit="¢/gallon",
            source="yahoo", yf_fmt="PN{M}{YY}.NYM",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.072, synthetic_spot=68.0,
            ns_bounds=([5, -60, -60, 0.5], [200, 60, 60, 60]),
        ),
        "Ethanol (CBOT)": dict(
            name="Ethanol (CBOT)", unit="$/gallon",
            source="yahoo", yf_fmt="EH{M}{YY}.CBT",
            active_months="FHKNUX", liquid_months=6,
            storage_cost=0.072, synthetic_spot=1.65,
            ns_bounds=([0.3, -2, -2, 0.5], [6, 2, 2, 60]),
        ),
    },

    # ── Freight ───────────────────────────────────────────────────────────────
    "Freight": {
        "Capesize (BCI 5TC)": dict(
            name="Capesize (BCI 5TC)", unit="$/day",
            source="tradingview", tv_prefix="BCSA1", tv_exchange="CME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.0, synthetic_spot=18000.0,
            ns_bounds=([1000, -20000, -20000, 0.5], [80000, 20000, 20000, 60]),
        ),
        "Panamax (BPI 4TC)": dict(
            name="Panamax (BPI 4TC)", unit="$/day",
            source="tradingview", tv_prefix="BPSA1", tv_exchange="CME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.0, synthetic_spot=12000.0,
            ns_bounds=([500, -12000, -12000, 0.5], [50000, 12000, 12000, 60]),
        ),
        "Supramax (BSI 10TC)": dict(
            name="Supramax (BSI 10TC)", unit="$/day",
            source="tradingview", tv_prefix="BSSA1", tv_exchange="CME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.0, synthetic_spot=10000.0,
            ns_bounds=([500, -10000, -10000, 0.5], [40000, 10000, 10000, 60]),
        ),
        "VLCC Tanker (TD3C)": dict(
            name="VLCC Tanker (TD3C)", unit="WS points",
            source="tradingview", tv_prefix="BDTA1", tv_exchange="CME",
            active_months="FGHJKMNQUVXZ", liquid_months=12,
            storage_cost=0.0, synthetic_spot=55.0,
            ns_bounds=([5, -80, -80, 0.5], [250, 80, 80, 60]),
        ),
    },

    # ── Carbon & Environmental ────────────────────────────────────────────────
    "Carbon & Environmental": {
        "EU Carbon EUA": dict(
            name="EU Carbon EUA", unit="€/tCO2",
            source="tradingview", tv_prefix="EUAD", tv_exchange="ICE",
            active_months="HMNUZ", liquid_months=12,
            storage_cost=0.024, synthetic_spot=65.0,
            ns_bounds=([5, -80, -80, 0.5], [200, 80, 80, 60]),
        ),
        "UK Carbon UKA": dict(
            name="UK Carbon UKA", unit="£/tCO2",
            source="tradingview", tv_prefix="UKAD", tv_exchange="ICE",
            active_months="HMNUZ", liquid_months=12,
            storage_cost=0.024, synthetic_spot=40.0,
            ns_bounds=([3, -50, -50, 0.5], [150, 50, 50, 60]),
        ),
        "California Carbon CCA": dict(
            name="California Carbon CCA", unit="$/tCO2",
            source="tradingview", tv_prefix="CCA1", tv_exchange="CME",
            active_months="HMNUZ", liquid_months=12,
            storage_cost=0.024, synthetic_spot=30.0,
            ns_bounds=([3, -40, -40, 0.5], [120, 40, 40, 60]),
        ),
        "RGGI Carbon (NE USA)": dict(
            name="RGGI Carbon (NE USA)", unit="$/tCO2",
            source="tradingview", tv_prefix="RGGI1", tv_exchange="CME",
            active_months="HMUZ", liquid_months=8,
            storage_cost=0.024, synthetic_spot=14.0,
            ns_bounds=([1, -20, -20, 0.5], [60, 20, 20, 60]),
        ),
    },
}

# ══════════════════════════════════════════════════════════════════════════════
#  PART 1 — SELECTION DIALOG
# ══════════════════════════════════════════════════════════════════════════════

def run_selector() -> dict:
    """
    Premium tkinter dialog: family → commodity → parameters → run.
    Returns config dict or exits if cancelled.
    """
    result = {}

    root = tk.Tk()
    root.title("Commodity Forward Curve Analyzer  ™ by AEG")
    root.geometry("700x660")
    root.resizable(False, False)
    root.configure(bg="#0D1117")

    # ── Palette ───────────────────────────────────────────────────────────
    BG       = "#0D1117"
    PANEL    = "#161B22"
    PANEL2   = "#1C2128"
    BORDER   = "#30363D"
    BORDER2  = "#21262D"
    TEXT     = "#E6EDF3"
    MUTED    = "#8B949E"
    ACCENT   = "#58A6FF"
    AMBER    = "#F0A500"
    GREEN    = "#238636"
    GREEN2   = "#2EA043"
    RED      = "#DA3633"
    PURPLE   = "#BC8CFF"

    FONT      = ("Consolas", 10)
    FONT_B    = ("Consolas", 10, "bold")
    FONT_SM   = ("Consolas", 9)
    FONT_XS   = ("Consolas", 8)
    FONT_T    = ("Consolas", 15, "bold")
    FONT_TM   = ("Consolas", 9, "italic")

    # ── Helpers ───────────────────────────────────────────────────────────
    def lbl(parent, text, fg=MUTED, font=FONT_SM, bg=BG, **kw):
        return tk.Label(parent, text=text, bg=bg, fg=fg, font=font, **kw)

    def sep_line(parent):
        tk.Frame(parent, bg=BORDER, height=1).pack(fill="x", padx=20, pady=(0, 12))

    def section_header(parent, num, title):
        f = tk.Frame(parent, bg=BG)
        f.pack(fill="x", padx=20, pady=(10, 6))
        tk.Label(f, text=f" {num} ", bg=ACCENT, fg=BG,
                 font=("Consolas", 9, "bold"),
                 padx=5, pady=1).pack(side="left")
        tk.Label(f, text=f"  {title}", bg=BG, fg=TEXT,
                 font=FONT_B).pack(side="left")

    def mk_entry(parent, default="", width=10, show=None):
        kw = dict(bg=PANEL2, fg=TEXT, insertbackground=ACCENT,
                  font=FONT, relief="flat", bd=0,
                  highlightthickness=1, highlightbackground=BORDER,
                  highlightcolor=ACCENT, width=width)
        if show:
            kw["show"] = show
        e = tk.Entry(parent, **kw)
        e.insert(0, default)
        return e

    # ── Header ────────────────────────────────────────────────────────────
    header = tk.Frame(root, bg=PANEL, pady=0)
    header.pack(fill="x")

    # left accent bar
    tk.Frame(header, bg=ACCENT, width=4).pack(side="left", fill="y")

    title_block = tk.Frame(header, bg=PANEL, padx=18, pady=14)
    title_block.pack(side="left", fill="both", expand=True)

    title_row = tk.Frame(title_block, bg=PANEL)
    title_row.pack(anchor="w")
    tk.Label(title_row, text="FORWARD CURVE ANALYZER",
             bg=PANEL, fg=TEXT, font=FONT_T).pack(side="left")
    tk.Label(title_row, text="  ™ by AEG",
             bg=PANEL, fg=MUTED, font=FONT_TM).pack(side="left", pady=(4, 0))

    tk.Label(title_block,
             text="Multi-commodity forward curve analysis  |  Hedging & Trading toolkit",
             bg=PANEL, fg=MUTED, font=FONT_SM).pack(anchor="w", pady=(3, 0))

    # right: live clock
    clock_lbl = tk.Label(header, bg=PANEL, fg=MUTED, font=FONT_SM, padx=18)
    clock_lbl.pack(side="right", anchor="e")

    def tick():
        clock_lbl.config(text=datetime.now().strftime("%d %b %Y   %H:%M:%S"))
        root.after(1000, tick)
    tick()

    tk.Frame(root, bg=BORDER, height=1).pack(fill="x")

    # ── Scrollable body ───────────────────────────────────────────────────
    body = tk.Frame(root, bg=BG)
    body.pack(fill="both", expand=True)

    # ── Section 1: Asset class ────────────────────────────────────────────
    section_header(body, "1", "ASSET CLASS")

    families   = list(COMMODITY_REGISTRY.keys())
    family_var = tk.StringVar(value=families[0])

    fam_frame = tk.Frame(body, bg=BG)
    fam_frame.pack(fill="x", padx=20, pady=(0, 2))

    FAMILY_COLORS = {"Energy": AMBER, "Metals": "#C0C0C0", "Agriculture": GREEN2}

    radio_btns = {}
    for fam in families:
        col = FAMILY_COLORS.get(fam, ACCENT)
        rb = tk.Radiobutton(
            fam_frame, text=f"  {fam}  ",
            variable=family_var, value=fam,
            bg=PANEL2, fg=col,
            selectcolor=PANEL, activebackground=PANEL2, activeforeground=col,
            font=FONT_B, relief="flat", bd=0,
            indicatoron=0,
            padx=12, pady=6,
            highlightthickness=1,
            highlightbackground=BORDER,
            command=lambda: update_commodities(),
        )
        rb.pack(side="left", padx=(0, 8))
        radio_btns[fam] = rb

    sep_line(body)

    # ── Section 2: Commodity ──────────────────────────────────────────────
    section_header(body, "2", "COMMODITY")

    cb_frame = tk.Frame(body, bg=BG)
    cb_frame.pack(fill="x", padx=20, pady=(0, 6))

    commodity_var = tk.StringVar()

    style = ttk.Style()
    style.theme_use("clam")
    style.configure("Dark.TCombobox",
                    fieldbackground=PANEL2, background=PANEL2,
                    foreground=TEXT, arrowcolor=ACCENT,
                    selectbackground=PANEL2, selectforeground=TEXT,
                    bordercolor=BORDER, lightcolor=BORDER, darkcolor=BORDER)
    style.map("Dark.TCombobox",
              fieldbackground=[("readonly", PANEL2)],
              background=[("readonly", PANEL2)],
              foreground=[("readonly", TEXT)])

    commodity_cb = ttk.Combobox(cb_frame, textvariable=commodity_var,
                                 state="readonly", font=FONT, width=46,
                                 style="Dark.TCombobox")
    commodity_cb.pack(side="left")

    # Info badge row
    info_frame = tk.Frame(body, bg=BG)
    info_frame.pack(fill="x", padx=20, pady=(4, 4))

    source_badge = tk.Label(info_frame, text="", bg=BG, fg=GREEN2, font=FONT_XS,
                             padx=8, pady=3, relief="flat")
    source_badge.pack(side="left", padx=(0, 8))

    unit_badge = tk.Label(info_frame, text="", bg=PANEL2, fg=MUTED, font=FONT_XS,
                           padx=8, pady=3,
                           highlightthickness=1, highlightbackground=BORDER)
    unit_badge.pack(side="left", padx=(0, 8))

    horizon_badge = tk.Label(info_frame, text="", bg=PANEL2, fg=PURPLE, font=FONT_XS,
                              padx=8, pady=3,
                              highlightthickness=1, highlightbackground=BORDER)
    horizon_badge.pack(side="left")

    def update_source(*_):
        fam  = family_var.get()
        name = commodity_var.get()
        if not name:
            return
        cfg  = COMMODITY_REGISTRY[fam][name]
        src  = cfg["source"]
        unit = cfg["unit"]
        if src == "yahoo":
            source_badge.config(text=" Yahoo Finance ", bg="#0D3015",
                                 fg=GREEN2,
                                 highlightthickness=1, highlightbackground="#2EA043")
        else:
            source_badge.config(text=f" TradingView / {cfg['tv_exchange']} ",
                                 bg="#2D1F00", fg=AMBER,
                                 highlightthickness=1, highlightbackground=AMBER)
        unit_badge.config(text=f" {unit} ")
        horizon_badge.config(text=f" {cfg['liquid_months']}M horizon ")

    def update_commodities(*_):
        fam  = family_var.get()
        opts = list(COMMODITY_REGISTRY[fam].keys())
        commodity_cb["values"] = opts
        commodity_cb.current(0)
        update_source()

    commodity_cb.bind("<<ComboboxSelected>>", update_source)
    update_commodities()

    sep_line(body)

    # ── Section 3: Parameters ─────────────────────────────────────────────
    section_header(body, "3", "PARAMETERS")

    params_outer = tk.Frame(body, bg=BG)
    params_outer.pack(fill="x", padx=20, pady=(0, 6))

    # Row 1: RF rate + months
    row1 = tk.Frame(params_outer, bg=BG)
    row1.pack(fill="x", pady=(0, 8))

    # RF rate card
    rf_card = tk.Frame(row1, bg=PANEL2,
                        highlightthickness=1, highlightbackground=BORDER)
    rf_card.pack(side="left", padx=(0, 12), ipadx=12, ipady=8)
    lbl(rf_card, "RISK-FREE RATE  (%)", fg=MUTED, font=FONT_XS,
        bg=PANEL2).pack(anchor="w", padx=8, pady=(6, 2))
    rf_entry = mk_entry(rf_card, "5.0", width=10)
    rf_entry.pack(padx=8, pady=(0, 6))

    # Months card
    nm_card = tk.Frame(row1, bg=PANEL2,
                        highlightthickness=1, highlightbackground=BORDER)
    nm_card.pack(side="left", padx=(0, 12), ipadx=12, ipady=8)
    lbl(nm_card, "MONTHS FORWARD", fg=MUTED, font=FONT_XS,
        bg=PANEL2).pack(anchor="w", padx=8, pady=(6, 2))
    nm_entry = mk_entry(nm_card, "", width=10)
    nm_entry.pack(padx=8, pady=(0, 6))
    lbl(nm_card, "blank = commodity default", fg=BORDER, font=FONT_XS,
        bg=PANEL2).pack(padx=8, pady=(0, 4))

    sep_line(body)

    # ── Section 4: TradingView credentials ────────────────────────────────
    section_header(body, "4", "TRADINGVIEW CREDENTIALS  (optional)")

    tv_outer = tk.Frame(body, bg=PANEL2,
                         highlightthickness=1, highlightbackground=BORDER)
    tv_outer.pack(fill="x", padx=20, pady=(0, 4))

    tv_row = tk.Frame(tv_outer, bg=PANEL2)
    tv_row.pack(fill="x", padx=12, pady=10)

    lbl(tv_row, "Username", fg=MUTED, font=FONT_XS,
        bg=PANEL2).grid(row=0, column=0, sticky="w", padx=(0, 8))
    tv_user = mk_entry(tv_row, TV_USERNAME, width=24)
    tv_user.grid(row=1, column=0, padx=(0, 20), pady=(2, 0))

    lbl(tv_row, "Password", fg=MUTED, font=FONT_XS,
        bg=PANEL2).grid(row=0, column=1, sticky="w")
    tv_pass = mk_entry(tv_row, TV_PASSWORD, width=24, show="*")
    tv_pass.grid(row=1, column=1, pady=(2, 0))

    lbl(tv_outer,
        "  Leave empty for anonymous session — sufficient for most NYMEX contracts.",
        fg=BORDER, font=FONT_XS, bg=PANEL2).pack(anchor="w", padx=12, pady=(0, 8))

    # ── Footer: status + run button ───────────────────────────────────────
    tk.Frame(root, bg=BORDER, height=1).pack(fill="x")

    footer = tk.Frame(root, bg=PANEL, pady=0)
    footer.pack(fill="x", side="bottom")

    tk.Frame(footer, bg=ACCENT, width=4).pack(side="left", fill="y")

    footer_inner = tk.Frame(footer, bg=PANEL, padx=16, pady=12)
    footer_inner.pack(side="left", fill="both", expand=True)

    status_var = tk.StringVar(value="")
    tk.Label(footer_inner, textvariable=status_var, bg=PANEL, fg=GREEN2,
             font=FONT_SM).pack(anchor="w", pady=(0, 6))

    def confirm():
        fam  = family_var.get()
        name = commodity_var.get()
        if not name:
            messagebox.showerror("Error", "Please select a commodity.")
            return
        try:
            rf = float(rf_entry.get().strip()) / 100
        except ValueError:
            messagebox.showerror("Error", "Risk-free rate must be a number (e.g. 5.0).")
            return

        cfg = COMMODITY_REGISTRY[fam][name].copy()
        nm_raw = nm_entry.get().strip()
        if nm_raw:
            try:
                cfg["liquid_months"] = int(nm_raw)
            except ValueError:
                messagebox.showerror("Error", "Months forward must be an integer.")
                return

        result["cfg"]        = cfg
        result["rf"]         = rf
        result["tv_user"]    = tv_user.get().strip()
        result["tv_pass"]    = tv_pass.get().strip()
        status_var.set(f"  Launching  {name}  ...")
        root.after(350, root.destroy)

    run_btn = tk.Button(
        footer_inner, text="  RUN ANALYSIS  \u25b6",
        bg=ACCENT, fg=BG, font=("Consolas", 12, "bold"),
        relief="flat", padx=20, pady=8, cursor="hand2", bd=0,
        activebackground="#79B8FF", activeforeground=BG,
        command=confirm,
    )
    run_btn.pack(anchor="w")

    lbl(footer, "™ by AEG", fg=BORDER, font=FONT_TM,
        bg=PANEL).pack(side="right", padx=18, anchor="e")

    # Bring window to front — required in Spyder/IPython environments
    root.lift()
    root.attributes("-topmost", True)
    root.after(100, lambda: root.attributes("-topmost", False))
    root.focus_force()
    root.mainloop()

    if not result:
        print("Cancelled.")
        sys.exit(0)

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  PART 2 — TICKER BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_tickers(cfg: dict) -> list[dict]:
    """
    Generate contract metadata for the next n months,
    restricted to the commodity's active month codes.
    """
    now       = datetime.now()
    n         = cfg["liquid_months"]
    active    = cfg["active_months"]
    contracts = []

    month_offset = 0
    while len(contracts) < n:
        m    = (now.month - 1 + month_offset) % 12
        year = now.year + (now.month - 1 + month_offset) // 12
        month_offset += 1

        if MONTH_CODES[m] not in active:
            continue

        yr2 = str(year)[-2:]
        if cfg["source"] == "yahoo":
            ticker = cfg["yf_fmt"].replace("{M}", MONTH_CODES[m]).replace("{YY}", yr2)
        else:
            ticker = f"{cfg['tv_prefix']}{MONTH_CODES[m]}{year}"

        contracts.append({
            "ticker":        ticker,
            "label":         f"{MONTH_NAMES[m]}-{year}",
            "month_code":    MONTH_CODES[m],
            "maturity":      datetime(year, m + 1, 20),
            "months_to_mat": len(contracts) + 1,
        })

    return contracts


# ══════════════════════════════════════════════════════════════════════════════
#  PART 3 — DATA DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════════

def get_forward_curve(cfg: dict, rf: float,
                      tv_user: str = "", tv_pass: str = "") -> pd.DataFrame:
    """Route download to Yahoo Finance or TradingView based on cfg['source']."""
    if cfg["source"] == "yahoo":
        return _download_yahoo(cfg)
    else:
        return _download_tradingview(cfg, tv_user, tv_pass)


def _download_yahoo(cfg: dict) -> pd.DataFrame:
    """Single grouped yf.download() — avoids rate limiting."""
    try:
        import yfinance as yf
    except ImportError:
        print("  yfinance not installed -- pip install yfinance")
        return _synthetic_curve(cfg)

    contracts = build_tickers(cfg)
    tickers   = [c["ticker"] for c in contracts]
    n         = len(tickers)

    print(f"\n1. Downloading {n} contracts from Yahoo Finance...")
    print(f"   {tickers[0]}  ->  {tickers[-1]}")
    time.sleep(3)

    raw = yf.download(tickers, period="5d", auto_adjust=True, progress=False)

    if raw.empty:
        print("   No data received -- switching to synthetic curve.")
        return _synthetic_curve(cfg)

    closes = (raw["Close"] if isinstance(raw.columns, pd.MultiIndex)
              else raw[["Close"]]).iloc[-1]

    results, missing = [], []
    for c in contracts:
        t = c["ticker"]
        if t in closes.index and pd.notna(closes[t]):
            results.append({**c, "price": round(float(closes[t]), 2)})
            print(f"   {t:<16} {c['label']:<12}  {closes[t]:.2f} {cfg['unit']}")
        else:
            missing.append(t)

    print(f"\n   {len(results)}/{n} contracts loaded"
          + (f"  |  missing: {', '.join(missing)}" if missing else ""))

    if len(results) < 2:
        print("   Insufficient data -- switching to synthetic curve.")
        return _synthetic_curve(cfg)

    return _to_df(results, cfg)


def _download_tradingview(cfg: dict, tv_user: str, tv_pass: str) -> pd.DataFrame:
    """Per-contract tv.get_hist() with 1s sleep between calls."""
    try:
        from tvdatafeed import TvDatafeed, Interval
    except ImportError:
        print("  tvdatafeed not installed.")
        print("  Run: pip install git+https://github.com/StreamAlpha/tvdatafeed.git")
        return _synthetic_curve(cfg)

    contracts = build_tickers(cfg)
    n         = len(contracts)

    print(f"\n1. Connecting to TradingView ({cfg['tv_exchange']})...")
    try:
        tv = TvDatafeed(tv_user, tv_pass) if tv_user else TvDatafeed()
        print("   Session established.")
    except Exception as e:
        print(f"   Session failed ({e}) -- trying anonymous...")
        tv = TvDatafeed()

    print(f"\n2. Downloading {n} contracts...")
    results, missing = [], []

    for i, c in enumerate(contracts, 1):
        if i > 1:
            time.sleep(1)
        sym = c["ticker"]
        print(f"   [{i:2d}/{n}] {sym:<16} ({c['label']})  ...", end=" ", flush=True)
        try:
            hist = tv.get_hist(symbol=sym, exchange=cfg["tv_exchange"],
                               interval=Interval.in_daily, n_bars=5)
            if hist is not None and not hist.empty and "close" in hist.columns:
                price = round(float(hist["close"].dropna().iloc[-1]), 2)
                print(f"{price:.2f} {cfg['unit']}")
                results.append({**c, "price": price})
            else:
                print("no data"); missing.append(sym)
        except Exception as e:
            print(f"error -- {e}"); missing.append(sym)

    print(f"\n   {len(results)}/{n} contracts loaded"
          + (f"  |  missing: {', '.join(missing)}" if missing else ""))

    if len(results) < 2:
        print("   Insufficient data -- switching to synthetic curve.")
        return _synthetic_curve(cfg)

    return _to_df(results, cfg)


def _to_df(results: list, cfg: dict) -> pd.DataFrame:
    df = (pd.DataFrame(results)
            .sort_values("months_to_mat")
            .reset_index(drop=True))
    df = df.dropna(subset=["price"]).reset_index(drop=True)
    df["months_to_mat"] = range(1, len(df) + 1)
    print(f"   Spot  M1 : {df['price'].iloc[0]:.2f} {cfg['unit']}"
          f"   |   M{len(df)} : {df['price'].iloc[-1]:.2f} {cfg['unit']}"
          f" ({df['label'].iloc[-1]})")
    return df


def _synthetic_curve(cfg: dict) -> pd.DataFrame:
    """Realistic fallback curve using cost-of-carry model.
    Uses commodity-specific synthetic_spot so NS bounds are always valid.
    """
    print("   [synthetic mode] Generating fallback curve...")
    np.random.seed(42)
    contracts = build_tickers(cfg)
    spot = cfg.get("synthetic_spot", 100.0)
    records = []
    for i, c in enumerate(contracts):
        T        = (i + 1) / 12
        seasonal = 0.02 * spot * np.sin(2 * np.pi * (i + 2) / 12)
        noise    = np.random.normal(0, spot * 0.003)
        price    = round(spot * np.exp((0.05 - cfg["storage_cost"]) * T)
                         + seasonal + noise, 4)
        records.append({**c, "price": price})
    return pd.DataFrame(records)


# ══════════════════════════════════════════════════════════════════════════════
#  PART 4 — ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

class ForwardCurveAnalyzer:
    """Quantitative analysis of a commodity forward curve."""

    def __init__(self, df: pd.DataFrame, cfg: dict, r: float = 0.05):
        self.df   = df.copy().reset_index(drop=True)
        self.cfg  = cfg
        self.r    = r
        self.spot = df["price"].iloc[0]
        self.T    = df["months_to_mat"].values.astype(float)
        self.F    = df["price"].values

    def market_structure(self) -> dict:
        slope, _ = np.polyfit(self.T, self.F, 1)
        r2        = np.corrcoef(self.T, self.F)[0, 1] ** 2
        structure = "BACKWARDATION" if slope < 0 else "CONTANGO"
        return {
            "structure":       structure,
            "slope_per_month": round(slope, 4),
            "slope_per_year":  round(abs(slope) * 12, 3),
            "r_squared":       round(r2, 4),
            "interpretation":  (
                "Tight market — premium on immediate delivery (high convenience yield)."
                if structure == "BACKWARDATION" else
                "Carry-driven — abundant storage or anticipated future demand."
            ),
        }

    def convenience_yield(self) -> pd.DataFrame:
        rows = []
        u = self.cfg["storage_cost"]
        for i in range(1, len(self.T)):
            T_yr = self.T[i] / 12
            if T_yr <= 0 or self.F[i] <= 0:
                continue
            cy = self.r + u - np.log(self.F[i] / self.spot) / T_yr
            ry = (self.spot - self.F[i]) / self.F[i] / T_yr
            rows.append({
                "label":             self.df["label"].iloc[i],
                "months_to_mat":     int(self.T[i]),
                "price":             round(self.F[i], 2),
                "convenience_yield": round(cy * 100, 3),
                "roll_yield":        round(ry * 100, 3),
            })
        return pd.DataFrame(rows)

    def calendar_spreads(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "leg_near": self.df["label"].iloc[i],
            "leg_far":  self.df["label"].iloc[i + 1],
            "spread":   round(self.F[i + 1] - self.F[i], 3),
            "pct_spot": round((self.F[i + 1] - self.F[i]) / self.spot * 100, 3),
        } for i in range(len(self.F) - 1)])

    def nelson_siegel_fit(self) -> dict:
        """
        Robust multi-attempt Nelson-Siegel fitting strategy.

        Core principle: β₀ bounds are ALWAYS anchored on the last observed
        futures price (F_last), NOT on registry absolute bounds.

        Rationale: β₀ is the long-run equilibrium (T→∞). The best estimate
        of where the curve converges is the most distant contract observed,
        extended by ±20%. Using registry bounds (e.g. [100, 8000] for Palladium)
        lets β₀ diverge to 3671 while β₂ hits its bound — economically absurd.

        Attempt 1 — Full NS, tight β₂ (±spot×0.08), β₀ anchored on F_last
            For curves with genuine curvature (hump or dip mid-curve).

        Attempt 2 — NS with β₂=0 (monotone), β₀ anchored on F_last
            For flat/monotone curves (most commodity forward curves).
            Runs when Attempt 1 has β₂ hitting its bound.

        Attempt 3 — Exponential F(T) = F∞ + (S-F∞)·exp(-k·T)
            Last resort for very sparse curves (N ≤ 5).

        Winner: attempt without bound-hit, lowest RMSE.
        """
        # ── Setup ──────────────────────────────────────────────────────────────
        lo_raw, hi_raw = self.cfg["ns_bounds"]
        lo = list(lo_raw)
        hi = list(hi_raw)

        F_last    = float(self.F[-1])    # last observed contract price
        F_mid     = float(np.median(self.F))
        b1_range  = float(abs(self.spot - F_last) * 3 + self.spot * 0.05)

        # β₀ anchored on F_last ±20%: the best estimate of LT equilibrium
        # is where the visible curve is heading — NOT a registry absolute limit.
        b0_lo = float(np.clip(F_last * 0.80, lo[0], hi[0] * 0.90))
        b0_hi = float(np.clip(F_last * 1.20, b0_lo * 1.01, hi[0]))

        # β₁ bounds: proportional to price level
        b1_lo = float(np.clip(-b1_range, lo[1], -1e-6))
        b1_hi = float(np.clip( b1_range,  1e-6, hi[1]))

        # β₂ tight: ±8% of spot (proportional, not absolute)
        b2_tight = self.spot * 0.08

        def ns(T, b0, b1, b2, tau):
            tau = max(tau, 0.1)
            phi = (1 - np.exp(-T / tau)) / (T / tau)
            return b0 + b1 * phi + b2 * (phi - np.exp(-T / tau))

        def ns_no_b2(T, b0, b1, tau):
            tau = max(tau, 0.1)
            phi = (1 - np.exp(-T / tau)) / (T / tau)
            return b0 + b1 * phi

        def _rmse(F_obs, F_fit):
            return float(np.sqrt(np.mean((np.array(F_obs) - np.array(F_fit)) ** 2)))

        def _at_bound(val, bound, tol=0.90):
            return abs(bound) > 1e-9 and abs(val) > tol * abs(bound)

        def _safe_p0_b0():
            return float(np.clip(F_last * 1.0, b0_lo * 1.01, b0_hi * 0.99))

        candidates = []

        # ── Attempt 1: full NS (β₂ ≠ 0), tight proportional bounds ────────────
        if len(self.T) > 6:
            try:
                lo1 = [b0_lo, b1_lo, -b2_tight, 0.3]
                hi1 = [b0_hi, b1_hi,  b2_tight, 60.0]
                p01 = [_safe_p0_b0(),
                       float(np.clip((F_last - self.spot) * 0.5, b1_lo, b1_hi)),
                       0.0, 5.0]
                popt1, _ = curve_fit(ns, self.T, self.F,
                                     p0=p01, bounds=(lo1, hi1), maxfev=15000)
                fitted1  = ns(self.T, *popt1)
                rmse1    = _rmse(self.F, fitted1)
                hit1     = _at_bound(popt1[2], b2_tight)
                candidates.append({
                    "beta0": round(float(popt1[0]), 3),
                    "beta1": round(float(popt1[1]), 3),
                    "beta2": round(float(popt1[2]), 3),
                    "tau":   round(float(popt1[3]), 3),
                    "rmse":  round(rmse1, 4),
                    "fitted":[round(float(p), 2) for p in fitted1],
                    "model": "Nelson-Siegel",
                    "_hit":  hit1,
                })
            except Exception:
                pass

        # ── Attempt 2: NS β₂=0 (monotone), β₀ anchored on F_last ──────────────
        try:
            lo2 = [b0_lo, b1_lo, 0.3]
            hi2 = [b0_hi, b1_hi, 60.0]
            p02 = [_safe_p0_b0(),
                   float(np.clip((F_last - self.spot) * 0.5, b1_lo, b1_hi)),
                   5.0]
            popt2, _ = curve_fit(ns_no_b2, self.T, self.F,
                                  p0=p02, bounds=(lo2, hi2), maxfev=15000)
            fitted2  = ns_no_b2(self.T, *popt2)
            rmse2    = _rmse(self.F, fitted2)
            candidates.append({
                "beta0": round(float(popt2[0]), 3),
                "beta1": round(float(popt2[1]), 3),
                "beta2": 0.0,
                "tau":   round(float(popt2[2]), 3),
                "rmse":  round(rmse2, 4),
                "fitted":[round(float(p), 2) for p in fitted2],
                "model": "NS monotone (β₂=0)",
                "_hit":  False,
            })
        except Exception:
            pass

        # ── Attempt 3: exponential fallback (N≤5 or all above failed) ──────────
        if len(self.T) <= 5 or not candidates:
            def simple_exp(T, F_inf, k):
                return F_inf + (self.spot - F_inf) * np.exp(-k * T)
            try:
                lo_e = float(np.clip(F_last * 0.80, lo[0], hi[0] * 0.90))
                hi_e = float(np.clip(F_last * 1.20, lo_e * 1.01, hi[0]))
                popt_e, _ = curve_fit(simple_exp, self.T, self.F,
                                       p0=[F_last, 0.15], maxfev=10000,
                                       bounds=([lo_e, 0.001], [hi_e, 10.0]))
                fitted_e = simple_exp(self.T, *popt_e)
                candidates.append({
                    "beta0": round(float(popt_e[0]), 3),
                    "beta1": round(float(self.spot - popt_e[0]), 3),
                    "beta2": 0.0,
                    "tau":   round(float(1 / max(popt_e[1], 1e-6)), 3),
                    "rmse":  round(_rmse(self.F, fitted_e), 4),
                    "fitted":[round(float(p), 2) for p in fitted_e],
                    "model": "Exponential (N≤5)",
                    "_hit":  False,
                })
            except Exception:
                pass

        if not candidates:
            return {"error": "All fitting attempts failed"}

        # ── Winner: no bound-hit first, then lowest RMSE ───────────────────────
        clean = [c for c in candidates if not c["_hit"]]
        best  = min(clean if clean else candidates, key=lambda c: c["rmse"])
        best.pop("_hit", None)
        return best

    def interpolate(self, n_points: int = 100) -> pd.DataFrame:
        T_fine = np.linspace(self.T[0], self.T[-1], n_points)
        return pd.DataFrame({"T": T_fine, "F": CubicSpline(self.T, self.F)(T_fine)})

    def report(self) -> dict:
        unit    = self.cfg["unit"]
        name    = self.cfg["name"]
        struct  = self.market_structure()
        cy_df   = self.convenience_yield()
        spreads = self.calendar_spreads()
        ns      = self.nelson_siegel_fit()

        W = 70
        print("\n" + "=" * W)
        print(f"  FORWARD CURVE REPORT -- {name}".center(W))
        print("=" * W)
        print(f"  Spot (M1)   :  {self.spot:.2f} {unit}")
        print(f"  Structure   :  {struct['structure']}")
        print(f"  Slope       :  {struct['slope_per_month']:+.4f} {unit}/month"
              f"   ({struct['slope_per_year']:+.3f} {unit}/year)")
        print(f"  R2          :  {struct['r_squared']:.4f}")
        print(f"  {struct['interpretation']}")

        print(f"\n  CONVENIENCE & ROLL YIELD -- first 6 months")
        print("  " + cy_df[["label","price","convenience_yield","roll_yield"]]
              .head(6).rename(columns={"convenience_yield":"cy_%","roll_yield":"ry_%"})
              .to_string(index=False).replace("\n", "\n  "))

        print(f"\n  CALENDAR SPREADS M+1 - M -- first 6 months")
        print("  " + spreads[["leg_near","leg_far","spread","pct_spot"]]
              .head(6).to_string(index=False).replace("\n", "\n  "))

        print(f"\n  NELSON-SIEGEL FIT")
        if "error" not in ns:
            print(f"    b0 (LT level)  = {ns['beta0']:.3f} {unit}")
            print(f"    b1 (slope)     = {ns['beta1']:+.3f}")
            print(f"    b2 (curvature) = {ns['beta2']:+.3f}")
            print(f"    tau (decay)    = {ns['tau']:.3f} months")
            print(f"    RMSE           = {ns['rmse']:.4f} {unit}")
        else:
            print(f"    fit failed: {ns['error']}")
        print("=" * W)

        return {"spot": self.spot, "structure": struct,
                "convenience_yields": cy_df.to_dict("records"),
                "calendar_spreads":   spreads.to_dict("records"),
                "nelson_siegel":      ns}


# ══════════════════════════════════════════════════════════════════════════════
#  PART 5 — DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

def plot_dashboard(df_today:  pd.DataFrame,
                   df_7d:     pd.DataFrame,
                   df_14d:    pd.DataFrame,
                   analyzer:  ForwardCurveAnalyzer,
                   save_path: str | None = None) -> None:
    """4-panel matplotlib dashboard auto-saved with timestamp."""
    cfg  = analyzer.cfg
    name = cfg["name"]
    unit = cfg["unit"]
    slug = name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("#","")

    if save_path is None:
        save_path = datetime.now().strftime(f"forward_curve_{slug}_%Y%m%d_%H%M%S.png")

    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec

    BG, PANEL, BORDER = "#0D1117", "#161B22", "#30363D"
    AMBER, BLUE, GREEN, RED, GRAY, TEXT = \
        "#F0A500", "#58A6FF", "#3FB950", "#FF7B72", "#8B949E", "#E6EDF3"

    fig = plt.figure(figsize=(16, 10), facecolor=BG)
    gs  = gridspec.GridSpec(2, 2, figure=fig,
                            hspace=0.42, wspace=0.33,
                            left=0.07, right=0.97, top=0.90, bottom=0.07)
    ax1, ax2, ax3, ax4 = [fig.add_subplot(gs[i//2, i%2]) for i in range(4)]

    def _style(ax, title):
        ax.set_facecolor(PANEL)
        ax.set_title(title, color=TEXT, fontsize=10, pad=10, fontweight="bold")
        ax.tick_params(colors=GRAY, labelsize=8)
        for s in ax.spines.values(): s.set_edgecolor(BORDER)
        ax.xaxis.label.set_color(GRAY)
        ax.yaxis.label.set_color(GRAY)
        ax.grid(True, alpha=0.10, color=BORDER)

    labels = df_today["label"].tolist()
    prices = df_today["price"].values
    x      = df_today["months_to_mat"].values

    # Panel 1 — Forward curve
    spl = analyzer.interpolate()
    ax1.plot(spl["T"], spl["F"], color=AMBER, lw=2.5, label="Today (spline)", zorder=4)
    ax1.scatter(x, prices, color=AMBER, s=40, zorder=5)
    ax1.plot(df_7d["months_to_mat"],  df_7d["price"],
             color=BLUE, lw=1.5, ls="--", alpha=0.75, label="7d ago")
    ax1.plot(df_14d["months_to_mat"], df_14d["price"],
             color=GRAY, lw=1.2, ls=":",  alpha=0.55, label="14d ago")
    spot_val = analyzer.spot
    ax1.axhline(spot_val, color=GREEN, lw=0.9, ls="-.", alpha=0.5,
                label=f"Spot {spot_val:.2f} {unit}")
    ax1.legend(fontsize=8, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
    ax1.set_ylabel(unit)
    ax1.set_xticks(x[::2])
    ax1.set_xticklabels(labels[::2], rotation=35, ha="right", fontsize=7)
    # Annualised total return annotation
    if len(prices) > 1 and not np.isnan(prices[0]) and not np.isnan(prices[-1]) and prices[0] > 0:
        ann_ret = (prices[-1] / prices[0]) ** (12 / len(prices)) - 1
        ax1.text(0.02, 0.05,
                 f"Ann. implied return: {ann_ret*100:+.1f}%",
                 transform=ax1.transAxes, fontsize=7.5, color=GRAY,
                 fontfamily="monospace")
    _style(ax1, f"{name} — Forward Curve Temporal Comparison")

    # Panel 2 — Calendar spreads
    sp   = analyzer.calendar_spreads()
    sv   = sp["spread"].values
    cols = [GREEN if v < 0 else RED for v in sv]
    ax2.bar(range(len(sv)), sv, color=cols, alpha=0.8, width=0.7,
            edgecolor=BG, linewidth=0.5)
    ax2.axhline(0, color=GRAY, lw=0.8)
    ax2.set_xticks(range(0, len(sv), 2))
    ax2.set_xticklabels(sp["leg_near"].iloc[::2], rotation=35, ha="right", fontsize=7)
    ax2.set_ylabel(f"Spread ({unit})")
    ax2.text(0.98, 0.96,
             f"Backwardation: {(sv<0).sum()}   Contango: {(sv>=0).sum()}",
             transform=ax2.transAxes, ha="right", va="top",
             color=GRAY, fontsize=7.5, fontfamily="monospace")
    # Roll cost annotation: cumulative M1→M6 spread
    if len(sv) >= 5:
        roll_cost_6m = sv[:5].sum()
        ax2.text(0.02, 0.05,
                 f"Roll cost M1→M6: {roll_cost_6m:+.2f} {unit}",
                 transform=ax2.transAxes, fontsize=7.5, color=GRAY,
                 fontfamily="monospace")
    _style(ax2, "Calendar Spreads  M+1 − M")

    # Panel 3 — Convenience yield
    cy   = analyzer.convenience_yield()
    T_cy = cy["months_to_mat"].values
    cv   = cy["convenience_yield"].values
    ax3.fill_between(T_cy, cv, alpha=0.18, color=AMBER)
    ax3.plot(T_cy, cv, color=AMBER, lw=2, label="Implied convenience yield")
    ax3.axhline(analyzer.r * 100, color=BLUE, lw=1, ls="--",
                label=f"Risk-free ({analyzer.r*100:.1f}%)")
    ax3.axhline(0, color=GRAY, lw=0.8)
    ax3.set_xlabel("Maturity (months)")
    ax3.set_ylabel("Convenience yield (%)")
    ax3.legend(fontsize=8, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
    # Hedging signal zone: shade when CY > 2 * RF (strong backwardation signal)
    hedge_threshold = analyzer.r * 100 * 2
    ax3.axhline(hedge_threshold, color="#FF7B72", lw=0.8, ls=":",
                label=f"Hedge signal (2× RF = {hedge_threshold:.1f}%)")
    ax3.fill_between(T_cy, hedge_threshold, cv,
                     where=(cv > hedge_threshold),
                     alpha=0.12, color="#FF7B72", label="_nolegend_")
    ax3.legend(fontsize=7.5, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
    _style(ax3, "Implied Convenience Yield")

    # Panel 4 — Nelson-Siegel
    ns = analyzer.nelson_siegel_fit()
    ax4.scatter(x, prices, color=AMBER, s=50, label="Observed", zorder=5)
    if "fitted" in ns:
        ax4.plot(x, ns["fitted"], color=BLUE, lw=2,
                 label=f"Nelson-Siegel  (RMSE = {ns['rmse']:.3f})")
        ax4.text(0.98, 0.06,
                 f"β0={ns['beta0']:.2f}  β1={ns['beta1']:+.2f}"
                 f"  β2={ns['beta2']:+.2f}  τ={ns['tau']:.1f}m",
                 transform=ax4.transAxes, ha="right", fontsize=7.5,
                 color=GRAY, fontfamily="monospace")
    ax4.set_ylabel(unit)
    ax4.set_xticks(x[::3])
    ax4.set_xticklabels(labels[::3], rotation=35, ha="right", fontsize=7)
    ax4.legend(fontsize=8, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
    # Fair value band: β₀ ± 1 RMSE
    if "fitted" in ns and "error" not in ns:
        b0, rmse = ns["beta0"], ns["rmse"]
        ax4.axhline(b0, color=GREEN, lw=1, ls="--", alpha=0.7,
                    label=f"β₀ fair value: {b0:.2f}")
        ax4.axhspan(b0 - rmse, b0 + rmse, alpha=0.07, color=GREEN)
        ax4.legend(fontsize=7.5, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
    _style(ax4, "Nelson-Siegel Curve Fitting")

    struct   = analyzer.market_structure()
    src_tag  = "Yahoo Finance" if cfg["source"] == "yahoo" else f"TradingView ({cfg.get('tv_exchange','')})"
    is_synth = cfg.get("_is_synthetic", False)
    data_tag = "SYNTHETIC DATA — no live prices" if is_synth else src_tag
    t_color  = "#FF7B72" if is_synth else TEXT

    fig.suptitle(
        f"{name}  —  Forward Curve Analysis  |  "
        f"{datetime.now().strftime('%d %b %Y  %H:%M:%S')}  |  "
        f"{struct['structure']}  ({struct['slope_per_month']:+.3f} {unit}/month)  |  "
        f"RF={analyzer.r*100:.1f}%  |  {data_tag}",
        fontsize=9, fontweight="bold", color=t_color, y=0.975,
    )
    fig.text(0.99, 0.005, "™ by AEG", ha="right", va="bottom",
             fontsize=8, color=GRAY, fontstyle="italic", fontfamily="monospace")

    # ── Hedger/Trader KPI bar ─────────────────────────────────────────────────
    cy_df    = analyzer.convenience_yield()
    sp_df    = analyzer.calendar_spreads()
    ns_res   = analyzer.nelson_siegel_fit()
    avg_cy6  = cy_df["convenience_yield"].head(6).mean() if not cy_df.empty else float("nan")
    m1_m3    = sp_df["spread"].head(2).sum() if len(sp_df) >= 2 else float("nan")
    lt_level = ns_res.get("beta0", float("nan")) if "error" not in ns_res else float("nan")
    basis    = prices[0] - lt_level if not np.isnan(lt_level) else float("nan")

    kpi_text = (
        f"  Avg CY 6M: {avg_cy6:+.1f}%"
        f"  |  M1-M3 spread: {m1_m3:+.2f} {unit}"
        f"  |  NS long-term (β₀): {lt_level:.2f} {unit}"
        f"  |  Basis vs LT: {basis:+.2f} {unit}"
        f"  |  Backwardation: {(sp_df['spread'].values < 0).sum()}/{len(sp_df)} months"
    )
    fig.text(0.5, 0.935, kpi_text, ha="center", va="bottom",
             fontsize=8, color=GRAY, fontfamily="monospace",
             bbox=dict(boxstyle="round,pad=0.3", facecolor="#161B22",
                       edgecolor="#30363D", alpha=0.9))

    plt.savefig(save_path, dpi=150, bbox_inches="tight", facecolor=BG)
    print(f"   Dashboard saved -> {save_path}")
    plt.close()


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

# ── Storage layout ─────────────────────────────────────────────────────────────
#
#   data/
#   ├── curves/
#   │   ├── wti_crude_oil/
#   │   │   ├── 2026-04-05.csv      ← one file per day per commodity
#   │   │   ├── 2026-04-04.csv
#   │   │   └── ...
#   │   └── natural_gas_henry_hub/
#   │       └── ...
#   ├── dashboards/                 ← PNG outputs
#   └── logs/
#       └── scheduler.log


# ── Logging ────────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}]  {msg}"
    print(line)
    with open(LOGS_DIR / "scheduler.log", "a") as f:
        f.write(line + "\n")


# ── CSV persistence ────────────────────────────────────────────────────────────

def commodity_slug(name: str) -> str:
    """Convert commodity name to filesystem-safe slug."""
    return (name.lower()
               .replace(" ", "_")
               .replace("(", "").replace(")", "")
               .replace("#", "").replace("/", "_")
               .replace(".", "").strip("_"))


def save_curve(df, commodity_name: str, date: datetime = None) -> Path:
    """
    Save a forward curve DataFrame to CSV.
    Path: data/curves/<slug>/YYYY-MM-DD.csv
    """
    if date is None:
        date = datetime.now()

    slug     = commodity_slug(commodity_name)
    curve_dir = CURVES_DIR / slug
    curve_dir.mkdir(parents=True, exist_ok=True)

    filepath = curve_dir / f"{date.strftime('%Y-%m-%d')}.csv"
    df.to_csv(filepath, index=False)
    log(f"Curve saved -> {filepath}  ({len(df)} contracts)")
    return filepath


def load_curve(commodity_name: str, date: datetime) -> "pd.DataFrame | None":
    """
    Load a saved curve for a given date. Returns None if not found.
    Tries the exact date first, then searches ±2 days (handles weekends/holidays).
    """
    
    slug      = commodity_slug(commodity_name)
    curve_dir = CURVES_DIR / slug

    if not curve_dir.exists():
        return None

    # Try exact date, then ±1, ±2 days (handles weekends/holidays)
    for delta in [0, -1, 1, -2, 2]:
        target = date + timedelta(days=delta)
        path   = curve_dir / f"{target.strftime('%Y-%m-%d')}.csv"
        if path.exists():
            df = pd.read_csv(path)
            if delta != 0:
                log(f"  Loaded curve from {target.strftime('%Y-%m-%d')} "
                    f"(requested {date.strftime('%Y-%m-%d')}, delta={delta:+d}d)")
            return df

    return None


def load_historical_curves(commodity_name: str) -> "dict[str, pd.DataFrame]":
    """
    Load today, J-7 and J-14 curves.
    Returns dict with keys: 'today', '7d', '14d'.
    Missing snapshots are None (caller must handle fallback).
    """
    now    = datetime.now()
    result = {}

    result["today"] = load_curve(commodity_name, now)
    result["7d"]    = load_curve(commodity_name, now - timedelta(days=7))
    result["14d"]   = load_curve(commodity_name, now - timedelta(days=14))

    found = sum(1 for v in result.values() if v is not None)
    log(f"Historical snapshots loaded: {found}/3 "
        f"(today={'Y' if result['today'] is not None else 'N'}, "
        f"7d={'Y' if result['7d'] is not None else 'N'}, "
        f"14d={'Y' if result['14d'] is not None else 'N'})")

    return result


def list_available_dates(commodity_name: str) -> list:
    """List all dates with saved curves for a given commodity."""
    slug      = commodity_slug(commodity_name)
    curve_dir = CURVES_DIR / slug

    if not curve_dir.exists():
        return []

    dates = sorted([
        f.stem for f in curve_dir.glob("*.csv")
    ], reverse=True)
    return dates


# ── Run history ────────────────────────────────────────────────────────────────

def save_run_record(commodity_name: str, cfg: dict, report: dict,
                    png_path: str) -> None:
    """
    Append a JSON record of this run to the run history file.
    Useful for tracking structure changes over time.
    """
    record = {
        "timestamp":   datetime.now().isoformat(),
        "commodity":   commodity_name,
        "source":      cfg.get("source", ""),
        "spot":        report.get("spot"),
        "structure":   report.get("structure", {}).get("structure"),
        "slope":       report.get("structure", {}).get("slope_per_month"),
        "r2":          report.get("structure", {}).get("r_squared"),
        "png":         str(png_path),
    }

    history_file = DATA_DIR / "run_history.jsonl"
    with open(history_file, "a") as f:
        f.write(json.dumps(record) + "\n")


def load_run_history(commodity_name: str = None) -> list:
    """
    Load run history. Filter by commodity if provided.
    Returns list of dicts sorted by timestamp desc.
    """
    history_file = DATA_DIR / "run_history.jsonl"
    if not history_file.exists():
        return []

    records = []
    with open(history_file) as f:
        for line in f:
            try:
                r = json.loads(line.strip())
                if commodity_name is None or r.get("commodity") == commodity_name:
                    records.append(r)
            except json.JSONDecodeError:
                continue

    return sorted(records, key=lambda x: x["timestamp"], reverse=True)


# ── Single run ─────────────────────────────────────────────────────────────────

def run_once(commodity_name: str, family: str,
             rf: float = 0.05,
             tv_user: str = "", tv_pass: str = "") -> None:
    """
    Execute a full forward curve analysis for one commodity:
      1. Download live data
      2. Save curve to CSV
      3. Load historical J-7 / J-14 from CSV (real data, not simulated)
      4. Run analysis
      5. Generate dashboard
      6. Save run record
    """
    

    log(f"=== Starting run: {commodity_name} ===")

    # ── Get config ──────────────────────────────────────────
    if family not in COMMODITY_REGISTRY:
        log(f"ERROR: Family '{family}' not found in registry.")
        return
    if commodity_name not in COMMODITY_REGISTRY[family]:
        log(f"ERROR: '{commodity_name}' not found in '{family}'.")
        return

    cfg = COMMODITY_REGISTRY[family][commodity_name].copy()

    # ── Download today's curve ───────────────────────────────
    df_today = get_forward_curve(cfg, rf, tv_user, tv_pass)
    save_curve(df_today, commodity_name)

    # ── Load historical snapshots (real data) ────────────────
    hist     = load_historical_curves(commodity_name)
    df_today_loaded = hist["today"] if hist["today"] is not None else df_today

    # Fallback to simulated shift if real historical data unavailable
    if hist["7d"] is not None:
        df_7d = hist["7d"]
        log("  J-7 : loaded from CSV (real data)")
    else:
        np.random.seed(1)
        s = df_today["price"].mean() * 0.015
        df_7d = df_today.assign(
            price=df_today["price"] + np.random.normal(s, s * 0.2, len(df_today)))
        log("  J-7 : no CSV found -- using simulated shift")

    if hist["14d"] is not None:
        df_14d = hist["14d"]
        log("  J-14: loaded from CSV (real data)")
    else:
        np.random.seed(2)
        s = df_today["price"].mean() * 0.030
        df_14d = df_today.assign(
            price=df_today["price"] + np.random.normal(s, s * 0.2, len(df_today)))
        log("  J-14: no CSV found -- using simulated shift")

    # ── Analysis ─────────────────────────────────────────────
    analyzer = ForwardCurveAnalyzer(df_today_loaded, cfg, r=rf)
    report   = analyzer.report()

    # ── Dashboard ────────────────────────────────────────────
    slug = commodity_slug(commodity_name)
    png_dir  = DASHBOARDS_DIR / slug
    png_dir.mkdir(parents=True, exist_ok=True)
    png_path = png_dir / datetime.now().strftime(f"{slug}_%Y%m%d_%H%M%S.png")

    plot_dashboard(df_today_loaded, df_7d, df_14d, analyzer,
                   save_path=str(png_path))

    # ── Record ───────────────────────────────────────────────
    save_run_record(commodity_name, cfg, report, png_path)
    log(f"=== Run complete: {commodity_name} ===\n")


# ── Scheduled batch run ────────────────────────────────────────────────────────

def run_batch(targets: list[dict], rf: float = 0.05) -> None:
    """
    Run analysis for multiple commodities in sequence.
    targets = [{"family": "Energy", "commodity": "WTI Crude Oil"}, ...]
    """
    log(f"Batch run started -- {len(targets)} commodities")
    for t in targets:
        try:
            run_once(
                commodity_name = t["commodity"],
                family         = t["family"],
                rf             = rf,
                tv_user        = t.get("tv_user", ""),
                tv_pass        = t.get("tv_pass", ""),
            )
            time.sleep(5)   # pause between commodities
        except Exception as e:
            log(f"ERROR on {t['commodity']}: {e}")

    log(f"Batch run complete -- {len(targets)} commodities processed")


DEFAULT_BATCH = [
    {"family": "Energy",      "commodity": "WTI Crude Oil"},
    {"family": "Energy",      "commodity": "Brent Crude Oil"},
    {"family": "Energy",      "commodity": "Natural Gas (Henry Hub)"},
    {"family": "Energy",      "commodity": "Jet Kerosene CIF NWE (Platts)"},
    {"family": "Metals",      "commodity": "Gold"},
    {"family": "Metals",      "commodity": "Copper"},
    {"family": "Agriculture", "commodity": "Corn"},
    {"family": "Agriculture", "commodity": "Wheat (CBOT)"},
    {"family": "Agriculture", "commodity": "Soybeans"},
    {"family": "Agriculture", "commodity": "Sugar #11"},
    {"family": "Agriculture", "commodity": "Coffee (Arabica)"},
]


# ── Entry point ────────────────────────────────────────────────────────────────

def _load_snaps(commodity_name: str, df_today) -> dict:
    snap = {"today": df_today, "7d": None, "14d": None}
    hist = load_historical_curves(commodity_name)
    for k in ["7d", "14d"]:
        if hist.get(k) is not None:
            snap[k] = hist[k]
    np.random.seed(1)
    if snap["7d"] is None:
        s = df_today["price"].mean() * 0.015
        snap["7d"] = df_today.assign(price=df_today["price"] + np.random.normal(s, s*0.2, len(df_today)))
    np.random.seed(2)
    if snap["14d"] is None:
        s = df_today["price"].mean() * 0.030
        snap["14d"] = df_today.assign(price=df_today["price"] + np.random.normal(s, s*0.2, len(df_today)))
    return snap

# ── EIA Series IDs ─────────────────────────────────────────────────────────────
# Full reference: https://api.eia.gov/v2/

EIA_SERIES = {
    # Petroleum & crude
    "cushing_stocks":       "PET.WCSSTUS1.W",    # Cushing OK crude stocks (kbbl), weekly
    "us_crude_stocks":      "PET.WCRSTUS1.W",    # US total crude stocks (kbbl), weekly
    "us_crude_production":  "PET.WCRFPUS2.W",    # US crude production (kbbl/day), weekly
    "wti_spot":             "PET.RWTC.D",         # WTI spot price ($/bbl), daily
    "brent_spot":           "PET.RBRTE.D",        # Brent spot price ($/bbl), daily
    "us_refinery_runs":     "PET.WCRRIUS2.W",    # Refinery utilization (%), weekly
    "gasoline_stocks":      "PET.WGTSTUS1.W",    # US gasoline stocks (kbbl), weekly
    "distillate_stocks":    "PET.WDISTUS1.W",    # US distillate stocks (kbbl), weekly

    # Natural gas
    "natgas_storage":       "NG.NW2_EPG0_SWO_R48_BCF.W",  # US NG storage (Bcf), weekly
    "natgas_spot":          "NG.RNGWHHD.D",       # Henry Hub spot ($/MMBtu), daily
    "natgas_production":    "NG.N9070US2.M",      # US NG production (Bcf/month), monthly
}




class EIAClient:
    """
    Client for the EIA Open Data API v2.
    Fetches energy fundamental time series with local caching (24h TTL).
    """

    BASE_URL = "https://api.eia.gov/v2/seriesid/{series_id}"

    def __init__(self, api_key: str, cache_ttl_hours: int = 24):
        self.api_key       = api_key
        self.cache_ttl     = timedelta(hours=cache_ttl_hours)
        self.session       = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    # ── Single series fetch ──────────────────────────────────────────────────

    def fetch_series(self, series_id: str, n_periods: int = 52) -> pd.DataFrame:
        """
        Fetch one EIA time series. Returns DataFrame with [date, value].
        Uses local cache to avoid redundant API calls.
        """
        cache_file = EIA_CACHE_DIR / f"{series_id.replace('.', '_')}.csv"

        # Check cache freshness
        if cache_file.exists():
            age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if age < self.cache_ttl:
                df = pd.read_csv(cache_file, parse_dates=["date"])
                return df

        # Fetch from API
        url    = self.BASE_URL.format(series_id=series_id)
        params = {
            "api_key": self.api_key,
            "data[0]": "value",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": n_periods,
        }

        try:
            r = self.session.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()

            rows = data.get("response", {}).get("data", [])
            if not rows:
                print(f"   EIA: no data for {series_id}")
                return pd.DataFrame(columns=["date", "value"])

            df = pd.DataFrame(rows)[["period", "value"]]
            df.columns = ["date", "value"]
            df["date"]  = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna().sort_values("date").reset_index(drop=True)

            df.to_csv(cache_file, index=False)
            return df

        except requests.RequestException as e:
            print(f"   EIA fetch error ({series_id}): {e}")
            if cache_file.exists():
                return pd.read_csv(cache_file, parse_dates=["date"])
            return pd.DataFrame(columns=["date", "value"])

    # ── Batch fetch ──────────────────────────────────────────────────────────

    def fetch_all(self, series_keys: list = None) -> dict:
        """
        Fetch multiple series. Returns dict {key: DataFrame}.
        """
        if series_keys is None:
            series_keys = list(EIA_SERIES.keys())

        result = {}
        for key in series_keys:
            series_id = EIA_SERIES.get(key)
            if series_id is None:
                print(f"   EIA: unknown series key '{key}'")
                continue
            print(f"   EIA fetching {key}...", end=" ", flush=True)
            df = self.fetch_series(series_id)
            result[key] = df
            print(f"{len(df)} periods" if not df.empty else "no data")
            time.sleep(0.3)   # polite rate limiting

        return result

    # ── Summary statistics ───────────────────────────────────────────────────

    def weekly_change(self, df: pd.DataFrame) -> dict:
        """Compute week-on-week change for the latest reading."""
        if df is None or len(df) < 2:
            return {"latest": None, "prev": None, "change": None, "pct_change": None}

        latest  = float(df["value"].iloc[-1])
        prev    = float(df["value"].iloc[-2])
        change  = latest - prev
        pct     = change / prev * 100 if prev != 0 else 0

        return {
            "latest":     round(latest, 2),
            "prev":       round(prev, 2),
            "change":     round(change, 2),
            "pct_change": round(pct, 2),
            "date":       df["date"].iloc[-1].strftime("%Y-%m-%d"),
        }

    def fundamentals_summary(self, data: dict) -> dict:
        """
        Build a compact fundamentals summary dict for dashboard overlay.
        """
        summary = {}
        for key, df in data.items():
            summary[key] = self.weekly_change(df)
        return summary


# ── Matplotlib overlay ─────────────────────────────────────────────────────────

def overlay_fundamentals(ax, data: dict, commodity_name: str) -> None:
    """
    Add an EIA fundamentals annotation box to a matplotlib axis.
    Shows the most recent reading + weekly change for relevant series.
    """
    name_lower = commodity_name.lower()
    lines = []

    if "crude" in name_lower or "wti" in name_lower or "brent" in name_lower:
        relevant = [
            ("Cushing stocks",   "cushing_stocks",    "kbbl"),
            ("US crude stocks",  "us_crude_stocks",   "kbbl"),
            ("WTI spot",         "wti_spot",          "$/bbl"),
            ("US production",    "us_crude_production","kbbl/d"),
        ]
    elif "gas" in name_lower:
        relevant = [
            ("NG storage",       "natgas_storage",    "Bcf"),
            ("HH spot",          "natgas_spot",       "$/MMBtu"),
        ]
    elif "gasoline" in name_lower or "rbob" in name_lower:
        relevant = [
            ("Gasoline stocks",  "gasoline_stocks",   "kbbl"),
            ("Refinery runs",    "us_refinery_runs",  "%"),
        ]
    elif "heating" in name_lower or "gasoil" in name_lower or "diesel" in name_lower:
        relevant = [
            ("Distillate stocks","distillate_stocks", "kbbl"),
            ("Refinery runs",    "us_refinery_runs",  "%"),
        ]
    else:
        return   # no EIA overlay for non-energy commodities

    for label, key, unit in relevant:
        df = data.get(key)
        if df is None or df.empty:
            continue
        latest = float(df["value"].iloc[-1])
        if len(df) >= 2:
            prev   = float(df["value"].iloc[-2])
            chg    = latest - prev
            arrow  = "▲" if chg > 0 else "▼" if chg < 0 else "─"
            lines.append(f"{label}: {latest:,.0f} {unit}  {arrow}{abs(chg):,.0f}")
        else:
            lines.append(f"{label}: {latest:,.0f} {unit}")

    if not lines:
        return

    date_str = ""
    for key, _ in [(k, None) for k, _, _ in relevant]:
        df = data.get(key)
        if df is not None and not df.empty:
            date_str = f"EIA week of {df['date'].iloc[-1].strftime('%d %b')}"
            break

    text = date_str + "\n" + "\n".join(lines)
    ax.text(0.98, 0.98, text,
            transform=ax.transAxes, ha="right", va="top",
            fontsize=7, fontfamily="monospace",
            color="#8B949E",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="#161B22",
                      edgecolor="#30363D", alpha=0.85))


# ── Standalone test ────────────────────────────────────────────────────────────

def generate_trading_signals(struct, cy_df, spreads, ns, spot, rf, unit, cfg, **kwargs):
    """
    Comprehensive trading signal engine — 25+ signals across 12 categories.
    Returns list of signal dicts: level, icon, title, detail, rationale, category.
    level: BUY / SELL / SPREAD / HEDGE / ARBITRAGE / NEUTRAL / WARNING
    """
    import numpy as np
    signals = []

    # ── Pre-compute all metrics ───────────────────────────────────────────────
    rf_pct      = rf * 100
    storage_pct = cfg.get("storage_cost", 0.06) * 100
    sv          = spreads["spread"].values if not spreads.empty else np.array([])
    n_sv        = len(sv)
    prices_sv   = spreads  # full spreads df
    n_back      = int((sv < 0).sum()) if n_sv else 0
    n_cont      = int((sv >= 0).sum()) if n_sv else 0
    n_total     = n_back + n_cont
    back_ratio  = n_back / n_total if n_total > 0 else 0
    slope       = struct["slope_per_month"]
    r2          = struct.get("r_squared", 0)

    # CY / roll yield metrics
    cy_all = cy_df["convenience_yield"].values if not cy_df.empty else np.array([0])
    ry_all = cy_df["roll_yield"].values        if not cy_df.empty else np.array([0])
    avg_cy      = float(np.mean(cy_all[:6]))  if len(cy_all) >= 1 else 0
    avg_ry      = float(np.mean(ry_all[:6]))  if len(ry_all) >= 1 else 0
    max_cy      = float(np.max(cy_all))       if len(cy_all) >= 1 else 0
    min_cy      = float(np.min(cy_all))       if len(cy_all) >= 1 else 0
    cy_slope    = float(cy_all[-1] - cy_all[0]) if len(cy_all) >= 2 else 0

    # Spread metrics
    m1m2  = float(sv[0])         if n_sv >= 1 else 0
    m1m3  = float(sv[0]+sv[1])   if n_sv >= 2 else 0
    m1m6  = float(sv[:5].sum())  if n_sv >= 5 else float(sv.sum())
    m1m12 = float(sv[:11].sum()) if n_sv >= 11 else float(sv.sum())
    m3m6  = float(sv[2:5].sum()) if n_sv >= 5 else 0
    max_spread_idx = int(np.argmax(np.abs(sv))) if n_sv else 0
    max_spread_val = float(sv[max_spread_idx]) if n_sv else 0

    # NS metrics
    beta0  = ns.get("beta0", spot) or spot
    beta1  = ns.get("beta1", 0) or 0
    beta2  = ns.get("beta2", 0) or 0
    tau    = ns.get("tau", 5) or 5
    basis  = (spot - beta0) / spot * 100 if beta0 > 0 else 0

    # Curve shape
    curve_pct  = (spot - float(cy_df["price"].iloc[-1])) / spot * 100 if not cy_df.empty else 0
    net_carry  = rf_pct + storage_pct - avg_cy  # positive = carry costs exceed convenience

    def _sig(level, icon, title, detail, rationale, cat):
        signals.append(dict(level=level, icon=icon, title=title,
                            detail=detail, rationale=rationale, category=cat))

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 1 — CONVENIENCE YIELD
    # ═══════════════════════════════════════════════════════════════════════════

    # 1a. CY >> 2x RF → strong physical accumulation signal
    if avg_cy > rf_pct * 2.5:
        _sig("BUY","▲","Strong physical accumulation",
             f"CY {avg_cy:+.1f}% >> 2.5x RF ({rf_pct*2.5:.1f}%)",
             f"Exceptionally high convenience yield ({avg_cy:.1f}%) relative to financing cost "
             f"({rf_pct:.1f}%). Physical holders earn a large implied premium — strong incentive "
             f"to accumulate and hold inventory.","Convenience Yield")

    elif avg_cy > rf_pct * 1.5:
        _sig("BUY","▲","Physical storage signal",
             f"CY {avg_cy:+.1f}% > 1.5x RF ({rf_pct*1.5:.1f}%)",
             f"Convenience yield above 1.5× risk-free rate signals that market participants "
             f"value physical ownership premium. Favours long physical / long prompt futures.","Convenience Yield")

    # 1b. CY < 0 → cash-and-carry arbitrage
    if avg_cy < -rf_pct * 0.5:
        _sig("ARBITRAGE","⇄","Cash-and-carry arbitrage",
             f"CY {avg_cy:+.1f}% (negative)  |  forward premium = {-avg_cy:.1f}%",
             f"Negative convenience yield: forward price exceeds full cost of carry. "
             f"Classic arbitrage: buy spot, store, sell forward at premium. "
             f"Entry point: borrow at {rf_pct:.1f}%, storage {storage_pct:.1f}%/yr, "
             f"sell M{min(6,n_sv+1)} at {m1m6:+.2f} {unit} premium.","Convenience Yield")

    # 1c. CY vs storage cost only
    if 0 < avg_cy < storage_pct * 0.5:
        _sig("NEUTRAL","◉","CY below storage cost",
             f"CY {avg_cy:+.1f}% < 50% of storage cost ({storage_pct:.1f}%/yr)",
             f"Convenience yield covers less than half the storage cost. "
             f"Physical inventory not economically justified at current levels. "
             f"Prefer futures exposure over physical storage.","Convenience Yield")

    # 1d. CY term structure — increasing (spot tightening)
    if cy_slope > 3 and len(cy_all) >= 4:
        _sig("BUY","▲","CY term structure steepening",
             f"CY slope: {cy_all[0]:.1f}% (M2) → {cy_all[-1]:.1f}% (M{len(cy_all)+1}), +{cy_slope:.1f}pp",
             f"Rising convenience yield along the curve suggests progressive tightening "
             f"of the physical market. Prompt shortage spreading to deferred months.","Convenience Yield")

    elif cy_slope < -3 and len(cy_all) >= 4:
        _sig("SELL","▼","CY term structure flattening",
             f"CY slope: {cy_all[0]:.1f}% (M2) → {cy_all[-1]:.1f}% (M{len(cy_all)+1}), {cy_slope:.1f}pp",
             f"Declining CY along the curve signals improving supply balance. "
             f"Near-term tightness fading into deferred months — bearish for prompt.","Convenience Yield")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 2 — ROLL YIELD & CARRY
    # ═══════════════════════════════════════════════════════════════════════════

    # 2a. Strong positive roll yield → long futures carry
    if avg_ry > 8:
        _sig("BUY","▲","Strong roll carry — long futures",
             f"Avg roll yield {avg_ry:+.1f}%/yr  ({n_back}/{n_total} months backwardation)",
             f"Rolling a long position in strong backwardation generates {avg_ry:.1f}%/yr carry. "
             f"Highly attractive for passive long investors (ETFs, index trackers). "
             f"Total return = spot return + {avg_ry:.1f}% roll yield.","Roll Yield")

    elif avg_ry > 3:
        _sig("BUY","▲","Positive roll yield — long bias",
             f"Avg roll yield {avg_ry:+.1f}%/yr",
             f"Moderate positive roll yield supports long futures positions. "
             f"Each monthly roll from M1 to M2 generates positive carry.","Roll Yield")

    # 2b. Negative roll yield → avoid or short via spreads
    if avg_ry < -8:
        _sig("SELL","▼","Heavy roll cost — avoid long futures",
             f"Avg roll yield {avg_ry:+.1f}%/yr  ({n_cont}/{n_total} months contango)",
             f"Severe negative roll yield ({avg_ry:.1f}%/yr) destroys returns for passive longs. "
             f"Commodity indices underperform spot by this amount annually. "
             f"Alternative: short-dated contracts, physical exposure, or short the roll.","Roll Yield")

    elif avg_ry < -3:
        _sig("SELL","▼","Negative roll yield — contango drag",
             f"Avg roll yield {avg_ry:+.1f}%/yr",
             f"Negative roll yield erodes long positions. Each roll costs the contango spread. "
             f"Consider deferred contracts or flat-price alternatives.","Roll Yield")

    # 2c. Net carry cost analysis
    if net_carry > rf_pct * 1.5:
        _sig("SELL","▼","High net carry cost",
             f"Net carry = RF {rf_pct:.1f}% + storage {storage_pct:.1f}% - CY {avg_cy:.1f}% = {net_carry:+.1f}%/yr",
             f"Full cost of carry ({rf_pct+storage_pct:.1f}%/yr) far exceeds convenience yield "
             f"({avg_cy:.1f}%). Physical holders are losing to carry costs. "
             f"Market overvalued relative to theoretical forward price.","Roll Yield")

    elif net_carry < -rf_pct:
        _sig("BUY","▲","Net carry positive — convenience dominates",
             f"Net carry = RF {rf_pct:.1f}% + storage {storage_pct:.1f}% - CY {avg_cy:.1f}% = {net_carry:+.1f}%/yr",
             f"Convenience yield more than offsets all carry costs. "
             f"Physical position earns positive net yield — bullish for spot.","Roll Yield")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 3 — CALENDAR SPREADS
    # ═══════════════════════════════════════════════════════════════════════════

    # 3a. M1-M2 prompt spread (most liquid)
    if n_sv >= 1:
        if m1m2 < -spot * 0.008:
            _sig("SPREAD","↕","Sell prompt spread M1-M2",
                 f"M1-M2 = {m1m2:+.3f} {unit}  ({m1m2/spot*100:+.2f}% of spot)",
                 f"Front-end backwardation: M1 trades at premium to M2. "
                 f"Trade: buy M2, sell M1. Capture premium as M1 rolls into M2. "
                 f"Risk: supply squeeze could widen further.","Calendar Spreads")

        elif m1m2 > spot * 0.006:
            _sig("SPREAD","↕","Buy prompt spread M1-M2",
                 f"M1-M2 = {m1m2:+.3f} {unit}  ({m1m2/spot*100:+.2f}% of spot)",
                 f"Front-end contango: M2 trades at premium to M1. "
                 f"Trade: buy M1, sell M2. Roll-down profit as M2 converges to M1. "
                 f"Risk: demand surge could flip to backwardation.","Calendar Spreads")

    # 3b. M1-M3 spread
    if n_sv >= 2:
        if m1m3 < -spot * 0.015:
            _sig("SPREAD","↕","M1-M3 backwardation — sell the spread",
                 f"M1-M3 = {m1m3:+.3f} {unit}  ({m1m3/spot*100:+.2f}% of spot)",
                 f"Two-month backwardation reflects tight near-term supply. "
                 f"Buy M1, sell M3. Annualised return: {m1m3/spot*100*6:.1f}% if spread closes.","Calendar Spreads")

        elif m1m3 > spot * 0.012:
            _sig("SPREAD","↕","M1-M3 contango — buy the spread",
                 f"M1-M3 = {m1m3:+.3f} {unit}  ({m1m3/spot*100:+.2f}% of spot)",
                 f"Two-month contango reflects storage surplus. "
                 f"Sell M1, buy M3. Profit = contango minus storage costs.","Calendar Spreads")

    # 3c. M1-M6 roll cost (hedger's 6-month horizon)
    if n_sv >= 5:
        if m1m6 < -spot * 0.025:
            _sig("SPREAD","↕","6-month roll gain — roll M1 to M6",
                 f"M1-M6 cumulative = {m1m6:+.3f} {unit}  ({m1m6/spot*100:+.2f}% of spot)",
                 f"Rolling a long position from M1 to M6 generates {abs(m1m6):.2f} {unit} gain "
                 f"({abs(m1m6/spot*100):.1f}% of spot). Attractive for long-term buyers.","Calendar Spreads")

        elif m1m6 > spot * 0.02:
            _sig("SPREAD","↕","6-month roll cost — contango drag",
                 f"M1-M6 cumulative = {m1m6:+.3f} {unit}  ({m1m6/spot*100:+.2f}% of spot)",
                 f"Rolling costs {m1m6:.2f} {unit} over 6 months ({m1m6/spot*100:.1f}% of spot). "
                 f"Long investors should use deferred entry or physical storage alternatives.","Calendar Spreads")

    # 3d. Back-end vs front-end spread divergence (M3-M6 vs M1-M3)
    if n_sv >= 5 and abs(m1m3) > 0.001:
        ratio = m3m6 / m1m3 if m1m3 != 0 else 0
        if m1m3 < 0 and m3m6 > 0:
            _sig("SPREAD","↕","Mixed structure — front backwardation / back contango",
                 f"M1-M3 = {m1m3:+.3f}  |  M3-M6 = {m3m6:+.3f} {unit}",
                 f"Curve inverts: front in backwardation, back in contango. "
                 f"Short-term supply squeeze expected to normalise. "
                 f"Trade: sell M1-M3, buy M3-M6 (fly).","Calendar Spreads")

        elif m1m3 > 0 and m3m6 < 0:
            _sig("SPREAD","↕","Inverted structure — front contango / back backwardation",
                 f"M1-M3 = {m1m3:+.3f}  |  M3-M6 = {m3m6:+.3f} {unit}",
                 f"Unusual humped structure: near-term surplus, future tightness priced in. "
                 f"Trade: buy M1-M3, sell M3-M6 (fly). "
                 f"Watch for demand acceleration signal.","Calendar Spreads")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 4 — NELSON-SIEGEL MODEL
    # ═══════════════════════════════════════════════════════════════════════════

    # 4a. Basis vs fair value
    if basis > 12:
        _sig("SELL","▼","NS mean-reversion — spot overvalued",
             f"Spot {basis:+.1f}% above NS equilibrium ({beta0:.2f} {unit})",
             f"Nelson-Siegel model estimates long-run fair value at {beta0:.2f} {unit}. "
             f"Spot is {abs(basis):.1f}% expensive. Mean-reversion historically takes "
             f"{tau:.0f} months. Potential short entry with {tau:.0f}M target at {beta0:.2f}.","Nelson-Siegel")

    elif basis < -12:
        _sig("BUY","▲","NS mean-reversion — spot undervalued",
             f"Spot {basis:+.1f}% below NS equilibrium ({beta0:.2f} {unit})",
             f"Nelson-Siegel model estimates long-run fair value at {beta0:.2f} {unit}. "
             f"Spot is {abs(basis):.1f}% cheap. Reversion target {beta0:.2f} {unit} "
             f"in approximately {tau:.0f} months.","Nelson-Siegel")

    elif 5 < abs(basis) <= 12:
        dir_ = "overvalued" if basis > 0 else "undervalued"
        act_ = "short bias" if basis > 0 else "long bias"
        _sig("NEUTRAL","◉",f"NS fair value signal — {dir_}",
             f"Spot {basis:+.1f}% vs NS β₀={beta0:.2f} {unit}  (τ={tau:.1f}mo)",
             f"Moderate divergence from model equilibrium. "
             f"Favours {act_} on mean-reversion horizon of ~{tau:.0f} months.","Nelson-Siegel")

    # 4b. Beta2 curvature signal
    if beta2 != 0 and abs(beta2) > spot * 0.02:
        if beta2 > 0:
            _sig("NEUTRAL","◉","NS hump — mid-curve premium",
                 f"β₂ = +{beta2:.2f} (positive curvature)  τ={tau:.1f}mo",
                 f"Positive β₂ creates a hump in the forward curve around maturity {tau:.0f}M. "
                 f"Mid-curve contracts are relatively expensive vs front and back. "
                 f"Sell mid-curve, buy wings (calendar butterfly).","Nelson-Siegel")

        else:
            _sig("NEUTRAL","◉","NS valley — mid-curve discount",
                 f"β₂ = {beta2:.2f} (negative curvature)  τ={tau:.1f}mo",
                 f"Negative β₂ creates a valley in the forward curve around maturity {tau:.0f}M. "
                 f"Mid-curve contracts are cheap relative to wings. "
                 f"Buy mid-curve, sell wings (reverse butterfly).","Nelson-Siegel")

    # 4c. Fast vs slow mean-reversion
    if tau < 3 and abs(basis) > 5:
        _sig("NEUTRAL","◉","Fast mean-reversion — short-dated signal",
             f"τ = {tau:.1f} months (fast decay)  |  basis = {basis:+.1f}%",
             f"Low τ means the curve converges to equilibrium quickly (~{tau:.0f} months). "
             f"Short-dated options or M{min(3,n_sv)} contracts capture the reversion.","Nelson-Siegel")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 5 — STRUCTURAL REGIME
    # ═══════════════════════════════════════════════════════════════════════════

    # 5a. Persistent deep backwardation
    if back_ratio >= 0.85 and n_total >= 4:
        _sig("BUY","▲","Persistent supply squeeze",
             f"{n_back}/{n_total} months backwardation  |  slope {slope:+.3f} {unit}/mo",
             f"Backwardation across {n_back} consecutive months signals genuine physical tightness. "
             f"Supply cannot meet current demand at spot. Forward premium on prompt delivery. "
             f"Historically resolves via price spike or demand destruction.","Structural Regime")

    elif back_ratio >= 0.60 and n_total >= 4:
        _sig("BUY","▲","Moderate backwardation — market tightening",
             f"{n_back}/{n_total} months backwardation  ({back_ratio*100:.0f}% of curve)",
             f"Majority of curve in backwardation indicates tightening fundamentals. "
             f"Physical scarcity premium being priced into prompt contracts.","Structural Regime")

    # 5b. Persistent deep contango
    elif back_ratio <= 0.15 and n_total >= 4:
        _sig("SELL","▼","Deep contango — storage overhang",
             f"{n_cont}/{n_total} months contango  |  slope {slope:+.3f} {unit}/mo",
             f"Deep contango across {n_cont} months reflects significant storage surplus. "
             f"Physical market overwhelmed — supply exceeds near-term demand. "
             f"Bearish for spot; forward curve pricing cost of carry.","Structural Regime")

    elif back_ratio <= 0.40 and n_total >= 4:
        _sig("SELL","▼","Mild contango — supply ample",
             f"{n_cont}/{n_total} months contango  ({(1-back_ratio)*100:.0f}% of curve)",
             f"Most of curve in contango reflects comfortable supply balance. "
             f"No physical scarcity premium — carry costs dominate pricing.","Structural Regime")

    # 5c. Mixed / transitional structure
    if 0.45 <= back_ratio <= 0.55 and n_total >= 4:
        _sig("NEUTRAL","◉","Transitional structure — balanced market",
             f"{n_back}/{n_total} months back  |  {n_cont}/{n_total} months contango",
             f"Near-equal split between backwardation and contango months suggests "
             f"the market is at equilibrium or in transition. Monitor for direction. "
             f"R² = {r2:.2f} — {'trend clear' if r2 > 0.9 else 'curve non-linear'}.","Structural Regime")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 6 — HEDGER SIGNALS
    # ═══════════════════════════════════════════════════════════════════════════

    # 6a. Producer hedge
    if back_ratio >= 0.70:
        hedge_target = beta0 if basis > 5 else spot * 0.97
        _sig("HEDGE","⛊","Producer hedge opportunity",
             f"Forward curve above fair value — lock in prices now",
             f"Strong backwardation offers producers an opportunity to sell forward "
             f"at currently elevated spot prices. Recommended: sell M3-M6 futures at "
             f"{m1m6/spot*100:.1f}% below spot vs {abs(m1m6):.2f} {unit} roll gain. "
             f"Fair value target: {beta0:.2f} {unit}.","Hedger Signals")

    # 6b. Consumer / buyer hedge
    if back_ratio <= 0.30 and m1m12 > spot * 0.03:
        _sig("HEDGE","⛊","Consumer hedge — lock in deferred prices",
             f"Contango curve: deferred prices {m1m6/spot*100:+.1f}% above spot",
             f"Buyers and end-consumers benefit from fixing forward prices before contango widens. "
             f"Buy M6-M12 futures at current forward prices to hedge future procurement cost. "
             f"Current forward premium: {abs(m1m12):.2f} {unit} over 12M.","Hedger Signals")

    # 6c. Collar strategy signal
    if 0.35 <= back_ratio <= 0.65:
        _sig("HEDGE","⛊","Collar strategy — balanced hedge",
             f"Mixed structure — uncertainty in both directions",
             f"Neither pure backwardation nor contango — ideal for a collar strategy. "
             f"Producers: sell upside calls at {spot*1.05:.2f} {unit}, buy put protection at "
             f"{spot*0.95:.2f} {unit}. Net cost near zero in balanced market.","Hedger Signals")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 7 — CURVE SHAPE & VOLATILITY PROXY
    # ═══════════════════════════════════════════════════════════════════════════

    # 7a. Curve steepness as implied vol proxy
    total_move_pct = abs(curve_pct)
    if total_move_pct > 15:
        _sig("WARNING","⚡","High implied volatility — steep curve",
             f"Spot-to-last = {curve_pct:+.1f}%  |  slope {slope:+.3f} {unit}/mo",
             f"Steep forward curve (±{total_move_pct:.1f}% front-to-back) implies high "
             f"market uncertainty. Options on this commodity should carry elevated premium. "
             f"R² = {r2:.3f} — {'highly linear' if r2>0.95 else 'non-linear curve'}.","Curve Shape")

    elif total_move_pct < 2 and n_total >= 6:
        _sig("NEUTRAL","─","Flat curve — low volatility regime",
             f"Spot-to-last = {curve_pct:+.1f}%  |  R² = {r2:.3f}",
             f"Very flat forward curve implies low expected volatility and balanced market. "
             f"Carry trades dominate over directional positions. "
             f"Calendar spreads offer small but low-risk returns.","Curve Shape")

    # 7b. Non-linearity detection
    if r2 < 0.80 and n_total >= 4:
        _sig("WARNING","⚡","Non-linear curve — kink detected",
             f"R² = {r2:.3f} (low linearity)  |  {n_back}B/{n_cont}C months",
             f"Low R² suggests the curve is not a simple contango or backwardation — "
             f"there may be a kink, hump, or structural break. "
             f"Investigate specific spread legs for mispricing opportunities.","Curve Shape")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 8 — ARBITRAGE
    # ═══════════════════════════════════════════════════════════════════════════

    # 8a. Reverse cash-and-carry (strong backwardation)
    if avg_cy > (rf_pct + storage_pct) * 1.5:
        implied_borrow = avg_cy - storage_pct
        _sig("ARBITRAGE","⇄","Reverse cash-and-carry",
             f"CY {avg_cy:.1f}% >> RF + storage ({rf_pct+storage_pct:.1f}%)",
             f"Convenience yield far exceeds carry cost — physical holders earn "
             f"{avg_cy - rf_pct - storage_pct:.1f}% excess return. "
             f"Implied borrowing rate against physical collateral: {implied_borrow:.1f}%. "
             f"Attractive for commodity financing trades.","Arbitrage")

    # 8b. Theoretical forward vs actual (mispricing)
    if n_sv >= 1 and beta0 > 0:
        theoretical_m6 = spot * (1 + (rf + cfg.get("storage_cost",0.06)) * 0.5)
        actual_m6_price = float(cy_df["price"].iloc[min(4, len(cy_df)-1)]) if not cy_df.empty else spot
        theo_diff = (actual_m6_price - theoretical_m6) / theoretical_m6 * 100
        if abs(theo_diff) > 5:
            direction = "above" if theo_diff > 0 else "below"
            _sig("ARBITRAGE","⇄",f"Forward mispricing vs theoretical",
                 f"M6 actual {actual_m6_price:.2f} vs theoretical {theoretical_m6:.2f} {unit} ({theo_diff:+.1f}%)",
                 f"Actual 6-month forward is {abs(theo_diff):.1f}% {direction} theoretical cost-of-carry price "
                 f"(spot × e^((RF+storage)×T) = {theoretical_m6:.2f}). "
                 f"{'Sell forward, buy spot' if theo_diff > 0 else 'Buy forward, sell spot'} to capture the gap.","Arbitrage")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 9 — TEMPORAL (J-7/J-14)
    # ═══════════════════════════════════════════════════════════════════════════

    df_7d  = kwargs.get("df_7d")
    df_14d = kwargs.get("df_14d")
    prices_now = kwargs.get("prices_now", np.array([spot]))
    spot_7d = None

    if df_7d is not None and not df_7d.empty:
        spot_7d  = float(df_7d["price"].iloc[0])
        chg_7d   = (spot - spot_7d) / spot_7d * 100 if spot_7d > 0 else 0
        prices_7d = df_7d["price"].values

        # Momentum
        if abs(chg_7d) > 3:
            _sig("BUY" if chg_7d>0 else "SELL",
                 "▲" if chg_7d>0 else "▼",
                 f"7-day momentum — {'bullish' if chg_7d>0 else 'bearish'}",
                 f"Spot 7d change: {chg_7d:+.2f}%  ({spot_7d:.2f} -> {spot:.2f} {unit})",
                 f"{'Rally' if chg_7d>0 else 'Decline'} of {abs(chg_7d):.1f}% in 7 days. "
                 f"Trend signal: {'add long near support' if chg_7d>0 else 'add short near resistance'}. "
                 f"Mean-reversion target: NS fair value {beta0:.2f} {unit}.", "Temporal")
        elif abs(chg_7d) > 1:
            _sig("NEUTRAL","◉",f"Mild 7d drift {chg_7d:+.1f}%",
                 f"Spot: {spot_7d:.2f} -> {spot:.2f} {unit}",
                 "No strong momentum. Watch for breakout.","Temporal")

        # Curve twist
        min_n = min(len(prices_now), len(prices_7d))
        if min_n >= 3:
            shifts = prices_now[:min_n] - prices_7d[:min_n]
            twist  = float(shifts[-1] - shifts[0])
            if abs(twist) > spot * 0.004:
                if twist < 0:
                    _sig("SPREAD","↕","Curve twist — front led rally",
                         f"Front shift: {shifts[0]:+.2f}  Back shift: {shifts[-1]:+.2f}  Twist: {twist:+.2f} {unit}",
                         "Front end rallied more than back — prompt supply tightening. "
                         "Spread M1-back widening. Buy front / sell back.", "Temporal")
                else:
                    _sig("SPREAD","↕","Curve twist — back moved more",
                         f"Front shift: {shifts[0]:+.2f}  Back shift: {shifts[-1]:+.2f}  Twist: {twist:+.2f} {unit}",
                         "Deferred contracts moved more than prompt — structural tightening. "
                         "Long-dated contracts expensive vs front.", "Temporal")
            elif min_n >= 4 and abs(float(np.std(shifts))) < abs(float(np.mean(shifts)))*0.20 and abs(float(np.mean(shifts))) > spot*0.003:
                _sig("NEUTRAL","◉","Parallel curve shift — macro driver",
                     f"Uniform shift of {float(np.mean(shifts)):+.2f} {unit} across all maturities",
                     "All maturities moved equally — macro/dollar driver rather than fundamentals. "
                     "Spread structure unchanged.", "Temporal")

    if df_7d is not None and df_14d is not None and not df_7d.empty and not df_14d.empty:
        spot_7d_v  = float(df_7d["price"].iloc[0])  if spot_7d is None else spot_7d
        spot_14d_v = float(df_14d["price"].iloc[0])
        chg_7d_v   = (spot - spot_7d_v)  / spot_7d_v  * 100 if spot_7d_v  > 0 else 0
        chg_14d_v  = (spot - spot_14d_v) / spot_14d_v * 100 if spot_14d_v > 0 else 0
        acceleration = chg_7d_v - (chg_14d_v - chg_7d_v)
        if abs(acceleration) > 2:
            _sig("WARNING" if abs(acceleration)>5 else "NEUTRAL",
                 "⚡" if abs(acceleration)>5 else "◉",
                 "Price acceleration" if acceleration>0 else "Price deceleration",
                 f"Wk1: {(chg_14d_v-chg_7d_v):+.1f}%  Wk2: {chg_7d_v:+.1f}%  Accel: {acceleration:+.1f}pp",
                 f"Move is {'accelerating' if acceleration>0 else 'decelerating'}. "
                 f"{'Trend continuation likely.' if abs(acceleration)>3 else 'Watch for reversal.'} "
                 f"14d total: {chg_14d_v:+.1f}%  ({spot_14d_v:.2f} -> {spot:.2f} {unit}).", "Temporal")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 10 — RISK & VOLATILITY
    # ═══════════════════════════════════════════════════════════════════════════

    if spot_7d is not None and spot_7d > 0:
        chg_7d_r   = abs((spot - spot_7d) / spot_7d * 100)
        daily_vol  = chg_7d_r / 7
        ann_vol    = daily_vol * (252 ** 0.5)
        var95_1d   = spot * daily_vol / 100 * 1.645
        var99_1d   = spot * daily_vol / 100 * 2.326
        _sig("NEUTRAL","◉","Implied volatility estimate",
             f"Ann. vol proxy: {ann_vol:.1f}%  |  VaR 95%: {var95_1d:.3f} {unit}/day  |  VaR 99%: {var99_1d:.3f} {unit}/day",
             f"Based on 7-day spot move ({chg_7d_r:.2f}%). Annualised vol: {ann_vol:.1f}%. "
             f"1-day VaR (95%): {var95_1d:.2f} {unit}. 1-day VaR (99%): {var99_1d:.2f} {unit}. "
             f"Use for position sizing — not a substitute for proper options-implied vol.", "Risk")

    # Curve non-linearity warning
    if r2 < 0.75 and n_total >= 4:
        _sig("WARNING","⚡","Non-linear curve — kink or hump detected",
             f"R² = {r2:.3f}  |  {n_back}B / {n_cont}C months  |  β₂={beta2:.3f}",
             "Low R² means the curve cannot be described by a simple slope. "
             "Possible causes: seasonal storage pattern, delivery constraint, or illiquid contracts. "
             "Treat individual spread signals with extra caution.", "Risk")

    # ═══════════════════════════════════════════════════════════════════════════
    # CATEGORY 11 — SUMMARY SCORE
    # ═══════════════════════════════════════════════════════════════════════════

    buy_n   = sum(1 for s in signals if s["level"]=="BUY")
    sell_n  = sum(1 for s in signals if s["level"]=="SELL")
    arb_n   = sum(1 for s in signals if s["level"] in ("ARBITRAGE","SPREAD"))

    if buy_n >= 3 and buy_n >= sell_n * 2:
        _sig("BUY","★","STRONG BUY — multiple aligned signals",
             f"BUY: {buy_n}  SELL: {sell_n}  SPREAD/ARB: {arb_n}  Total: {len(signals)}",
             f"Multiple independent indicators are aligned bullish. "
             f"Convenience yield, structural regime, roll carry, and NS fair value all point long. "
             f"High-conviction directional bias.", "Summary")

    elif sell_n >= 3 and sell_n >= buy_n * 2:
        _sig("SELL","★","STRONG SELL — multiple aligned signals",
             f"SELL: {sell_n}  BUY: {buy_n}  SPREAD/ARB: {arb_n}  Total: {len(signals)}",
             f"Multiple independent indicators are aligned bearish. "
             f"Negative carry, deep contango, roll drag all point short. "
             f"High-conviction directional bias.", "Summary")

    # ═══════════════════════════════════════════════════════════════════════════
    # DEFAULT if no signals generated
    # ═══════════════════════════════════════════════════════════════════════════
    if not signals:
        _sig("NEUTRAL","─","No strong signal — balanced market",
             f"CY {avg_cy:+.1f}%  |  roll yield {avg_ry:+.1f}%  |  slope {slope:+.3f}  |  b0={beta0:.2f}",
             f"No actionable signal detected. Market is balanced with no significant "
             f"divergence from fair value, carry, or structural norms.","Overview")

    return signals

def _is_streamlit() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except Exception:
        return False


def run_streamlit_app() -> None:
    """Streamlit interactive dashboard. Run with: streamlit run cfcap.py"""
    import streamlit as st
    import plotly.graph_objects as go
    from scipy.interpolate import CubicSpline as _CS

    AMBER, BLUE, GREEN = "#F0A500", "#58A6FF", "#3FB950"
    RED,   GRAY, PURPLE = "#FF7B72", "#8B949E", "#BC8CFF"

    # Sidebar
    with st.sidebar:
        st.markdown("## CFCAP")
        st.markdown("*Commodity Forward Curve Analytics Platform*")
        st.markdown("---")
        family    = st.selectbox("Asset class", list(COMMODITY_REGISTRY.keys()))
        commodity = st.selectbox("Commodity", list(COMMODITY_REGISTRY[family].keys()))
        cfg       = COMMODITY_REGISTRY[family][commodity].copy()
        st.markdown("---")
        # Risk-free rate: precise number input + central bank presets
        if "rf_val" not in st.session_state:
            st.session_state["rf_val"] = 5.0

        rf_pct = st.number_input(
            "Risk-free rate (%)",
            min_value=0.0,
            max_value=20.0,
            value=st.session_state["rf_val"],
            step=0.01,
            format="%.2f",
            key="rf_input",
            help="Type any value (e.g. 4.37) or use arrows (step: 0.01%)"
        )
        st.session_state["rf_val"] = rf_pct

        rf = rf_pct / 100
        n_mo      = st.number_input("Months forward", 2, 36, int(cfg["liquid_months"]), 1)
        cfg["liquid_months"] = n_mo
        st.markdown("---")
        src_lbl   = "Yahoo Finance" if cfg["source"] == "yahoo" else f"TradingView ({cfg.get('tv_exchange','')})"
        src_col   = "#3FB950" if cfg["source"] == "yahoo" else "#F0A500"
        st.markdown(f'<span style="color:{src_col};font-weight:500">{src_lbl}</span>', unsafe_allow_html=True)
        st.markdown(f"Unit: **{cfg['unit']}**")
        st.markdown("---")
        tv_user   = st.text_input("TradingView username", value=TV_USERNAME)
        tv_pass_i = st.text_input("TradingView password",  value=TV_PASSWORD, type="password")
        eia_key   = st.text_input("EIA API key (optional)", value=EIA_API_KEY, type="password",
                                   help="Free at eia.gov/opendata")
        run_btn   = st.button("Run Analysis", type="primary", use_container_width=True)

    src_badge = '<span style="background:#1C2128;border:0.5px solid #30363D;border-radius:4px;padding:2px 8px;font-size:0.72rem;color:#8B949E;font-family:JetBrains Mono,monospace">' + src_lbl + '</span>'
    unit_badge = '<span style="background:#1C2128;border:0.5px solid #30363D;border-radius:4px;padding:2px 8px;font-size:0.72rem;color:#8B949E;font-family:JetBrains Mono,monospace">' + cfg['unit'] + '</span>'
    rf_badge   = '<span style="background:#1C2128;border:0.5px solid #30363D;border-radius:4px;padding:2px 8px;font-size:0.72rem;color:#8B949E;font-family:JetBrains Mono,monospace">RF ' + f'{rf_pct:.2f}%' + '</span>'
    fam_badge  = '<span style="background:#1C2128;border:0.5px solid #30363D;border-radius:4px;padding:2px 8px;font-size:0.72rem;color:#8B949E;font-family:JetBrains Mono,monospace">' + family + '</span>'
    ts_badge   = '<span style="background:#1C2128;border:0.5px solid #30363D;border-radius:4px;padding:2px 8px;font-size:0.72rem;color:#8B949E;font-family:JetBrains Mono,monospace">' + datetime.now().strftime('%d %b %Y  %H:%M') + '</span>'
    st.markdown(
        f'<h1 style="font-family:Inter,sans-serif;font-size:1.7rem;font-weight:600;letter-spacing:-0.02em;margin-bottom:8px">{commodity}</h1>'
        f'<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:4px">{fam_badge} {unit_badge} {src_badge} {rf_badge} {ts_badge}</div>',
        unsafe_allow_html=True
    )

    for k in ["df","az","snap","combo"]:
        if k not in st.session_state: st.session_state[k] = None

    combo = (commodity, family, rf, n_mo)

    @st.cache_data(ttl=300, show_spinner=False)
    def _fetch(cn, fn, rf_, u, p, h):
        c = COMMODITY_REGISTRY[fn][cn].copy()
        return get_forward_curve(c, rf_, u, p)

    if run_btn or st.session_state["combo"] != combo:
        with st.spinner(f"Loading {commodity}..."):
            try:
                df   = _fetch(commodity, family, rf, tv_user, tv_pass_i, str(cfg))
                save_curve(df, commodity)
                snap = _load_snaps(commodity, df)
                az   = ForwardCurveAnalyzer(df, cfg, r=rf)
                st.session_state.update({"df":df,"az":az,"snap":snap,"combo":combo})
            except Exception as e:
                st.error(f"Error: {e}"); st.stop()

    df   = st.session_state["df"]
    az   = st.session_state["az"]
    snap = st.session_state["snap"]

    if df is None:
        st.info("Click Run Analysis to start."); st.stop()

    unit    = cfg["unit"]
    spot    = az.spot
    struct  = az.market_structure()
    cy_df   = az.convenience_yield()
    spreads = az.calendar_spreads()
    ns      = az.nelson_siegel_fit()
    labels  = df["label"].tolist()
    prices  = df["price"].values
    x       = df["months_to_mat"].values

    avg_cy = cy_df["convenience_yield"].head(6).mean() if not cy_df.empty else 0
    m1m3   = spreads.iloc[:2]["spread"].sum() if len(spreads) >= 2 else 0

    # ── Smart number formatter: max 5 significant digits, no scientific notation
    def _fmt(v, decimals=2):
        if v is None or (isinstance(v, float) and (v != v)):  # nan check
            return "N/A"
        if abs(v) >= 10000:  return f"{v:,.0f}"
        if abs(v) >= 1000:   return f"{v:,.1f}"
        if abs(v) >= 100:    return f"{v:.2f}"
        if abs(v) >= 10:     return f"{v:.3f}"
        if abs(v) >= 1:      return f"{v:.{min(decimals,4)}f}"
        return f"{v:.{min(decimals,5)}f}"

    slope    = struct["slope_per_month"]
    beta0    = ns.get("beta0", spot)
    rmse     = ns.get("rmse", 0)
    struct_c = "#3FB950" if struct["structure"] == "BACKWARDATION" else "#FF7B72"

    kpi_html = f"""
    <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:20px">
      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:14px 16px">
        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Spot M1</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:#E6EDF3;white-space:nowrap">{_fmt(spot)} {unit}</div>
        <div style="font-size:0.72rem;color:#8B949E;margin-top:4px">front-month price</div>
      </div>
      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:14px 16px">
        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Structure</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:{struct_c};white-space:nowrap">{struct["structure"]}</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:#8B949E;margin-top:4px">{slope:+.3f} {unit}/mo</div>
      </div>
      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:14px 16px">
        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Avg CY 6M</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:#E6EDF3;white-space:nowrap">{avg_cy:+.2f}%</div>
        <div style="font-size:0.72rem;color:{'#3FB950' if avg_cy > rf*100 else '#FF7B72'};margin-top:4px">{'above' if avg_cy > rf*100 else 'below'} risk-free</div>
      </div>
      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:14px 16px">
        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">M1-M3 Spread</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:#E6EDF3;white-space:nowrap">{_fmt(m1m3)} {unit}</div>
        <div style="font-size:0.72rem;color:#8B949E;margin-top:4px">2-month roll cost</div>
      </div>
      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:14px 16px">
        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Beta0 LT</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:#E6EDF3;white-space:nowrap">{_fmt(beta0)} {unit}</div>
        <div style="font-size:0.72rem;color:#8B949E;margin-top:4px">NS long-term level</div>
      </div>
      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:14px 16px">
        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">NS RMSE</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:#E6EDF3;white-space:nowrap">{_fmt(rmse, 4)} {unit}</div>
        <div style="font-size:0.72rem;color:#8B949E;margin-top:4px">{ns.get("model","Nelson-Siegel")}</div>
      </div>
    </div>
    """
    st.markdown(kpi_html, unsafe_allow_html=True)
    st.markdown("---")

    t1,t2,t3,t4,t5,t6 = st.tabs(["📉 Forward Curve","📊 Calendar Spreads","🔄 Convenience Yield","📐 Nelson-Siegel","⚡ Trading Signals","🕐 Historical Comparison"])

    with t1:
        # ── Historical date selector ───────────────────────────────────────────
        _avail_all = list_available_dates(commodity)
        _col_ctrl1, _col_ctrl2, _col_ctrl3 = st.columns([2, 2, 3])

        with _col_ctrl1:
            _compare_mode = st.selectbox(
                "Compare with",
                ["7d ago / 14d ago", "Custom date", "None"],
                key="compare_mode",
                label_visibility="visible",
            )
        _df_custom = None
        _custom_label = ""
        with _col_ctrl2:
            if _compare_mode == "Custom date" and _avail_all:
                _date_opts = _avail_all  # list of "YYYY-MM-DD" strings
                _sel_date  = st.selectbox("Select date", _date_opts, key="sel_date")
                _df_custom = load_curve(commodity, 
                                        datetime.strptime(_sel_date, "%Y-%m-%d"))
                _custom_label = _sel_date
            elif _compare_mode == "Custom date":
                st.caption("No saved snapshots yet.")
        with _col_ctrl3:
            if _avail_all:
                st.caption(f"**{len(_avail_all)}** snapshots available  "
                           f"({_avail_all[-1]} → {_avail_all[0]})")

        # ── Build chart ────────────────────────────────────────────────────────
        f1 = go.Figure()
        if len(x) >= 4:
            cs = _CS(x, prices)
            xf = np.linspace(x[0],x[-1],200)
            f1.add_trace(go.Scatter(x=xf,y=cs(xf),mode="lines",name="Today (spline)",
                line=dict(color=AMBER,width=2.5)))
        f1.add_trace(go.Scatter(x=x,y=prices,mode="markers",name="Observed",
            marker=dict(color=AMBER,size=8),
            customdata=labels,hovertemplate="%{customdata}<br>%{y:.3f}<extra></extra>"))

        if _compare_mode == "7d ago / 14d ago":
            f1.add_trace(go.Scatter(x=snap["7d"]["months_to_mat"],y=snap["7d"]["price"],
                mode="lines",name="7d ago",line=dict(color=BLUE,width=1.5,dash="dash")))
            f1.add_trace(go.Scatter(x=snap["14d"]["months_to_mat"],y=snap["14d"]["price"],
                mode="lines",name="14d ago",line=dict(color=GRAY,width=1.2,dash="dot")))
        elif _compare_mode == "Custom date" and _df_custom is not None:
            f1.add_trace(go.Scatter(
                x=_df_custom["months_to_mat"], y=_df_custom["price"],
                mode="lines+markers", name=f"Snapshot {_custom_label}",
                line=dict(color="#BC8CFF",width=1.8,dash="dashdot"),
                marker=dict(color="#BC8CFF",size=5),
            ))

        f1.add_hline(y=spot,line=dict(color=GREEN,width=1,dash="dashdot"),
            annotation_text=f"Spot {spot:.2f}",annotation_position="right")
        f1.update_layout(template="plotly_dark",height=420,
            xaxis=dict(title="Maturity (months)",tickvals=list(x[::2]),ticktext=labels[::2]),
            yaxis=dict(title=unit),
            legend=dict(orientation="h",yanchor="bottom",y=1.02),
            margin=dict(l=60,r=100,t=40,b=60),hovermode="x unified")
        st.plotly_chart(f1,use_container_width=True)
        avail = _avail_all  # already fetched above

        # ── Export PNG button ──────────────────────────────────────────────
        _col_exp, _col_info = st.columns([1, 4])
        with _col_exp:
            if st.button("Export PNG dashboard", use_container_width=True):
                import io as _io
                _slug2    = commodity_slug(commodity)
                _png_dir2 = DASHBOARDS_DIR / _slug2          # absolute path
                _png_dir2.mkdir(parents=True, exist_ok=True)
                _png_path2 = _png_dir2 / datetime.now().strftime(f"{_slug2}_%Y%m%d_%H%M%S.png")
                try:
                    import matplotlib
                    matplotlib.use("Agg")   # non-interactive backend for Streamlit
                    plot_dashboard(df, snap["7d"], snap["14d"], az, save_path=str(_png_path2))
                    with open(_png_path2, "rb") as _pf:
                        _png_bytes = _pf.read()
                    st.download_button(
                        label="Download PNG",
                        data=_png_bytes,
                        file_name=_png_path2.name,
                        mime="image/png",
                        use_container_width=True,
                    )
                    st.success(f"Saved: {_png_path2}")
                except Exception as _e:
                    st.error(f"Export failed: {_e}")
        with _col_info:
            st.caption(
                f"PNG saved in `data/dashboards/{commodity_slug(commodity)}/`"
            )
        st.caption(f"CSV snapshots: **{len(avail)}** saved" if avail else "No CSV snapshots yet — run `python cfcap.py --schedule`")

    with t2:
        sv = spreads["spread"].values
        f2 = go.Figure(go.Bar(x=spreads["leg_near"],y=sv,
            marker_color=[GREEN if v<0 else RED for v in sv],
            text=[f"{v:+.3f}" for v in sv],textposition="outside",
            customdata=spreads["leg_far"],
            hovertemplate="%{x}→%{customdata} %{y:+.3f}<extra></extra>"))
        f2.add_hline(y=0,line=dict(color=GRAY,width=1))
        f2.update_layout(template="plotly_dark",height=380,
            xaxis=dict(title="Contract"),yaxis=dict(title=f"Spread ({unit})"),
            margin=dict(l=60,r=20,t=30,b=60))
        st.plotly_chart(f2,use_container_width=True)
        st.dataframe(spreads,use_container_width=True,height=200)

    with t3:
        if not cy_df.empty:
            f3 = go.Figure()
            f3.add_trace(go.Scatter(x=cy_df["months_to_mat"],y=cy_df["convenience_yield"],
                mode="lines+markers",name="Conv. Yield",line=dict(color=AMBER,width=2),
                fill="tozeroy",fillcolor="rgba(240,165,0,0.12)"))
            f3.add_trace(go.Scatter(x=cy_df["months_to_mat"],y=cy_df["roll_yield"],
                mode="lines",name="Roll Yield",line=dict(color=PURPLE,width=1.5,dash="dot")))
            f3.add_hline(y=rf*100,line=dict(color=BLUE,dash="dash"),annotation_text=f"RF {rf*100:.1f}%")
            f3.add_hline(y=0,line=dict(color=GRAY,width=0.8))
            f3.update_layout(template="plotly_dark",height=380,
                xaxis=dict(title="Maturity (months)"),yaxis=dict(title="Yield (%)"),
                legend=dict(orientation="h",yanchor="bottom",y=1.02),
                margin=dict(l=60,r=20,t=40,b=60))
            st.plotly_chart(f3,use_container_width=True)
            st.dataframe(cy_df,use_container_width=True,height=200)
        else:
            st.warning("Not enough contracts.")

    with t4:
        ca,cb = st.columns([2,1])
        with ca:
            f4 = go.Figure()
            f4.add_trace(go.Scatter(x=labels,y=prices,mode="markers",name="Observed",marker=dict(color=AMBER,size=10)))
            if "fitted" in ns:
                f4.add_trace(go.Scatter(x=labels,y=ns["fitted"],mode="lines",
                    name=f"NS fit (RMSE={ns['rmse']:.3f})",line=dict(color=BLUE,width=2)))
            if "beta0" in ns:
                f4.add_hline(y=ns["beta0"],line=dict(color=GREEN,dash="dash",width=1),
                    annotation_text=f"beta0={ns['beta0']:.2f} LT fair value")
            f4.update_layout(template="plotly_dark",height=360,
                xaxis=dict(title="Contract"),yaxis=dict(title=unit),
                legend=dict(orientation="h",yanchor="bottom",y=1.02),
                margin=dict(l=60,r=20,t=40,b=60))
            st.plotly_chart(f4,use_container_width=True)
        with cb:
            st.markdown("**NS Parameters**")
            if "error" not in ns:
                for k,v in {"beta0 (LT)":f"{ns['beta0']:.3f} {unit}",
                             "beta1 (slope)":f"{ns['beta1']:+.3f}",
                             "beta2 (curv.)":f"{ns['beta2']:+.3f}",
                             "tau (decay)":f"{ns['tau']:.1f} mo",
                             "RMSE":f"{ns['rmse']:.4f}",
                             "Model":ns.get("model","NS")}.items():
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;padding:3px 0;'
                        f'border-bottom:0.5px solid #30363D">'
                        f'<span style="color:#8B949E;font-size:12px">{k}</span>'
                        f'<span style="font-family:monospace;font-size:12px">{v}</span></div>',
                        unsafe_allow_html=True)
            else:
                st.error(f"Fit failed: {ns['error']}")


    # ══════════════════════════════════════════════════════════════════════
    # TAB 5 — Trading Signals
    # ══════════════════════════════════════════════════════════════════════
    with t5:
        def _generate_signals(struct_, cy_df_, spreads_, ns_, rf_, spot_, unit_, az_):
            """
            Rule-based trading signal engine.
            Returns list of dicts: {level, category, signal, rationale, action}
            level: "BUY" | "SELL" | "HEDGE" | "WATCH" | "NEUTRAL"
            """
            sigs = []
            avg_cy6  = cy_df_["convenience_yield"].head(6).mean() if not cy_df_.empty else 0
            slope    = struct_["slope_per_month"]
            n_back   = (spreads_["spread"].values < 0).sum() if not spreads_.empty else 0
            n_total  = len(spreads_)
            roll6    = spreads_["spread"].values[:5].sum() if len(spreads_) >= 5 else 0
            beta0_   = ns_.get("beta0", spot_)
            basis_pct= (spot_ - beta0_) / beta0_ * 100 if beta0_ > 0 else 0

            rf_pct   = rf_ * 100
            two_rf   = rf_pct * 2

            # ── Signal 1: Storage / Physical buy ────────────────────────
            if avg_cy6 > two_rf:
                sigs.append(dict(
                    level="BUY", category="Physical Storage",
                    signal=f"Convenience yield {avg_cy6:.1f}% >> 2×RF ({two_rf:.1f}%)",
                    rationale="Holding physical inventory earns more than the cost of financing. "
                              "Market is pricing extreme scarcity of immediate supply.",
                    action=f"Buy spot / M1. Sell M3-M6 to lock the carry. "
                           f"Roll cost: {roll6:+.2f} {unit_} over 5 months."
                ))
            elif avg_cy6 > rf_pct:
                sigs.append(dict(
                    level="WATCH", category="Physical Storage",
                    signal=f"Convenience yield {avg_cy6:.1f}% > RF ({rf_pct:.1f}%)",
                    rationale="Mild incentive to hold physical. Monitor — if CY exceeds 2×RF, storage becomes strongly attractive.",
                    action="Monitor weekly. Set alert at CY = 2×RF."
                ))

            # ── Signal 2: Roll yield harvest ─────────────────────────────
            if n_back >= n_total * 0.6 and roll6 < 0:
                sigs.append(dict(
                    level="BUY", category="Roll Yield",
                    signal=f"Strong backwardation: {n_back}/{n_total} months negative spread",
                    rationale=f"Rolling a long position generates positive P&L of {abs(roll6):.2f} {unit_} "
                              f"over 5 months (annualised: ~{abs(roll6)*2.4:.1f} {unit_}/year). "
                              "Classic 'roll yield harvest' strategy.",
                    action=f"Long M1, short M6. Harvest {abs(roll6):.2f} {unit_} roll gain on each roll cycle."
                ))

            # ── Signal 3: Contango — avoid unhedged long ─────────────────
            if slope > 0 and avg_cy6 < 0:
                sigs.append(dict(
                    level="SELL", category="Carry Warning",
                    signal=f"Contango + negative CY ({avg_cy6:.1f}%)",
                    rationale="Rolling a long futures position costs money (negative roll yield). "
                              "An unhedged long loses to the roll even if spot stays flat.",
                    action="Avoid unhedged long. If bullish on spot, use options (long call) instead of futures roll."
                ))

            # ── Signal 4: Mean reversion — basis vs β₀ ───────────────────
            if abs(basis_pct) > 15:
                direction = "above" if basis_pct > 0 else "below"
                level_    = "SELL" if basis_pct > 0 else "BUY"
                sigs.append(dict(
                    level=level_, category="Mean Reversion (NS)",
                    signal=f"Spot {basis_pct:+.1f}% {direction} NS long-term fair value ({beta0_:.2f} {unit_})",
                    rationale=f"Nelson-Siegel β₀ = {beta0_:.2f} {unit_} is the estimated long-run equilibrium. "
                              f"Spot at {spot_:.2f} represents a {abs(basis_pct):.0f}% {'premium' if basis_pct>0 else 'discount'}. "
                              "Historical mean-reversion suggests eventual convergence.",
                    action=f"{'Fade the rally — initiate short or buy put.' if basis_pct > 0 else 'Buy the dip — spot below fair value.'}"
                ))

            # ── Signal 5: Curve flattening / structure change ─────────────
            if 0 < abs(slope) < 0.05 * spot_ / 100:
                sigs.append(dict(
                    level="WATCH", category="Structure Transition",
                    signal=f"Curve near-flat (slope {slope:+.3f} {unit_}/month)",
                    rationale="A near-flat curve often precedes a structure change. "
                              "Watch for catalyst (inventory report, OPEC, weather) to trigger direction.",
                    action="Prepare both scenarios. Pre-position straddle or wait for breakout confirmation."
                ))

            # ── Signal 6: Deep backwardation — hedging opportunity ────────
            if slope < -0.5 * spot_ / 100 and n_back >= n_total * 0.8:
                sigs.append(dict(
                    level="HEDGE", category="Producer Hedge",
                    signal=f"Deep backwardation: slope {slope:+.3f} {unit_}/month, {n_back}/{n_total} months",
                    rationale="Futures significantly below spot. "
                              "Producers can lock in near-spot prices for forward delivery — rare opportunity.",
                    action="Sell M3-M12 futures to lock current elevated prices. Ideal for producers and short-term hedgers."
                ))

            if not sigs:
                sigs.append(dict(
                    level="NEUTRAL", category="No Clear Signal",
                    signal="Curve within normal parameters",
                    rationale="No actionable signal detected. Market structure is balanced with no extreme convenience yield, roll cost, or basis deviation.",
                    action="Continue monitoring. Revisit after next EIA/OPEC/fundamental release."
                ))

            return sigs

        signals = _generate_signals(struct, cy_df, spreads, ns, rf, spot, unit, az)

        # ── Signal level colors & icons ──────────────────────────────────
        _level_style = {
            "BUY":     ("#3FB950", "#0D2818", "BUY"),
            "SELL":    ("#FF7B72", "#2D0F0E", "SELL"),
            "HEDGE":   ("#F0A500", "#2D1E00", "HEDGE"),
            "WATCH":   ("#58A6FF", "#0C1D30", "WATCH"),
            "NEUTRAL": ("#8B949E", "#161B22", "—"),
        }

        # ── Render signal cards ──────────────────────────────────────────
        sig_html = '<div style="display:flex;flex-direction:column;gap:10px">'
        for sg in signals:
            col, bg, icon = _level_style.get(sg["level"], ("#8B949E","#161B22","?"))
            sig_html += f"""
            <div style="background:#161B22;border:0.5px solid #30363D;border-left:3px solid {col};
                        border-radius:0 10px 10px 0;padding:14px 18px">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
                <span style="background:{col};color:{bg};font-size:0.65rem;font-weight:700;
                             padding:3px 10px;border-radius:4px;letter-spacing:.08em">{sg["level"]}</span>
                <span style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;
                             letter-spacing:.06em">{sg["category"]}</span>
              </div>
              <div style="font-family:'JetBrains Mono',monospace;font-size:0.88rem;font-weight:500;
                          color:#E6EDF3;margin-bottom:6px">{sg["signal"]}</div>
              <div style="font-size:0.78rem;color:#8B949E;line-height:1.6;margin-bottom:8px">{sg["rationale"]}</div>
              <div style="background:#0D1117;border:0.5px solid #30363D;border-radius:6px;
                          padding:8px 12px;font-family:'JetBrains Mono',monospace;font-size:0.78rem;
                          color:{col}">ACTION: {sg["action"]}</div>
            </div>"""
        sig_html += '</div>'

        # ── Header with disclaimer ───────────────────────────────────────
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
          <div>
            <div style="font-size:1.0rem;font-weight:600;font-family:Inter,sans-serif">{commodity} — Trading Signals</div>
            <div style="font-size:0.72rem;color:#8B949E;margin-top:2px">
              Rule-based signals derived from forward curve structure, convenience yield and Nelson-Siegel fair value.
              Not financial advice.
            </div>
          </div>
          <div style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;color:#8B949E;text-align:right">
            RF = {rf*100:.2f}% &nbsp;|&nbsp; {len(signals)} signal{"s" if len(signals)!=1 else ""}
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(sig_html, unsafe_allow_html=True)

        # ── Summary table ────────────────────────────────────────────────
        st.markdown('<div style="margin-top:14px"></div>', unsafe_allow_html=True)
        _sig_df = pd.DataFrame([{
            "Level":     s["level"],
            "Category":  s["category"],
            "Signal":    s["signal"],
            "Action":    s["action"],
        } for s in signals])
        st.dataframe(_sig_df, use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 6 — Historical Comparison
    # ══════════════════════════════════════════════════════════════════════
    with t6:
        _slug_hist = commodity_slug(commodity)
        _avail_dates = list_available_dates(_slug_hist)

        if not _avail_dates:
            st.info(
                "No historical snapshots yet.  "
                "Run `python cfcap.py --schedule` once a day to build your history.  "
                "After 7+ days you can compare any two dates here."
            )
        else:
            st.markdown(
                f'<div style="font-size:0.72rem;color:#8B949E;margin-bottom:12px">'
                f'{len(_avail_dates)} snapshots available — '
                f'oldest: {_avail_dates[-1]} &nbsp;|&nbsp; latest: {_avail_dates[0]}'
                f'</div>', unsafe_allow_html=True
            )

            # ── Date pickers ─────────────────────────────────────────────
            _hc1, _hc2, _hc3 = st.columns([2,2,1])
            with _hc1:
                _date_a = st.selectbox(
                    "Reference date (A)",
                    options=_avail_dates,
                    index=0,
                    key="hist_date_a",
                    help="Most recent snapshot by default"
                )
            with _hc2:
                _default_b_idx = min(6, len(_avail_dates)-1)
                _date_b = st.selectbox(
                    "Comparison date (B)",
                    options=_avail_dates,
                    index=_default_b_idx,
                    key="hist_date_b",
                    help="7 days ago by default"
                )
            with _hc3:
                st.markdown('<div style="margin-top:28px"></div>', unsafe_allow_html=True)
                _show_diff = st.checkbox("Show difference", value=True)

            if _date_a == _date_b:
                st.warning("Select two different dates to compare.")
            else:
                _df_a = load_curve(commodity, datetime.strptime(_date_a, "%Y-%m-%d"))
                _df_b = load_curve(commodity, datetime.strptime(_date_b, "%Y-%m-%d"))

                if _df_a is None or _df_b is None:
                    st.error("Could not load one or both snapshots.")
                else:
                    _df_a = _df_a.reset_index(drop=True)
                    _df_b = _df_b.reset_index(drop=True)
                    _xa   = _df_a["months_to_mat"].values
                    _fa   = _df_a["price"].values
                    _xb   = _df_b["months_to_mat"].values
                    _fb   = _df_b["price"].values
                    _la   = _df_a["label"].tolist()

                    # ── KPI change cards ─────────────────────────────────
                    _spot_a   = _fa[0]
                    _spot_b   = _fb[0] if len(_fb) > 0 else _fa[0]
                    _spot_chg = _spot_a - _spot_b
                    _spot_pct = _spot_chg / _spot_b * 100 if _spot_b != 0 else 0
                    _slope_a  = float(np.polyfit(_xa, _fa, 1)[0])
                    _slope_b  = float(np.polyfit(_xb, _fb, 1)[0])
                    _struct_a = "BACKWARDATION" if _slope_a < 0 else "CONTANGO"
                    _struct_b = "BACKWARDATION" if _slope_b < 0 else "CONTANGO"
                    _m1m3_a   = (_fa[2] - _fa[0]) if len(_fa) >= 3 else 0
                    _m1m3_b   = (_fb[2] - _fb[0]) if len(_fb) >= 3 else 0

                    def _chg_color(v): return "#3FB950" if v > 0 else "#FF7B72" if v < 0 else "#8B949E"

                    _kpi_h = f"""<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px">
                      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:12px 14px">
                        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px">Spot M1 — A</div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:#F0A500">{_spot_a:.2f} {unit}</div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:{_chg_color(_spot_chg)};margin-top:3px">{_spot_chg:+.2f} ({_spot_pct:+.1f}%) vs B</div>
                      </div>
                      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:12px 14px">
                        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px">Spot M1 — B</div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:#58A6FF">{_spot_b:.2f} {unit}</div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:#8B949E;margin-top:3px">{_date_b}</div>
                      </div>
                      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:12px 14px">
                        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px">Structure change</div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:0.88rem;font-weight:500;color:#E6EDF3">{_struct_b} → {_struct_a}</div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:#8B949E;margin-top:3px">slope: {_slope_b:+.3f} → {_slope_a:+.3f}</div>
                      </div>
                      <div style="background:#161B22;border:0.5px solid #30363D;border-radius:10px;padding:12px 14px">
                        <div style="font-size:0.68rem;font-weight:500;color:#8B949E;text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px">M1-M3 spread</div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:1.0rem;font-weight:500;color:#E6EDF3">{_m1m3_a:+.3f} {unit}</div>
                        <div style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:{_chg_color(_m1m3_a-_m1m3_b)};margin-top:3px">{_m1m3_a-_m1m3_b:+.3f} vs B</div>
                      </div>
                    </div>"""
                    st.markdown(_kpi_h, unsafe_allow_html=True)

                    # ── Comparison chart ─────────────────────────────────
                    _fh = go.Figure()

                    if len(_xa) >= 4:
                        _cs_a = _CS(_xa, _fa)
                        _xf   = np.linspace(_xa[0], _xa[-1], 200)
                        _fh.add_trace(go.Scatter(
                            x=_xf, y=_cs_a(_xf), mode="lines",
                            name=f"A — {_date_a} (spline)",
                            line=dict(color=AMBER, width=2.5)
                        ))

                    _fh.add_trace(go.Scatter(
                        x=_xa, y=_fa, mode="markers",
                        name=f"A — {_date_a}",
                        marker=dict(color=AMBER, size=8),
                        customdata=_la,
                        hovertemplate="%{customdata}<br>%{y:.3f} " + unit + "<extra></extra>"
                    ))

                    if len(_xb) >= 4:
                        _cs_b = _CS(_xb, _fb)
                        _xfb  = np.linspace(_xb[0], _xb[-1], 200)
                        _fh.add_trace(go.Scatter(
                            x=_xfb, y=_cs_b(_xfb), mode="lines",
                            name=f"B — {_date_b} (spline)",
                            line=dict(color=BLUE, width=2, dash="dash")
                        ))

                    _fh.add_trace(go.Scatter(
                        x=_xb, y=_fb, mode="markers",
                        name=f"B — {_date_b}",
                        marker=dict(color=BLUE, size=7, symbol="diamond"),
                        hovertemplate="%{y:.3f} " + unit + "<extra></extra>"
                    ))

                    # Difference shading
                    if _show_diff and len(_xa) == len(_xb):
                        _diff = _fa - _fb
                        _fh.add_trace(go.Bar(
                            x=_xa, y=_diff,
                            name="A minus B",
                            marker_color=[AMBER if v > 0 else BLUE for v in _diff],
                            opacity=0.35,
                            yaxis="y2",
                            hovertemplate="M%{x}: %{y:+.3f} " + unit + "<extra>Diff A-B</extra>"
                        ))

                    _fh.update_layout(
                        template="plotly_dark", height=420,
                        xaxis=dict(title="Maturity (months)",
                                   tickvals=list(_xa[::2]),
                                   ticktext=_la[::2] if len(_la) > 0 else []),
                        yaxis=dict(title=unit),
                        yaxis2=dict(title="Difference", overlaying="y",
                                    side="right", showgrid=False) if _show_diff else {},
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        margin=dict(l=60, r=80, t=40, b=60),
                        hovermode="x unified",
                        barmode="overlay",
                    )
                    st.plotly_chart(_fh, use_container_width=True)

                    # ── Contract-by-contract table ────────────────────────
                    if len(_fa) == len(_fb):
                        _tbl = pd.DataFrame({
                            "Contract":  _la,
                            f"A ({_date_a})": [f"{p:.2f}" for p in _fa],
                            f"B ({_date_b})": [f"{p:.2f}" for p in _fb],
                            "Change":    [f"{d:+.2f}" for d in _fa - _fb],
                            "Change %":  [f"{d/b*100:+.1f}%" if b != 0 else "n/a"
                                          for d, b in zip(_fa - _fb, _fb)],
                        })
                        st.dataframe(_tbl, use_container_width=True, hide_index=True, height=220)


    st.markdown("---")
    # ── Trading signals banner ────────────────────────────────────────────────
    _signals = generate_trading_signals(
        struct, cy_df, spreads, ns, spot, rf, unit, cfg,
        df_7d=snap.get("7d"), df_14d=snap.get("14d"),
        prices_now=prices,
    )

    _lvl_colors = {
        "BUY":       ("#238636", "#3FB950", "#0D2819"),
        "SELL":      ("#DA3633", "#FF7B72", "#2D0A09"),
        "SPREAD":    ("#1F6FEB", "#58A6FF", "#051D4D"),
        "HEDGE":     ("#9E6A03", "#F0A500", "#2D1B00"),
        "ARBITRAGE": ("#6E40C9", "#BC8CFF", "#1A0F2E"),
        "NEUTRAL":   ("#444444", "#8B949E", "#1C2128"),
        "WARNING":   ("#9E2A2B", "#FF7B72", "#2D0909"),
    }

    # Filter controls
    _sig_col1, _sig_col2 = st.columns([3, 1])
    with _sig_col1:
        _all_cats = sorted(set(s["category"] for s in _signals))
        _all_lvls = sorted(set(s["level"]    for s in _signals))
        st.caption(f"{len(_signals)} signals detected across {len(_all_cats)} categories")
    with _sig_col2:
        _show_all = st.checkbox("Show all", value=True, key="sig_show_all")

    _filtered = _signals if _show_all else [s for s in _signals if s["level"] in ("BUY","SELL","ARBITRAGE")]

    # Group by category
    _cats_order = ["Summary","Temporal","Convenience Yield","Roll Yield",
                   "Calendar Spreads","Nelson-Siegel","Structural Regime",
                   "Hedger Signals","Curve Shape","Arbitrage","Risk","Overview"]
    _by_cat = {}
    for _s in _filtered:
        _c = _s.get("category","Other")
        _by_cat.setdefault(_c, []).append(_s)

    for _cat in _cats_order + [c for c in _by_cat if c not in _cats_order]:
        if _cat not in _by_cat:
            continue
        _cat_sigs = _by_cat[_cat]
        st.markdown(
            f'<div style="font-size:0.68rem;font-weight:500;color:#8B949E;'
            f'text-transform:uppercase;letter-spacing:.08em;margin:12px 0 6px">'
            f'{_cat}</div>',
            unsafe_allow_html=True
        )
        _sig_html = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px;margin-bottom:4px">'
        for _s in _cat_sigs:
            _bc, _tc, _bg = _lvl_colors.get(_s["level"], _lvl_colors["NEUTRAL"])
            _sig_html += (
                f'<div style="background:{_bg};border:0.5px solid {_bc};'
                f'border-radius:10px;padding:12px 14px">'
                f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">'
                f'<span style="font-size:0.70rem;font-weight:500;color:{_tc};'
                f'background:{_bc}33;padding:2px 8px;border-radius:20px;'
                f'letter-spacing:.05em;white-space:nowrap">'
                f'{_s["icon"]} {_s["level"]}</span>'
                f'<span style="font-size:0.82rem;font-weight:500;color:#E6EDF3;line-height:1.3">'
                f'{_s["title"]}</span></div>'
                f'<div style="font-family:JetBrains Mono,monospace;font-size:0.71rem;'
                f'color:{_tc};margin-bottom:5px;white-space:nowrap;overflow:hidden;'
                f'text-overflow:ellipsis">{_s["detail"]}</div>'
                f'<div style="font-size:0.70rem;color:#8B949E;line-height:1.55">'
                f'{_s["rationale"]}</div></div>'
            )
        _sig_html += '</div>'
        st.markdown(_sig_html, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(
        '<h3 style="font-family:Inter,sans-serif;font-size:1.1rem;font-weight:600;'
        'letter-spacing:-0.01em;margin:0 0 12px">EIA Fundamentals</h3>',
        unsafe_allow_html=True
    )
    if not eia_key:
        st.info("Add your free EIA API key in the sidebar — https://www.eia.gov/opendata/")
    else:
        with st.spinner("Fetching EIA data..."):
            try:
                client = EIAClient(eia_key)
                nm     = commodity.lower()
                if "crude" in nm or "wti" in nm or "brent" in nm:
                    keys = ["cushing_stocks","us_crude_stocks","us_crude_production",
                            "wti_spot","us_refinery_runs"]
                elif "gas" in nm:
                    keys = ["natgas_storage","natgas_spot","natgas_production"]
                elif "gasoline" in nm:
                    keys = ["gasoline_stocks","us_refinery_runs","wti_spot"]
                elif "heating" in nm or "gasoil" in nm or "diesel" in nm:
                    keys = ["distillate_stocks","us_refinery_runs","wti_spot"]
                else:
                    keys = ["wti_spot","natgas_spot"]

                data = client.fetch_all(keys)
                summ = client.fundamentals_summary(data)

                # ── EIA KPI cards (same style as main KPIs) ──────────────────
                _key_labels = {
                    "cushing_stocks":      ("Cushing Stocks",    "kbbl",       True),
                    "us_crude_stocks":     ("US Crude Stocks",   "kbbl",       True),
                    "us_crude_production": ("US Production",     "kbbl/day",   False),
                    "wti_spot":            ("WTI Spot",          "$/bbl",      False),
                    "brent_spot":          ("Brent Spot",        "$/bbl",      False),
                    "us_refinery_runs":    ("Refinery Runs",     "%",          False),
                    "gasoline_stocks":     ("Gasoline Stocks",   "kbbl",       True),
                    "distillate_stocks":   ("Distillate Stocks", "kbbl",       True),
                    "natgas_storage":      ("NG Storage",        "Bcf",        True),
                    "natgas_spot":         ("Henry Hub Spot",    "$/MMBtu",    False),
                    "natgas_production":   ("NG Production",     "Bcf/month",  False),
                }

                cards_html = '<div style="display:grid;grid-template-columns:repeat(' + str(len(keys)) + ',1fr);gap:10px;margin-bottom:16px">'
                for key in keys:
                    s = summ.get(key, {})
                    label, unit, inv = _key_labels.get(key, (key.replace("_"," ").title(), "", True))
                    if s.get("latest") is None:
                        continue
                    latest = s["latest"]
                    chg    = s.get("change", 0) or 0
                    pct    = s.get("pct_change", 0) or 0
                    date_s = s.get("date", "")

                    # Format value smartly
                    if abs(latest) >= 100000:
                        val_str = f"{latest/1000:,.0f}k"
                    elif abs(latest) >= 1000:
                        val_str = f"{latest:,.0f}"
                    elif abs(latest) >= 10:
                        val_str = f"{latest:,.1f}"
                    else:
                        val_str = f"{latest:.3f}"

                    # Direction color: inverse for stocks (high = bearish = red)
                    if inv:
                        chg_color = "#FF7B72" if chg > 0 else "#3FB950" if chg < 0 else "#8B949E"
                    else:
                        chg_color = "#3FB950" if chg > 0 else "#FF7B72" if chg < 0 else "#8B949E"

                    arrow = "▲" if chg > 0 else "▼" if chg < 0 else "─"
                    chg_fmt = f"{arrow} {abs(chg):,.0f}" if abs(chg) >= 10 else f"{arrow} {abs(chg):.2f}"

                    cards_html += (
                        f'<div style="background:#161B22;border:0.5px solid #30363D;'
                        f'border-radius:10px;padding:12px 14px">'
                        f'<div style="font-size:0.68rem;font-weight:500;color:#8B949E;'
                        f'text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px">'
                        f'{label}</div>'
                        f'<div style="font-family:JetBrains Mono,monospace;font-size:1.0rem;'
                        f'font-weight:500;color:#E6EDF3;white-space:nowrap">'
                        f'{val_str} <span style="font-size:0.72rem;color:#8B949E">{unit}</span></div>'
                        f'<div style="font-family:JetBrains Mono,monospace;font-size:0.72rem;'
                        f'color:{chg_color};margin-top:4px;white-space:nowrap">'
                        f'{chg_fmt} ({pct:+.1f}%)</div>'
                        f'<div style="font-size:0.65rem;color:#8B949E;margin-top:2px">'
                        f'week of {date_s}</div>'
                        f'</div>'
                    )
                cards_html += '</div>'
                st.markdown(cards_html, unsafe_allow_html=True)

                # ── Time series chart ────────────────────────────────────────
                _chart_keys = [
                    ("cushing_stocks",  AMBER, "Cushing crude stocks (kbbl)"),
                    ("us_crude_stocks", BLUE,  "US total crude stocks (kbbl)"),
                    ("natgas_storage",  BLUE,  "US natural gas storage (Bcf)"),
                    ("gasoline_stocks", GREEN, "US gasoline stocks (kbbl)"),
                ]
                for key, col_, title in _chart_keys:
                    if key in data and not data[key].empty:
                        dk = data[key].tail(52)
                        # Convert hex color to rgba for fill
                        r = int(col_[1:3], 16)
                        g = int(col_[3:5], 16)
                        b = int(col_[5:7], 16)
                        fill_rgba = f"rgba({r},{g},{b},0.10)"
                        fe = go.Figure(go.Scatter(
                            x=dk["date"], y=dk["value"],
                            mode="lines", name=title,
                            line=dict(color=col_, width=1.8),
                            fill="tozeroy", fillcolor=fill_rgba,
                            hovertemplate="%{x|%d %b %Y}<br>%{y:,.0f}<extra></extra>",
                        ))
                        fe.update_layout(
                            template="plotly_dark", height=220,
                            title=dict(text=title, font=dict(size=11, color="#8B949E")),
                            yaxis=dict(title="", tickformat=",.0f"),
                            xaxis=dict(title=""),
                            margin=dict(l=60, r=20, t=36, b=36),
                        )
                        st.plotly_chart(fe, use_container_width=True)
                        break

            except Exception as e:
                import traceback
                st.error(f"EIA error: {e}")

    st.markdown("---")
    st.caption(f"CFCAP  ·  {datetime.now().strftime('%d %b %Y %H:%M')}  ·  ™ by AEG")

# ── Streamlit bootstrap (module-level — required by Streamlit) ─────────────────
# Streamlit re-executes the entire file on each interaction.
# set_page_config MUST be the first st.* call and must run at module level.
try:
    import streamlit as _st_check
    _st_check.set_page_config(
        page_title="CFCAP -- Commodity Forward Curve Analytics Platform",
        page_icon="chart_with_upwards_trend",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _st_check.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif !important;
        }
        code, pre, .stCode, [class*="monospace"] {
            font-family: 'JetBrains Mono', monospace !important;
        }
        .block-container {
            padding-top: 1.5rem !important;
            padding-bottom: 2rem !important;
            max-width: 1400px;
        }
        h1 { font-size: 1.6rem !important; font-weight: 600 !important; letter-spacing: -0.02em; }
        .stCaption { color: #8B949E !important; font-size: 0.78rem !important; }

        /* KPI cards */
        div[data-testid="metric-container"] {
            background: #161B22 !important;
            border: 0.5px solid #30363D !important;
            border-radius: 10px !important;
            padding: 12px 16px !important;
        }
        div[data-testid="metric-container"] label {
            font-family: 'Inter', sans-serif !important;
            font-size: 0.72rem !important;
            font-weight: 500 !important;
            color: #8B949E !important;
            text-transform: uppercase !important;
            letter-spacing: 0.06em !important;
        }
        div[data-testid="metric-container"] [data-testid="stMetricValue"] {
            font-family: 'JetBrains Mono', monospace !important;
            font-size: 1.05rem !important;
            font-weight: 500 !important;
            color: #E6EDF3 !important;
            white-space: nowrap !important;
            overflow: visible !important;
        }
        div[data-testid="metric-container"] [data-testid="stMetricDelta"] {
            font-family: 'JetBrains Mono', monospace !important;
            font-size: 0.75rem !important;
        }

        /* Sidebar */
        [data-testid="stSidebar"] {
            background: #0D1117 !important;
            border-right: 0.5px solid #21262D !important;
        }
        [data-testid="stSidebar"] label {
            font-size: 0.75rem !important;
            font-weight: 500 !important;
            color: #8B949E !important;
            text-transform: uppercase !important;
            letter-spacing: 0.05em !important;
        }
        [data-testid="stSidebar"] .stSelectbox > div > div {
            background: #161B22 !important;
            border: 0.5px solid #30363D !important;
            border-radius: 6px !important;
            font-size: 0.85rem !important;
        }

        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            background: #0D1117 !important;
            border-bottom: 0.5px solid #21262D !important;
            gap: 4px !important;
        }
        .stTabs [data-baseweb="tab"] {
            font-family: 'Inter', sans-serif !important;
            font-size: 0.78rem !important;
            font-weight: 500 !important;
            padding: 8px 16px !important;
            color: #8B949E !important;
            border-radius: 6px 6px 0 0 !important;
            letter-spacing: 0.02em !important;
        }
        .stTabs [aria-selected="true"] {
            color: #F0A500 !important;
            border-bottom: 2px solid #F0A500 !important;
        }

        /* Dataframes */
        .stDataFrame { border: 0.5px solid #21262D !important; border-radius: 8px !important; }

        /* Buttons */
        .stButton > button {
            font-family: 'Inter', sans-serif !important;
            font-size: 0.82rem !important;
            font-weight: 500 !important;
            border-radius: 6px !important;
            letter-spacing: 0.03em !important;
        }

        /* Divider */
        hr { border-color: #21262D !important; margin: 1rem 0 !important; }
    </style>
    """, unsafe_allow_html=True)
except Exception:
    pass  # not running under Streamlit — ignore


if __name__ == "__main__" or _is_streamlit():

    if _is_streamlit():
        run_streamlit_app()

    else:
        parser = argparse.ArgumentParser(description="CFCAP — Commodity Forward Curve Analytics Platform")
        parser.add_argument("--schedule",  action="store_true")
        parser.add_argument("--commodity", type=str, default=None)
        parser.add_argument("--family",    type=str, default=None)
        parser.add_argument("--rf",        type=float, default=5.0)
        parser.add_argument("--list",      action="store_true")
        parser.add_argument("--streamlit", action="store_true",
                            help="Launch Streamlit dashboard in browser")
        args = parser.parse_args()
        rf   = args.rf / 100

        if getattr(args, "streamlit", False):
            import subprocess, sys as _sys
            print("Launching Streamlit dashboard...")
            print("Opening http://localhost:8501 in your browser.")
            print("Press Ctrl+C to stop.")
            subprocess.run([_sys.executable, "-m", "streamlit", "run", __file__],
                           check=False)
            _sys.exit(0)

        elif args.list:
            if CURVES_DIR.exists():
                for e in sorted(CURVES_DIR.iterdir()):
                    if e.is_dir():
                        d = list_available_dates(e.name)
                        print(f"  {e.name:<45} {len(d)} snapshots" + (f"  latest: {d[0]}" if d else ""))
            sys.exit(0)

        elif args.commodity and args.family:
            run_once(args.commodity, args.family, rf=rf, tv_user=TV_USERNAME, tv_pass=TV_PASSWORD)

        elif args.schedule:
            try:
                import schedule as _sched
            except ImportError:
                print("pip install schedule"); sys.exit(1)
            def _job():
                log("Batch run triggered")
                run_batch(DEFAULT_BATCH, rf=rf)
            log("Scheduler started -- running now, then daily at 09:15")
            _job()
            _sched.every().day.at("09:15").do(_job)
            while True:
                _sched.run_pending()
                time.sleep(60)

        else:
            selection = run_selector()
            cfg       = selection["cfg"]
            rf_sel    = selection["rf"]
            tv_u      = selection["tv_user"]
            tv_p      = selection["tv_pass"]
            print(f"\n  {cfg['name'].upper()} | RF={rf_sel*100:.2f}% | {cfg['liquid_months']} months")
            df_today  = get_forward_curve(cfg, rf_sel, tv_u, tv_p)
            save_curve(df_today, cfg["name"])
            snap      = _load_snaps(cfg["name"], df_today)
            analyzer  = ForwardCurveAnalyzer(df_today, cfg, r=rf_sel)
            report    = analyzer.report()
            slug      = commodity_slug(cfg["name"])
            png_dir   = DASHBOARDS_DIR / slug
            png_dir.mkdir(parents=True, exist_ok=True)
            png_path  = png_dir / datetime.now().strftime(f"{slug}_%Y%m%d_%H%M%S.png")
            plot_dashboard(df_today, snap["7d"], snap["14d"], analyzer, save_path=str(png_path))
            save_run_record(cfg["name"], cfg, report, png_path)
            print("Done.")

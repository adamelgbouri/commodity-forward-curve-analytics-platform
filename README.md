# CFCAP — Commodity Forward Curve Analytics Platform ™ by AEG

> A professional-grade commodity forward curve analyzer built in Python.  
> Covers 65+ commodities across 8 asset classes with live data, quantitative analytics, and an interactive Streamlit dashboard.

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://aeg-cfcap.streamlit.app/)

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=flat-square)
![Streamlit](https://img.shields.io/badge/Streamlit-1.32%2B-red?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

![Demo](https://jumpshare.com/share/hutVqw4cf7fjrBshksP4)
![CFCAP Screenshot 2](docs/Streamlit_Screenshot_CFCAP_2.png)

![Dashboard example](docs/wti_crude_oil_20260408_192720.png)

---

## Live Demo

**[aeg-cfcap.streamlit.app](https://aeg-cfcap.streamlit.app/)**

The app is live and free to use. No installation required.  
Source code available on request.

---

## Features

- **65+ commodities** — Energy, Metals, Agriculture, Base Metals, Freight, Carbon & Environmental
- **Dual data source routing** — Yahoo Finance (grouped download) or TradingView (tvdatafeed)
- **PCA decomposition** — Level, Slope and Curvature principal components with out-of-sample reconstruction
- **Schwartz-Smith 3-factor model** — short-term deviation, long-term equilibrium and seasonal component
- **Implied convenience yield & roll yield** — full term structure
- **Calendar spreads** — M+1−M with backwardation/contango classification
- **51 trading signals** — across 11 categories (carry, roll yield, spreads, SS fair value, temporal, hedging, arbitrage, risk)
- **EIA fundamentals** — US crude/gas inventories, production, spot prices (free API key)
- **Interactive Streamlit dashboard** — Plotly charts, historical date comparison, PNG export
- **Daily scheduler** — automated batch runs at market open with CSV persistence

---

## Data Sources

| Source | Commodities | Notes |
|--------|------------|-------|
| Yahoo Finance | WTI, Brent, NG, Gold, Silver, Copper, Grains, Softs... | Free, no credentials |
| TradingView | Jet CIF NWE, LME metals, TTF, Coal, Carbon, Freight... | Optional credentials |
| EIA Open Data | US crude/gas inventories, production, spot | Free API key at [`eia.gov/opendata`](https://eia.gov/opendata) |

---

## Commodities Covered

| Family | Commodities | Source |
|---|---|---|
| Energy | WTI, Brent, Natural Gas, RBOB, Heating Oil, Gasoil | Yahoo Finance |
| Energy+ | Jet CIF NWE, TTF, NBP, Coal API2/API4, Uranium | TradingView |
| Metals | Gold, Silver, Copper, Platinum, Palladium | Yahoo Finance |
| Base Metals | LME Copper, Aluminum, Zinc, Nickel, Lead, Tin, Cobalt | TradingView |
| Agriculture | Corn, Wheat, Soybeans, Sugar, Coffee, Cocoa, Cotton | Yahoo Finance |
| Agriculture+ | Soybean Oil/Meal, Oats, OJ, Cattle, Hogs, Lumber, Palm Oil | Yahoo Finance / TradingView |
| Freight | Capesize, Panamax, Supramax, VLCC | TradingView |
| Carbon | EU EUA, UK UKA, California CCA, RGGI | TradingView |

---

## Dependencies

| Library | Purpose |
|---|---|
| `numpy` | Numerical computations, PCA and Schwartz-Smith fitting |
| `pandas` | DataFrames, CSV persistence, historical data |
| `scipy` | Non-linear least squares (`least_squares`), spline interpolation |
| `scikit-learn` | PCA decomposition of the forward curve |
| `matplotlib` | 4-panel PNG dashboard |
| `requests` | EIA API calls |
| `yfinance` | Live futures data (Yahoo Finance) |
| `streamlit` | Interactive browser dashboard |
| `plotly` | Interactive charts in Streamlit |
| `schedule` | Daily scheduler automation |

---

## Trading Signals (51 signals across 11 categories)

- **Convenience Yield** — physical storage, cash-and-carry arbitrage, CY term structure
- **Roll Yield** — carry analysis, roll cost/gain, net carry
- **Calendar Spreads** — M1-M2, M1-M3, M1-M6, butterfly, mixed structure
- **Schwartz-Smith** — fair value mean-reversion, half-life of short-term deviation, seasonal amplitude
- **Structural Regime** — backwardation/contango depth, transitional markets
- **Temporal** — 7-day momentum, curve twist, parallel shift, price acceleration
- **Hedger Signals** — producer hedge, consumer hedge, collar strategy
- **Arbitrage** — reverse cash-and-carry, theoretical forward mispricing
- **Risk** — implied volatility proxy, VaR 95%/99%, curve non-linearity
- **Summary** — STRONG BUY/SELL when 3+ signals aligned

---

## Theory

The platform implements two quantitative models for forward curve analysis.

**PCA Decomposition** identifies the three dominant factors of curve movements:

$$\hat{F}(T) = \bar{F}(T) + \sum_{k=1}^{3} z_k \cdot \mathbf{v}_k(T)$$

Where $z_k$ are today's factor scores and $\mathbf{v}_k$ the principal component loadings. PC1 captures parallel shifts (~75% of variance), PC2 steepening/flattening (~15%), and PC3 curvature (~7%).

**Schwartz-Smith 3-Factor Model** decomposes the log futures price as:

$$\ln F(T) = \chi_0 \cdot e^{-\kappa T} + \xi_0 + A \cdot \sin(\omega T + \phi_0)$$

Where $\chi_0 e^{-\kappa T}$ is the short-term deviation (mean-reverting at speed $\kappa$), $e^{\xi_0}$ the long-term equilibrium price, and $A \sin(\omega T + \phi_0)$ a seasonal component.

The **implied convenience yield** is derived from the cost-of-carry model:

$$cy(T) = r + u - \frac{1}{T} \ln\left(\frac{F(T)}{S}\right)$$

The **roll yield** measures the annualised return from rolling a long futures position:

$$ry(T) = \frac{S - F(T)}{F(T) \cdot T}$$

See [`docs/Commodity_Forward_Curve_Analytics_Platform.pdf`](docs/Commodity_Forward_Curve_Analytics_Platform.pdf) for the full mathematical write-up.

---

## License

MIT — © 2026 Adam El Gbouri

---

*Built with Python · Yahoo Finance · TradingView · EIA Open Data*

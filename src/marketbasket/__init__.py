"""MarketBasket — refresh script for the web app.

Queries MarketUnified monthly, aggregates, writes per-state parquets to `site/data/`.
The site itself (site/index.html + app.js) is served by GitHub Pages.
"""
__version__ = "0.1.0"

#!/usr/bin/env python3
"""
MEV Dashboard — Active visualization of MEV opportunities, competition, and ROI.

Run from the mev/ directory:
    cd mev && python dashboard/app.py

Opens on http://0.0.0.0:8050
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

# Ensure we run from mev/ directory
SCRIPT_DIR = Path(__file__).parent
MEV_DIR = SCRIPT_DIR.parent
os.chdir(MEV_DIR)
sys.path.insert(0, str(SCRIPT_DIR))

import dash
from dash import dcc, html, dash_table, callback_context
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

import queries
import strategy_check
from pricing import PriceEngine

# ── App Setup ──
app = dash.Dash(
    __name__,
    title="MEV Dashboard",
    assets_folder=str(SCRIPT_DIR / "assets"),
    suppress_callback_exceptions=True,
)

# ── Color Palette ──
COLORS = {
    "bg": "#0d1117",
    "card": "#161b22",
    "border": "#30363d",
    "text": "#c9d1d9",
    "text_dim": "#8b949e",
    "green": "#3fb950",
    "yellow": "#d29922",
    "red": "#f85149",
    "blue": "#58a6ff",
    "purple": "#bc8cff",
    "cyan": "#39d353",
}

VERDICT_COLORS = {"Go": COLORS["green"], "Investigate": COLORS["yellow"], "Skip": COLORS["red"]}


def make_card(title, content, color=None):
    """Create a styled dashboard card."""
    border_color = color or COLORS["border"]
    return html.Div(
        [
            html.H4(title, style={"color": COLORS["text_dim"], "margin": "0 0 8px 0", "fontSize": "13px", "textTransform": "uppercase", "letterSpacing": "1px"}),
            html.Div(content, style={"color": COLORS["text"], "fontSize": "24px", "fontWeight": "bold"}),
        ],
        style={
            "backgroundColor": COLORS["card"],
            "borderLeft": f"4px solid {border_color}",
            "borderRadius": "6px",
            "padding": "16px 20px",
            "flex": "1",
            "minWidth": "200px",
        },
    )


def make_verdict_badge(verdict):
    color = VERDICT_COLORS.get(verdict, COLORS["text_dim"])
    return html.Span(
        verdict,
        style={
            "backgroundColor": color + "20",
            "color": color,
            "padding": "4px 12px",
            "borderRadius": "12px",
            "fontSize": "13px",
            "fontWeight": "bold",
            "border": f"1px solid {color}",
        },
    )


# ── Layout (function-based for server-side pre-rendering) ──

def serve_layout():
    """Generate layout with pre-rendered initial tab content."""
    # Pre-render the scorecard tab — use a single chain to avoid OOM
    data = get_data("ethereum")
    initial_content = render_scorecard(data) if data else html.Div(
        "Failed to load data. Check Parquet files in data/ directory.",
        style={"color": COLORS["red"], "textAlign": "center", "padding": "60px"},
    )
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    return html.Div(
        [
            # Header
            html.Div(
                [
                    html.H1("MEV DASHBOARD", style={"margin": "0", "fontSize": "20px", "letterSpacing": "3px", "color": COLORS["text"]}),
                    html.Span("Multi-Chain Capture", id="chain-label", style={"color": COLORS["text_dim"], "fontSize": "13px"}),
                    dcc.Dropdown(
                        id="chain-selector",
                        options=[{"label": "All Chains", "value": "all"}] + [
                            {"label": c.capitalize(), "value": c} for c in ["ethereum", "polygon", "blast", "base", "arbitrum"]
                        ],
                        value="ethereum",
                        clearable=False,
                        style={"width": "160px", "backgroundColor": COLORS["card"], "color": COLORS["text"], "fontSize": "13px"},
                    ),
                    html.Span(id="last-updated", children=f"Updated: {now}", style={"color": COLORS["text_dim"], "fontSize": "12px", "marginLeft": "auto"}),
                ],
                style={
                    "display": "flex", "alignItems": "center", "gap": "16px",
                    "padding": "12px 24px", "backgroundColor": COLORS["card"],
                    "borderBottom": f"1px solid {COLORS['border']}",
                },
            ),

            # Tabs
            dcc.Tabs(
                id="tabs",
                value="scorecard",
                children=[
                    dcc.Tab(label="Opportunity Scorecard", value="scorecard"),
                    dcc.Tab(label="Market Sizing (USD)", value="market"),
                    dcc.Tab(label="Competition Landscape", value="competition"),
                    dcc.Tab(label="ROI Projections", value="roi"),
                    dcc.Tab(label="Strategy Backtests", value="backtest"),
                ],
                style={"backgroundColor": COLORS["bg"]},
                colors={
                    "border": COLORS["border"],
                    "primary": COLORS["blue"],
                    "background": COLORS["card"],
                },
            ),

            # Tab content — pre-rendered with scorecard
            html.Div(id="tab-content", style={"padding": "24px"}, children=initial_content),

            # Auto-refresh
            dcc.Interval(id="refresh-interval", interval=60 * 1000, n_intervals=0),

            # Data store
            dcc.Store(id="data-store"),
        ],
        style={"backgroundColor": COLORS["bg"], "minHeight": "100vh", "fontFamily": "'JetBrains Mono', 'Fira Code', monospace"},
    )


app.layout = serve_layout


# ── Data Loading ──

def load_all_data(chain: str = "all"):
    """Load all data for the dashboard. Called on each refresh."""
    try:
        conn = queries.get_connection()
        price_engine = PriceEngine(conn)
        eth_price = price_engine.get_average_price()

        data = {
            "chain": chain,
            "available_chains": queries.available_chains(conn),
            "overview": queries.overview(conn, chain),
            "gas": queries.gas_stats(conn, chain),
            "gas_ts": queries.gas_time_series(conn, chain),
            "protocols": queries.swap_protocol_breakdown(conn, chain),
            "top_pools": queries.top_pools(conn, chain),
            "top_senders": queries.top_senders(conn, chain),
            "hhi": queries.herfindahl_index(conn, chain),
            "multi_swap": queries.multi_swap_distribution(conn, chain),
            "arb_candidates": queries.arb_candidates(conn, chain),
            "failed_txs": queries.failed_tx_stats(conn, chain),
            "liquidations": queries.liquidation_stats(conn, chain),
            "hourly": queries.hourly_swap_activity(conn, chain),
            "daily": queries.daily_activity(conn, chain),
            "strategy_results": strategy_check.run_all_checks(conn, chain),
            "eth_price": eth_price,
            "price_range": price_engine.get_price_range(),
        }

        # Cross-chain summary (only for "all" view)
        if chain == "all":
            try:
                data["cross_chain"] = queries.cross_chain_summary(conn)
            except Exception:
                data["cross_chain"] = pd.DataFrame()

        # Try optional queries (may fail on limited data)
        try:
            data["sender_rates"] = queries.sender_success_rates(conn, chain)
        except Exception:
            data["sender_rates"] = pd.DataFrame()

        try:
            data["sender_hourly"] = queries.sender_hourly_activity(conn, chain)
        except Exception:
            data["sender_hourly"] = pd.DataFrame()

        try:
            data["sandwich"] = queries.sandwich_candidates(conn, chain)
        except Exception:
            data["sandwich"] = pd.DataFrame()

        try:
            data["liq_details"] = queries.liquidation_details(conn)
        except Exception:
            data["liq_details"] = pd.DataFrame()

        conn.close()
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        import traceback
        traceback.print_exc()
        return None


# Cache data per chain
_cached_data = {}
_cache_times = {}


def get_data(chain: str = "all"):
    global _cached_data, _cache_times
    import time
    now = time.time()
    if chain not in _cached_data or (now - _cache_times.get(chain, 0)) > 30:
        _cached_data[chain] = load_all_data(chain)
        _cache_times[chain] = now
    return _cached_data[chain]


# ── Tab Renderers ──

def render_scorecard(data):
    if data is None:
        return html.Div("No data available", style={"color": COLORS["red"]})

    ov = data["overview"]
    results = data["strategy_results"]
    eth_price = data["eth_price"]

    # Summary cards
    total_mev_usd = sum(r.net_monthly_profit_usd for r in results)
    arb_count = data["arb_candidates"]["strong_arb_candidates"]
    liq_count = data["liquidations"]["total"]
    failed_bots = data["failed_txs"]["complex_failed"]

    summary_cards = html.Div(
        [
            make_card("Total Est. Monthly MEV", f"${total_mev_usd:,.0f}", COLORS["green"] if total_mev_usd > 0 else COLORS["red"]),
            make_card("Arb Candidates (7d)", f"{arb_count:,}", COLORS["blue"]),
            make_card("Liquidations (7d)", f"{liq_count:,}", COLORS["purple"]),
            make_card("Bot Reverts (7d)", f"{failed_bots:,}", COLORS["yellow"]),
            make_card("ETH/USD", f"${eth_price:,.0f}", COLORS["cyan"]),
        ],
        style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginBottom": "24px"},
    )

    # Strategy verdict table
    verdict_rows = []
    for r in results:
        verdict_rows.append(
            html.Tr([
                html.Td(r.strategy, style={"fontWeight": "bold"}),
                html.Td(f"{r.detected_count:,}"),
                html.Td(f"${r.net_monthly_profit_usd:,.0f}"),
                html.Td(f"{r.competition_hhi:.3f}"),
                html.Td(f"{r.score:.0f}/100"),
                html.Td(make_verdict_badge(r.verdict)),
            ], style={"borderBottom": f"1px solid {COLORS['border']}"})
        )

    verdict_table = html.Table(
        [
            html.Thead(html.Tr([
                html.Th("Strategy"), html.Th("Detected"),
                html.Th("Net Profit/mo"), html.Th("HHI"),
                html.Th("Score"), html.Th("Verdict"),
            ])),
            html.Tbody(verdict_rows),
        ],
        style={"width": "100%", "borderCollapse": "collapse", "color": COLORS["text"]},
    )

    # Radar chart
    categories = ["Market Size", "Competition\n(inverse)", "Simplicity", "Trend"]
    fig_radar = go.Figure()
    for r in results:
        size_s = min(100, max(0, 20 * np.log10(max(r.net_monthly_profit_usd, 1) / 100))) if r.net_monthly_profit_usd > 0 else 0
        comp_s = 100 * (1 - r.competition_hhi)
        # Complexity estimate by strategy
        complexity_map = {"Sandwich": 40, "DEX Arbitrage": 60, "Liquidation": 50, "Backrun": 70}
        simp_s = complexity_map.get(r.strategy, 50)
        trend_s = 50  # Neutral for now

        fig_radar.add_trace(go.Scatterpolar(
            r=[size_s, comp_s, simp_s, trend_s],
            theta=categories,
            fill="toself",
            name=r.strategy,
            opacity=0.6,
        ))

    fig_radar.update_layout(
        polar=dict(
            bgcolor=COLORS["card"],
            radialaxis=dict(visible=True, range=[0, 100], color=COLORS["text_dim"]),
            angularaxis=dict(color=COLORS["text"]),
        ),
        showlegend=True,
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["card"],
        font=dict(color=COLORS["text"]),
        margin=dict(l=60, r=60, t=40, b=40),
        height=400,
    )

    return html.Div([
        summary_cards,
        html.Div([
            html.Div([
                html.H3("Strategy Verdicts", style={"color": COLORS["text"], "marginBottom": "16px"}),
                verdict_table,
            ], style={"flex": "1", "backgroundColor": COLORS["card"], "padding": "20px", "borderRadius": "6px"}),
            html.Div([
                html.H3("Strategy Comparison", style={"color": COLORS["text"], "marginBottom": "16px"}),
                dcc.Graph(figure=fig_radar, config={"displayModeBar": False}),
            ], style={"flex": "1", "backgroundColor": COLORS["card"], "padding": "20px", "borderRadius": "6px"}),
        ], style={"display": "flex", "gap": "16px"}),
    ])


def render_market(data):
    if data is None:
        return html.Div("No data available", style={"color": COLORS["red"]})

    results = data["strategy_results"]
    eth_price = data["eth_price"]
    protocols = data["protocols"]
    gas = data["gas"]
    daily = data["daily"]
    gas_ts = data["gas_ts"]

    # Monthly profit bar chart
    fig_profit = go.Figure()
    strategies = [r.strategy for r in results]
    gross = [r.estimated_monthly_profit_usd for r in results]
    gas_costs = [r.gas_cost_usd for r in results]
    net = [r.net_monthly_profit_usd for r in results]

    fig_profit.add_trace(go.Bar(name="Gross Profit", x=strategies, y=gross, marker_color=COLORS["green"]))
    fig_profit.add_trace(go.Bar(name="Gas Cost", x=strategies, y=gas_costs, marker_color=COLORS["red"]))
    fig_profit.add_trace(go.Bar(name="Net Profit", x=strategies, y=net, marker_color=COLORS["blue"]))
    fig_profit.update_layout(
        barmode="group",
        title="Estimated Monthly Profit by Strategy (USD)",
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card"],
        font=dict(color=COLORS["text"]),
        xaxis=dict(gridcolor=COLORS["border"]),
        yaxis=dict(gridcolor=COLORS["border"], title="USD"),
        height=400,
    )

    # Protocol pie chart
    fig_proto = go.Figure()
    if not protocols.empty:
        fig_proto.add_trace(go.Pie(
            labels=protocols["protocol"], values=protocols["swaps"],
            hole=0.4,
            marker=dict(colors=[COLORS["blue"], COLORS["purple"], COLORS["green"], COLORS["yellow"], COLORS["red"]]),
        ))
    fig_proto.update_layout(
        title="Swap Volume by Protocol",
        paper_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        height=350,
    )

    # Daily swap activity
    fig_daily = go.Figure()
    if not daily.empty:
        daily["date"] = pd.to_datetime(daily["day_ts"], unit="s")
        fig_daily.add_trace(go.Scatter(
            x=daily["date"], y=daily["swaps"],
            mode="lines+markers", name="Swaps",
            line=dict(color=COLORS["blue"]),
        ))
        fig_daily.add_trace(go.Scatter(
            x=daily["date"], y=daily["unique_senders"],
            mode="lines+markers", name="Unique Senders",
            line=dict(color=COLORS["purple"]),
            yaxis="y2",
        ))
    fig_daily.update_layout(
        title="Daily Activity",
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card"],
        font=dict(color=COLORS["text"]),
        xaxis=dict(gridcolor=COLORS["border"]),
        yaxis=dict(gridcolor=COLORS["border"], title="Swaps"),
        yaxis2=dict(title="Unique Senders", overlaying="y", side="right", gridcolor=COLORS["border"]),
        height=350,
    )

    # Gas time series
    fig_gas = go.Figure()
    if not gas_ts.empty:
        gas_ts["datetime"] = pd.to_datetime(gas_ts["hour_ts"], unit="s")
        fig_gas.add_trace(go.Scatter(
            x=gas_ts["datetime"], y=gas_ts["avg_base_fee"],
            mode="lines", name="Base Fee (gwei)",
            line=dict(color=COLORS["yellow"]),
        ))
    fig_gas.update_layout(
        title="Hourly Average Base Fee (gwei)",
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card"],
        font=dict(color=COLORS["text"]),
        xaxis=dict(gridcolor=COLORS["border"]),
        yaxis=dict(gridcolor=COLORS["border"], title="gwei"),
        height=350,
    )

    # Summary cards
    cards = html.Div([
        make_card("Avg Base Fee", f"{gas['avg_base_fee']} gwei", COLORS["yellow"]),
        make_card("Block Utilization", f"{gas['avg_utilization']}%", COLORS["blue"]),
        make_card("ETH/USD", f"${eth_price:,.0f}", COLORS["cyan"]),
        make_card("Swaps (7d)", f"{data['overview']['swap_count']:,}", COLORS["green"]),
    ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginBottom": "24px"})

    return html.Div([
        cards,
        html.Div([
            html.Div([dcc.Graph(figure=fig_profit, config={"displayModeBar": False})],
                     style={"flex": "2", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
            html.Div([dcc.Graph(figure=fig_proto, config={"displayModeBar": False})],
                     style={"flex": "1", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
        ], style={"display": "flex", "gap": "16px", "marginBottom": "16px"}),
        html.Div([
            html.Div([dcc.Graph(figure=fig_daily, config={"displayModeBar": False})],
                     style={"flex": "1", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
            html.Div([dcc.Graph(figure=fig_gas, config={"displayModeBar": False})],
                     style={"flex": "1", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
        ], style={"display": "flex", "gap": "16px"}),
    ])


def render_competition(data):
    if data is None:
        return html.Div("No data available", style={"color": COLORS["red"]})

    hhi = data["hhi"]
    top_senders = data["top_senders"]
    hourly = data["hourly"]

    # HHI Gauge
    hhi_color = COLORS["green"] if hhi < 0.15 else (COLORS["yellow"] if hhi < 0.3 else COLORS["red"])
    fig_hhi = go.Figure(go.Indicator(
        mode="gauge+number",
        value=hhi,
        domain=dict(x=[0, 1], y=[0, 1]),
        title=dict(text="Herfindahl-Hirschman Index", font=dict(color=COLORS["text"])),
        number=dict(font=dict(color=COLORS["text"])),
        gauge=dict(
            axis=dict(range=[0, 1], tickcolor=COLORS["text_dim"]),
            bar=dict(color=hhi_color),
            bgcolor=COLORS["card"],
            bordercolor=COLORS["border"],
            steps=[
                dict(range=[0, 0.15], color="#3fb95020"),
                dict(range=[0.15, 0.3], color="#d2992220"),
                dict(range=[0.3, 1.0], color="#f8514920"),
            ],
            threshold=dict(line=dict(color=COLORS["red"], width=2), thickness=0.8, value=0.3),
        ),
    ))
    fig_hhi.update_layout(
        paper_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        height=300,
        margin=dict(t=60, b=20),
    )

    # Top senders treemap
    fig_tree = go.Figure()
    if not top_senders.empty:
        labels = [f"{s[:10]}..." for s in top_senders["sender"]]
        fig_tree = go.Figure(go.Treemap(
            labels=labels,
            parents=[""] * len(labels),
            values=top_senders["swaps"],
            textinfo="label+value",
            marker=dict(
                colors=top_senders["swaps"],
                colorscale="Blues",
            ),
        ))
    fig_tree.update_layout(
        title="Bot Market Share (by swap count)",
        paper_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        height=400,
        margin=dict(t=40, b=10, l=10, r=10),
    )

    # Hourly activity heatmap
    fig_hourly = go.Figure()
    if not hourly.empty:
        fig_hourly.add_trace(go.Bar(
            x=[f"{int(h):02d}:00" for h in hourly["hour"]],
            y=hourly["swaps"],
            marker_color=COLORS["blue"],
        ))
    fig_hourly.update_layout(
        title="Swap Activity by Hour (UTC)",
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card"],
        font=dict(color=COLORS["text"]),
        xaxis=dict(gridcolor=COLORS["border"]),
        yaxis=dict(gridcolor=COLORS["border"], title="Swaps"),
        height=300,
    )

    # Top senders table
    chain = data.get("chain", "all")
    explorer_urls = {
        "ethereum": "https://etherscan.io",
        "polygon": "https://polygonscan.com",
        "blast": "https://blastscan.io",
        "base": "https://basescan.org",
        "arbitrum": "https://arbiscan.io",
    }
    explorer = explorer_urls.get(chain, "https://etherscan.io")

    sender_rows = []
    if not top_senders.empty:
        for _, row in top_senders.iterrows():
            sender_rows.append(
                html.Tr([
                    html.Td(html.A(
                        f"{row['sender'][:16]}...",
                        href=f"{explorer}/address/{row['sender']}",
                        target="_blank",
                        style={"color": COLORS["blue"], "textDecoration": "none"},
                    )),
                    html.Td(f"{row['swaps']:,}"),
                    html.Td(f"{row['pools']:,}"),
                    html.Td(f"{row['protocols']}"),
                ], style={"borderBottom": f"1px solid {COLORS['border']}"})
            )

    sender_table = html.Table(
        [
            html.Thead(html.Tr([
                html.Th("Sender"), html.Th("Swaps"),
                html.Th("Pools"), html.Th("Protocols"),
            ], style={"borderBottom": f"2px solid {COLORS['border']}"})),
            html.Tbody(sender_rows),
        ],
        style={"width": "100%", "borderCollapse": "collapse", "color": COLORS["text"], "fontSize": "13px"},
    )

    # Summary cards
    hhi_label = "Low" if hhi < 0.15 else ("Moderate" if hhi < 0.3 else "High")
    cards = html.Div([
        make_card("HHI", f"{hhi:.4f} ({hhi_label})", hhi_color),
        make_card("Active Bots", f"{len(top_senders):,}", COLORS["blue"]),
        make_card("Bot Reverts", f"{data['failed_txs']['complex_failed']:,}", COLORS["yellow"]),
        make_card("Unique Pools", f"{data['overview']['unique_pools']:,}", COLORS["purple"]),
    ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginBottom": "24px"})

    return html.Div([
        cards,
        html.Div([
            html.Div([dcc.Graph(figure=fig_hhi, config={"displayModeBar": False})],
                     style={"flex": "1", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
            html.Div([dcc.Graph(figure=fig_tree, config={"displayModeBar": False})],
                     style={"flex": "2", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
        ], style={"display": "flex", "gap": "16px", "marginBottom": "16px"}),
        html.Div([
            html.Div([
                html.H3("Top Senders (likely bots / routers)", style={"color": COLORS["text"], "marginBottom": "12px"}),
                sender_table,
            ], style={"flex": "1", "backgroundColor": COLORS["card"], "padding": "20px", "borderRadius": "6px"}),
            html.Div([dcc.Graph(figure=fig_hourly, config={"displayModeBar": False})],
                     style={"flex": "1", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
        ], style={"display": "flex", "gap": "16px"}),
    ])


def render_roi(data):
    if data is None:
        return html.Div("No data available", style={"color": COLORS["red"]})

    results = data["strategy_results"]
    eth_price = data["eth_price"]

    # ROI table with sensitivity analysis
    capture_rates = [0.05, 0.10, 0.25]
    infra_cost = 50  # Monthly droplet cost

    rows = []
    for r in results:
        for cr in capture_rates:
            captured_profit = r.net_monthly_profit_usd * cr
            roi = ((captured_profit - infra_cost) / max(infra_cost, 1)) * 100
            rows.append({
                "Strategy": r.strategy,
                "Capture Rate": f"{cr*100:.0f}%",
                "Gross Profit": f"${r.estimated_monthly_profit_usd * cr:,.0f}",
                "Gas Cost": f"${r.gas_cost_usd * cr:,.0f}",
                "Net Profit": f"${captured_profit:,.0f}",
                "Monthly ROI": f"{roi:+,.0f}%",
                "Break-even": "Yes" if captured_profit > infra_cost else "No",
            })

    roi_df = pd.DataFrame(rows)

    # Sensitivity chart
    fig_sens = go.Figure()
    for r in results:
        rates = np.linspace(0.01, 0.50, 50)
        profits = [r.net_monthly_profit_usd * cr - infra_cost for cr in rates]
        fig_sens.add_trace(go.Scatter(
            x=rates * 100, y=profits,
            mode="lines", name=r.strategy,
        ))
    fig_sens.add_hline(y=0, line_dash="dash", line_color=COLORS["red"], annotation_text="Break-even")
    fig_sens.update_layout(
        title="Net Monthly Profit vs Capture Rate",
        xaxis=dict(title="Capture Rate (%)", gridcolor=COLORS["border"]),
        yaxis=dict(title="Net Profit (USD)", gridcolor=COLORS["border"]),
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card"],
        font=dict(color=COLORS["text"]),
        height=400,
    )

    # Break-even analysis
    breakeven_rows = []
    for r in results:
        if r.net_monthly_profit_usd > 0:
            be_rate = infra_cost / r.net_monthly_profit_usd
            be_pct = f"{be_rate * 100:.2f}%"
        else:
            be_pct = "N/A (negative)"
        breakeven_rows.append(
            html.Tr([
                html.Td(r.strategy, style={"fontWeight": "bold"}),
                html.Td(f"${r.net_monthly_profit_usd:,.0f}"),
                html.Td(be_pct),
                html.Td(f"${infra_cost}/mo"),
            ], style={"borderBottom": f"1px solid {COLORS['border']}"})
        )

    breakeven_table = html.Table([
        html.Thead(html.Tr([
            html.Th("Strategy"), html.Th("Total Market"),
            html.Th("Min Capture Rate"), html.Th("Infra Cost"),
        ], style={"borderBottom": f"2px solid {COLORS['border']}"})),
        html.Tbody(breakeven_rows),
    ], style={"width": "100%", "borderCollapse": "collapse", "color": COLORS["text"]})

    # Gas sensitivity
    fig_gas_sens = go.Figure()
    gas_multipliers = [0.5, 1.0, 1.5, 2.0, 3.0]
    for r in results:
        profits = [r.estimated_monthly_profit_usd - r.gas_cost_usd * m for m in gas_multipliers]
        fig_gas_sens.add_trace(go.Bar(
            x=[f"{m}x" for m in gas_multipliers],
            y=profits,
            name=r.strategy,
        ))
    fig_gas_sens.update_layout(
        title="Net Profit vs Gas Price Multiplier",
        barmode="group",
        xaxis=dict(title="Gas Multiplier", gridcolor=COLORS["border"]),
        yaxis=dict(title="Net Profit (USD)", gridcolor=COLORS["border"]),
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card"],
        font=dict(color=COLORS["text"]),
        height=350,
    )

    return html.Div([
        html.Div([
            make_card("Infra Cost", f"${infra_cost}/mo", COLORS["text_dim"]),
            make_card("ETH/USD", f"${eth_price:,.0f}", COLORS["cyan"]),
            make_card("Total Market", f"${sum(r.net_monthly_profit_usd for r in results):,.0f}/mo", COLORS["green"]),
        ], style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginBottom": "24px"}),

        html.Div([
            html.Div([dcc.Graph(figure=fig_sens, config={"displayModeBar": False})],
                     style={"flex": "1", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
            html.Div([
                html.H3("Break-even Analysis", style={"color": COLORS["text"], "marginBottom": "12px"}),
                breakeven_table,
            ], style={"flex": "1", "backgroundColor": COLORS["card"], "padding": "20px", "borderRadius": "6px"}),
        ], style={"display": "flex", "gap": "16px", "marginBottom": "16px"}),

        html.Div([
            html.Div([dcc.Graph(figure=fig_gas_sens, config={"displayModeBar": False})],
                     style={"flex": "1", "backgroundColor": COLORS["card"], "borderRadius": "6px", "padding": "8px"}),
            html.Div([
                html.H3("ROI by Capture Rate", style={"color": COLORS["text"], "marginBottom": "12px"}),
                dash_table.DataTable(
                    data=roi_df.to_dict("records"),
                    columns=[{"name": c, "id": c} for c in roi_df.columns],
                    style_header={"backgroundColor": COLORS["card"], "color": COLORS["text"], "fontWeight": "bold", "borderBottom": f"2px solid {COLORS['border']}"},
                    style_cell={"backgroundColor": COLORS["bg"], "color": COLORS["text"], "border": f"1px solid {COLORS['border']}", "padding": "8px", "fontSize": "12px"},
                    style_data_conditional=[
                        {"if": {"filter_query": '{Break-even} = "Yes"'}, "backgroundColor": "#3fb95015"},
                        {"if": {"filter_query": '{Break-even} = "No"'}, "backgroundColor": "#f8514915"},
                    ],
                    page_size=12,
                ),
            ], style={"flex": "1", "backgroundColor": COLORS["card"], "padding": "20px", "borderRadius": "6px"}),
        ], style={"display": "flex", "gap": "16px"}),
    ])


def render_backtest(data):
    if data is None:
        return html.Div("No data available", style={"color": COLORS["red"]})

    results = data["strategy_results"]

    # Risk.toml gate checks
    MIN_MONTHLY_PROFIT_ETH = 5.0
    MAX_HERFINDAHL = 0.30
    eth_price = data["eth_price"]
    min_monthly_profit_usd = MIN_MONTHLY_PROFIT_ETH * eth_price

    gate_rows = []
    for r in results:
        profit_pass = r.net_monthly_profit_usd > min_monthly_profit_usd
        hhi_pass = r.competition_hhi < MAX_HERFINDAHL

        gate_rows.append(
            html.Tr([
                html.Td(r.strategy, style={"fontWeight": "bold"}),
                html.Td(f"${r.net_monthly_profit_usd:,.0f}"),
                html.Td(
                    f"{'PASS' if profit_pass else 'FAIL'} (>{min_monthly_profit_usd:,.0f})",
                    style={"color": COLORS["green"] if profit_pass else COLORS["red"]},
                ),
                html.Td(f"{r.competition_hhi:.4f}"),
                html.Td(
                    f"{'PASS' if hhi_pass else 'FAIL'} (<{MAX_HERFINDAHL})",
                    style={"color": COLORS["green"] if hhi_pass else COLORS["red"]},
                ),
                html.Td(make_verdict_badge(r.verdict)),
            ], style={"borderBottom": f"1px solid {COLORS['border']}"})
        )

    gate_table = html.Table([
        html.Thead(html.Tr([
            html.Th("Strategy"), html.Th("Net Profit/mo"),
            html.Th("Profit Gate"), html.Th("HHI"),
            html.Th("HHI Gate"), html.Th("Verdict"),
        ], style={"borderBottom": f"2px solid {COLORS['border']}"})),
        html.Tbody(gate_rows),
    ], style={"width": "100%", "borderCollapse": "collapse", "color": COLORS["text"]})

    # Per-strategy detail sections
    detail_sections = []
    for r in results:
        verdict_color = VERDICT_COLORS.get(r.verdict, COLORS["text_dim"])

        # Sample transactions
        tx_links = []
        for h in r.sample_tx_hashes[:5]:
            tx_links.append(html.Li(
                html.A(
                    f"{h[:20]}...",
                    href=f"https://etherscan.io/tx/{h}",
                    target="_blank",
                    style={"color": COLORS["blue"], "textDecoration": "none", "fontSize": "12px"},
                )
            ))

        detail_sections.append(
            html.Div([
                html.Div([
                    html.H3(r.strategy, style={"color": COLORS["text"], "margin": "0", "display": "inline"}),
                    html.Span(f" ", style={"marginLeft": "12px"}),
                    make_verdict_badge(r.verdict),
                ], style={"marginBottom": "12px"}),
                html.Div([
                    html.Div([
                        html.P(f"Detected patterns: {r.detected_count:,}", style={"margin": "4px 0"}),
                        html.P(f"Gross profit: ${r.estimated_monthly_profit_usd:,.2f}/mo", style={"margin": "4px 0"}),
                        html.P(f"Gas cost: ${r.gas_cost_usd:,.2f}/mo", style={"margin": "4px 0"}),
                        html.P(f"Net profit: ${r.net_monthly_profit_usd:,.2f}/mo", style={"margin": "4px 0", "fontWeight": "bold"}),
                    ], style={"flex": "1"}),
                    html.Div([
                        html.P(f"Competition HHI: {r.competition_hhi:.4f}", style={"margin": "4px 0"}),
                        html.P(f"Unique competitors: {r.unique_competitors}", style={"margin": "4px 0"}),
                        html.P(f"Score: {r.score:.0f}/100", style={"margin": "4px 0"}),
                        html.P(f"Win rate: {r.win_rate*100:.1f}%" if r.win_rate > 0 else "Win rate: N/A", style={"margin": "4px 0"}),
                    ], style={"flex": "1"}),
                    html.Div([
                        html.P("Sample Transactions:", style={"margin": "4px 0", "fontWeight": "bold"}),
                        html.Ul(tx_links if tx_links else [html.Li("No samples available")], style={"margin": "4px 0", "paddingLeft": "16px"}),
                    ], style={"flex": "1"}),
                ], style={"display": "flex", "gap": "16px"}),
            ], style={
                "backgroundColor": COLORS["card"],
                "borderLeft": f"4px solid {verdict_color}",
                "borderRadius": "6px",
                "padding": "20px",
                "marginBottom": "16px",
                "color": COLORS["text"],
                "fontSize": "13px",
            })
        )

    return html.Div([
        html.Div([
            html.H3("Risk Gate Checks", style={"color": COLORS["text"], "marginBottom": "12px"}),
            html.P(
                f"Thresholds from risk.toml: min_monthly_profit = {MIN_MONTHLY_PROFIT_ETH} ETH (${min_monthly_profit_usd:,.0f}), max_herfindahl = {MAX_HERFINDAHL}",
                style={"color": COLORS["text_dim"], "fontSize": "13px", "marginBottom": "16px"},
            ),
            gate_table,
        ], style={"backgroundColor": COLORS["card"], "padding": "20px", "borderRadius": "6px", "marginBottom": "24px"}),

        html.H3("Strategy Details", style={"color": COLORS["text"], "marginBottom": "16px"}),
        *detail_sections,
    ])


# ── Callbacks ──

@app.callback(
    [Output("tab-content", "children"), Output("last-updated", "children"), Output("chain-label", "children")],
    [Input("tabs", "value"), Input("chain-selector", "value"), Input("refresh-interval", "n_intervals")],
)
def update_tab(tab, chain, _):
    chain = chain or "all"
    data = get_data(chain)
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    updated_text = f"Updated: {now}"
    chain_label = "Multi-Chain Capture" if chain == "all" else f"{chain.capitalize()} Capture"

    renderers = {
        "scorecard": render_scorecard,
        "market": render_market,
        "competition": render_competition,
        "roi": render_roi,
        "backtest": render_backtest,
    }

    renderer = renderers.get(tab, render_scorecard)
    return renderer(data), updated_text, chain_label


# ── Run ──

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    print(f"Starting MEV Dashboard on http://{args.host}:{args.port}")
    print(f"Data directory: {MEV_DIR / 'data'}")

    # Pre-cache ETH data so first page load is instant (avoid OOM with all-chains)
    print("Pre-loading Ethereum data...")
    try:
        _data = get_data("ethereum")
        if _data:
            print(f"ETH data loaded: {_data['overview']['swap_count']:,} swaps, {_data['overview']['blocks']:,} blocks")
        else:
            print("WARNING: Failed to load data")
    except Exception as e:
        print(f"WARNING: Data pre-load failed: {e}")

    app.run(host=args.host, port=args.port, debug=args.debug)

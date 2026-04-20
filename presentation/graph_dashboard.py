"""Graph Dashboard – visualises Neo4j graph data from the provider."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config

from infrastructure.graph.provider import Neo4jGraphProvider
from infrastructure.graph.cypher_commands import (
    ALL_CUSTOMERS,
    BENEFICIARY_HOTSPOTS,
    CUSTOMER_TX_CATEGORIES,
    BENEFICIARY_INCOMING_ANALYSIS,
    CHANNEL_RISK,
    CROSS_BORDER_FLOWS,
    CURRENCY_BREAKDOWN,
    CUSTOMER_BENEFICIARY_NETWORK,
    CUSTOMER_RISK_PROFILE,
    CUSTOMER_SUBGRAPH,
    DOMINANT_CURRENCY,
    HIGH_RISK_TRANSACTIONS,
    NETWORK_DEGREE_METRICS,
    SHARED_BENEFICIARY_CUSTOMERS,
)
import networkx as nx

_RISK_COLOURS = {"HIGH": "#EF553B", "MEDIUM": "#FFA15A", "LOW": "#636EFA"}


def _convert_neo4j_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    """Convert neo4j.time.DateTime columns to pandas-compatible datetime."""
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna()
            if not sample.empty and hasattr(sample.iloc[0], "to_native"):
                df[col] = df[col].apply(lambda v: v.to_native() if v is not None and hasattr(v, "to_native") else v)
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def _safe_query(provider: Neo4jGraphProvider, query: str, **params) -> list[dict[str, Any]]:
    """Run a read query, returning empty list on failure."""
    try:
        return provider._run_read_many(query, **params)
    except Exception as e:
        st.warning(f"Graph query failed: {e}")
        return []


@st.cache_data(ttl=300, show_spinner=False)
def _cached_query(_provider: Neo4jGraphProvider, query: str, **params) -> list[dict[str, Any]]:
    """Cached read query — avoids re-running on every Streamlit rerun."""
    return _safe_query(_provider, query, **params)


_CURRENCY_SYMBOLS = {
    "EUR": "€", "USD": "$", "GBP": "£", "CHF": "CHF", "JPY": "¥",
    "PLN": "zł", "SEK": "kr", "NOK": "kr", "DKK": "kr", "CZK": "Kč",
}


def _get_currency_symbol(provider: Neo4jGraphProvider) -> str:
    """Return the display symbol for the dominant currency in the graph."""
    rows = _cached_query(provider, DOMINANT_CURRENCY)
    if rows:
        code = rows[0].get("currency", "EUR")
        return _CURRENCY_SYMBOLS.get(code, code)
    return "€"


# Shared plotly config for network graphs — disables scroll zoom to avoid
# conflicts with page scrolling, and shows useful modebar buttons.
_PLOTLY_NET_CONFIG = dict(
    scrollZoom=False,
    displayModeBar=True,
    modeBarButtonsToAdd=["resetScale2d"],
    modeBarButtonsToRemove=["lasso2d", "select2d", "autoScale2d"],
    displaylogo=False,
)


_NODE_COLOURS = {
    "Customer": "#4C8BF5",
    "CustomerAccount": "#7B61FF",
    "Transaction": "#00B894",
    "Transaction_HIGH": "#EF553B",
    "Transaction_MEDIUM": "#FFA15A",
    "Transaction_LOW": "#00B828",
    "Beneficiary": "#E84393",
}


def _render_graph_explorer(provider: Neo4jGraphProvider) -> None:
    """Interactive node explorer – select a customer, see their full sub-graph."""

    st.subheader("🔎 Interactive Graph Explorer")
    st.caption(
        "Select a customer to visualise their accounts, transactions and beneficiaries "
    )

    all_custs = _safe_query(provider, ALL_CUSTOMERS)
    if not all_custs:
        st.info("No customers in the graph yet.")
        return

    # Fetch per-customer transaction categories for filtering
    cust_cat_rows = _safe_query(provider, CUSTOMER_TX_CATEGORIES)
    cust_categories: dict[Any, list[str]] = {}
    for r in cust_cat_rows:
        cust_categories[r["customer_id"]] = r["categories"]

    all_cust_ids = [r["customer_id"] for r in all_custs]

    col0, col1, col2, col3 = st.columns([2, 2, 2, 2])
    with col0:
        cat_options = ["Fraud", "Violation", "No risk"]
        selected_cats = st.multiselect(
            "Transaction category", options=cat_options, default=cat_options,
            key="graph_explorer_cat",
        )
    # Filter customer list to those having at least one matching category
    if selected_cats:
        cust_ids = [cid for cid in all_cust_ids if any(c in selected_cats for c in cust_categories.get(cid, []))]
    else:
        cust_ids = all_cust_ids

    if not cust_ids:
        st.warning("No customers match the selected categories.")
        return

    with col1:
        selected_cid = st.selectbox("Select customer", cust_ids, key="graph_explorer_cid")
    with col2:
        physics_enabled = st.checkbox("Enable physics simulation", value=True, key="graph_explorer_physics")
    with col3:
        graph_height = st.slider("Graph height (px)", 400, 900, 620, step=20, key="graph_explorer_h")

    all_rows = _safe_query(provider, CUSTOMER_SUBGRAPH, customer_id=selected_cid)
    if not all_rows:
        st.warning("No transactions found for this customer.")
        return

    # Filter transactions by selected categories
    def _tx_category(r: dict) -> str:
        if str(r.get("is_fraud", "")).lower() == "true":
            return "Fraud"
        if (r.get("risk_score") or 0) > 0:
            return "Violation"
        return "No risk"

    rows = [r for r in all_rows if _tx_category(r) in selected_cats] if selected_cats else all_rows
    if not rows:
        st.warning("No transactions match the selected categories for this customer.")
        return

    nodes_map: dict[str, Node] = {}
    edges: list[Edge] = []

    cid_str = str(selected_cid)
    nodes_map[f"cust_{cid_str}"] = Node(
        id=f"cust_{cid_str}",
        label=f"👤 {cid_str}",
        size=35,
        color=_NODE_COLOURS["Customer"],
        font={"color": "white", "size": 14, "bold": True},
        shape="dot",
        title=f"Customer {cid_str}",
    )

    seen_edges: set[tuple[str, str, str]] = set()

    for r in rows:
        acc_id = f"acc_{r['account']}"
        tx_id = f"tx_{r['tx_id']}"
        ben_id = f"ben_{r['beneficiary']}"

        if acc_id not in nodes_map:
            nodes_map[acc_id] = Node(
                id=acc_id,
                label=f"🏦 {str(r['account'])[-6:]}",
                size=25,
                color=_NODE_COLOURS["CustomerAccount"],
                shape="dot",
                title=f"Account: {r['account']}",
                font={"color": "white", "size": 11},
            )

        risk_score = r.get("risk_score") or 0
        risk_cat = r.get("risk_category") or ""
        tx_colour = _NODE_COLOURS["Transaction"] if risk_score == 0 else _NODE_COLOURS.get(f"Transaction_{risk_cat}", _NODE_COLOURS["Transaction"])
        amount = r.get("amount") or 0
        tx_size = 15 + min(float(amount) / 2000, 18)
        rules_txt = r.get("triggered_rules") or "none"
        fraud_flag = "⚠️ FRAUD" if str(r.get("is_fraud", "")).lower() == "true" else ""

        cur_code = r.get('currency', '')
        cur_sym = _CURRENCY_SYMBOLS.get(cur_code, cur_code)
        tx_title = (
            f"TX: {r['tx_id']}\n"
            f"Amount: {cur_sym}{amount:,.2f}\n"
            f"Channel: {r.get('channel', '?')}\n"
            f"Risk: {risk_score} ({risk_cat})\n"
            f"Rules: {rules_txt}\n"
            f"{fraud_flag}"
        )

        if tx_id not in nodes_map:
            nodes_map[tx_id] = Node(
                id=tx_id,
                label=f"💳 {cur_sym}{amount:,.0f}",
                size=tx_size,
                color=tx_colour,
                shape="dot",
                title=tx_title,
                font={"color": "white", "size": 10},
            )

        ben_label = str(r["beneficiary"])[-6:]
        ben_country = r.get("ben_country") or ""
        if ben_id not in nodes_map:
            nodes_map[ben_id] = Node(
                id=ben_id,
                label=f"🏧 {ben_label}",
                size=22,
                color=_NODE_COLOURS["Beneficiary"],
                shape="dot",
                title=f"Beneficiary: {r['beneficiary']}\nCountry: {ben_country}",
                font={"color": "white", "size": 11},
            )

        for src, tgt, lbl in [
            (f"cust_{cid_str}", acc_id, "OWNS"),
            (acc_id, tx_id, "TRANSFER"),
            (tx_id, ben_id, "TO"),
        ]:
            key = (src, tgt, lbl)
            if key not in seen_edges:
                seen_edges.add(key)
                edge_color = "#EF553B" if (lbl == "TRANSFER" and risk_cat == "HIGH") else "#888888"
                edges.append(Edge(
                    source=src, target=tgt, label=lbl,
                    color=edge_color,
                    width=2 if risk_cat == "HIGH" else 1,
                    font={"size": 9, "color": "#666"},
                ))

    config = Config(
        width="100%",
        height=graph_height,
        directed=True,
        physics=physics_enabled,
        hierarchical=False,
        nodeHighlightBehavior=True,
        highlightColor="#F7A7A6",
        collapsible=False,
        node={"labelProperty": "label"},
        link={"labelProperty": "label", "renderLabel": True},
    )

    st.markdown(
        """
        <style>
        .stAgraph {border: 1px solid #333; border-radius: 8px;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    legend_cols = st.columns(6)
    legend_cols[0].markdown('<span style="color:#4C8BF5">⬤</span> **Customer**', unsafe_allow_html=True)
    legend_cols[1].markdown('<span style="color:#7B61FF">⬤</span> **Account**', unsafe_allow_html=True)
    legend_cols[2].markdown('<span style="color:#00B894">⬤</span> **Transaction**', unsafe_allow_html=True)
    legend_cols[3].markdown('<span style="color:#EF553B">⬤</span> **High-Risk TX**', unsafe_allow_html=True)
    legend_cols[4].markdown('<span style="color:#FFA15A">⬤</span> **Medium-Risk TX**', unsafe_allow_html=True)
    legend_cols[5].markdown('<span style="color:#E84393">⬤</span> **Beneficiary**', unsafe_allow_html=True)

    selected_node = agraph(
        nodes=list(nodes_map.values()),
        edges=edges,
        config=config,
    )

    if selected_node:
        neighbor_ids = {selected_node}
        for e in edges:
            if e.source == selected_node:
                neighbor_ids.add(e.to)
            elif e.to == selected_node:
                neighbor_ids.add(e.source)

        focused_nodes = [n for n in nodes_map.values() if n.id in neighbor_ids]
        focused_edges = [e for e in edges if e.source in neighbor_ids and e.to in neighbor_ids]

        selected_label = nodes_map[selected_node].title if selected_node in nodes_map else selected_node
        st.info(f"🔎 **Focused view** for node: **{selected_label}** — showing {len(focused_nodes)} connected nodes. Select another customer above to reset.")

        focus_config = Config(
            width="100%",
            height=450,
            directed=True,
            physics=True,
            hierarchical=False,
            nodeHighlightBehavior=True,
            highlightColor="#F7A7A6",
            collapsible=False,
            node={"labelProperty": "label"},
            link={"labelProperty": "label", "renderLabel": True},
        )

        agraph(
            nodes=focused_nodes,
            edges=focused_edges,
            config=focus_config,
        )

    with st.expander(f"📋 All transactions for customer {selected_cid} ({len(rows)} rows)", expanded=False):
        detail_df = _convert_neo4j_datetimes(pd.DataFrame(rows))
        display = [c for c in [
            "tx_id", "amount", "currency", "channel", "risk_score",
            "risk_category", "triggered_rules", "beneficiary", "ben_country", "ts",
        ] if c in detail_df.columns]

        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            ben_opts = ["All"] + sorted(detail_df["beneficiary"].dropna().unique().tolist())
            ben_selected = st.selectbox(
                "Filter by beneficiary", options=ben_opts, index=0,
                key="explorer_ben_filter",
            )
        with fc2:
            risk_opts = sorted(detail_df["risk_category"].dropna().unique().tolist())
            risk_filter = st.multiselect(
                "Filter by risk level", options=risk_opts, default=risk_opts,
                key="explorer_risk_filter",
            )
        with fc3:
            channel_opts = sorted(detail_df["channel"].dropna().unique().tolist())
            channel_filter = st.multiselect(
                "Filter by channel", options=channel_opts, default=channel_opts,
                key="explorer_channel_filter",
            )

        mask = detail_df["beneficiary"].isin([ben_selected]) if ben_selected != "All" else pd.Series(True, index=detail_df.index)
        if risk_filter:
            mask = mask & detail_df["risk_category"].fillna("").isin(risk_filter)
        if channel_filter:
            mask = mask & detail_df["channel"].fillna("").isin(channel_filter)
        filtered = detail_df[mask]

        st.caption(f"Showing {len(filtered)} of {len(detail_df)} transactions")

        def _colour_risk(val):
            c = _RISK_COLOURS.get(val, "")
            return f"background-color: {c}; color: white; font-weight: bold" if c else ""

        st.dataframe(
            filtered[display].style.map(_colour_risk, subset=["risk_category"]),
            width='stretch',
            height=350,
        )


def render_graph_dashboard(provider: Neo4jGraphProvider) -> None:
    """Render graph-based analytics dashboard."""

    st.header("🕸️ Graph Intelligence Dashboard")
    st.caption("Insights derived from the Neo4j transaction graph")

    cur = _get_currency_symbol(provider)

    network_data = _cached_query(provider, CUSTOMER_BENEFICIARY_NETWORK)
    risk_data = _cached_query(provider, HIGH_RISK_TRANSACTIONS)
    customer_profiles = _cached_query(provider, CUSTOMER_RISK_PROFILE)
    hotspots = _cached_query(provider, BENEFICIARY_HOTSPOTS)
    country_flows = _cached_query(provider, CROSS_BORDER_FLOWS)
    channel_risk = _cached_query(provider, CHANNEL_RISK)

    if not network_data:
        st.info("No graph data available. Upload and process transactions first.")
        return

    net_df = pd.DataFrame(network_data)

    if customer_profiles:
        st.subheader("👤 Customer Risk Profiles (from Graph)")
        prof_df = pd.DataFrame(customer_profiles)

        v1, v2 = st.columns(2)
        with v1:
            fig = px.scatter(
                prof_df, x="total_volume", y="avg_risk_score",
                size="flagged_transactions", color="max_risk_score",
                hover_data=["customer_id", "total_transactions"],
                title="Customer Risk vs Transaction Volume",
                color_continuous_scale="OrRd",
                labels={"total_volume": f"Total Volume ({cur})", "avg_risk_score": "Avg Risk Score"},
            )
            st.plotly_chart(fig, width='stretch')

        with v2:
            top_risky = prof_df.nlargest(15, "avg_risk_score")
            top_risky["customer_id"] = top_risky["customer_id"].astype(str)
            fig = px.bar(
                top_risky, x="customer_id", y="avg_risk_score",
                color="flagged_transactions", color_continuous_scale="Reds",
                title="Top 15 Riskiest Customers",
                labels={"avg_risk_score": "Avg Risk Score"},
            )
            st.plotly_chart(fig, width='stretch')

        st.markdown("**Top 15 Riskiest Customers**")
        table_df = prof_df.nlargest(15, "avg_risk_score").reset_index(drop=True)
        table_df.index = table_df.index + 1
        renamed = {c: c.replace("_", " ").title() for c in table_df.columns}
        table_df = table_df.rename(columns=renamed)
        fmt = {k: v for k, v in {"Avg Risk Score": "{:.2f}", "Max Risk Score": "{:.2f}", "Total Volume": f"{cur}{{:,.0f}}"}.items() if k in table_df.columns}
        bar_cols = [c for c in ["Avg Risk Score", "Max Risk Score"] if c in table_df.columns]
        styled_table = table_df.style.format(fmt)
        for col in bar_cols:
            styled_table = styled_table.bar(subset=[col], color="#EF553B80")
        st.dataframe(styled_table, width='stretch')

    st.markdown("---")

    _render_graph_explorer(provider)
    st.markdown("---")

    st.subheader("📊 Graph Overview")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Unique Customers", f"{net_df['customer_id'].nunique():,}")
    m2.metric("Unique Beneficiaries", f"{net_df['beneficiary'].nunique():,}")
    m3.metric("Transfer Links", f"{len(net_df):,}")
    m4.metric("Total Volume", f"{cur}{net_df['total_amount'].sum():,.0f}")

    st.markdown("---")

    st.subheader("🔗 Customer → Beneficiary Transfer Network")
    st.caption("Force-directed layout — connected nodes cluster together, revealing shared beneficiaries and hidden links")

    # --- Filters (inside a form so changes don't trigger re-run until Apply) ---
    with st.form(key="network_filter_form"):
        fc1, fc2, fc3, fc4 = st.columns([2, 2, 3, 1])
        with fc1:
            min_amount = st.number_input(f"Min total amount ({cur})", min_value=0, value=0, step=500, key="net_min_amt")
        with fc2:
            min_tx = st.number_input("Min transaction count", min_value=1, value=1, step=1, key="net_min_tx")
        with fc3:
            country_opts_net = sorted(net_df["country"].dropna().unique().tolist())
            country_filter_net = st.multiselect("Filter by beneficiary country", options=country_opts_net, default=[], key="net_country")
        with fc4:
            st.markdown("<br>", unsafe_allow_html=True)
            st.form_submit_button("Apply filters", use_container_width=True)

    filtered_net = net_df.copy()
    if min_amount > 0:
        filtered_net = filtered_net[filtered_net["total_amount"] >= min_amount]
    if min_tx > 1:
        filtered_net = filtered_net[filtered_net["tx_count"] >= min_tx]
    if country_filter_net:
        filtered_net = filtered_net[filtered_net["country"].isin(country_filter_net)]

    if filtered_net.empty:
        st.warning("No links match the current filters.")
    else:
        # Build networkx graph for force-directed layout
        G = nx.Graph()
        for _, row in filtered_net.iterrows():
            cid = f"c_{row['customer_id']}"
            bid = f"b_{row['beneficiary']}"
            G.add_node(cid, kind="customer", label=str(row["customer_id"]))
            G.add_node(bid, kind="beneficiary", label=str(row["beneficiary"])[-8:],
                       country=row.get("country", ""))
            w = float(row["total_amount"])
            if G.has_edge(cid, bid):
                G[cid][bid]["weight"] += w
            else:
                G.add_edge(cid, bid, weight=w)

        pos = nx.spring_layout(G, k=1.8 / math.sqrt(max(len(G), 1)), iterations=60, seed=42, weight="weight")

        # Compute degree for sizing
        degree = dict(G.degree())

        node_x, node_y, node_text, node_color, node_size, node_labels = [], [], [], [], [], []
        for nid, (x, y) in pos.items():
            data = G.nodes[nid]
            node_x.append(x)
            node_y.append(y)
            d = degree[nid]
            if data["kind"] == "customer":
                cust_vol = filtered_net[filtered_net["customer_id"].astype(str) == data["label"]]["total_amount"].sum()
                node_text.append(f"👤 Customer {data['label']}<br>Links: {d}<br>Volume: {cur}{cust_vol:,.0f}")
                node_color.append("#636EFA")
                node_size.append(14 + d * 4)
                node_labels.append(str(data["label"])[:8])
            else:
                ben_vol = filtered_net[filtered_net["beneficiary"] == data["label"].rjust(len(data["label"]))]["total_amount"].sum()
                # Highlight beneficiaries with multiple incoming customers
                is_hub = d >= 2
                colour = "#FF6B6B" if is_hub else "#E84393"
                hub_tag = "⚠️ SHARED HUB" if is_hub else ""
                node_text.append(f"🏧 Beneficiary …{data['label']}<br>Country: {data.get('country','?')}<br>Senders: {d} {hub_tag}")
                node_color.append(colour)
                node_size.append(12 + d * 5)
                node_labels.append(f"…{data['label'][-6:]}")

        # Edges with width proportional to volume
        max_w = max((edata["weight"] for _, _, edata in G.edges(data=True)), default=1)
        edge_traces = []
        for u, v, edata in G.edges(data=True):
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            w = 0.5 + 3.5 * (edata["weight"] / max_w)
            edge_traces.append(go.Scatter(
                x=[x0, x1, None], y=[y0, y1, None], mode="lines",
                line=dict(width=w, color="rgba(136,136,136,0.5)"),
                hoverinfo="text",
                text=f"{cur}{edata['weight']:,.0f}",
                showlegend=False,
            ))

        fig = go.Figure(data=edge_traces)
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y, mode="markers+text",
            marker=dict(size=node_size, color=node_color, line=dict(width=1, color="white")),
            text=node_labels,
            textposition="top center", textfont=dict(size=8),
            hovertext=node_text, hoverinfo="text",
            showlegend=False,
        ))
        fig.update_layout(
            title="Transfer Network (🔵 Customers ↔ 🔴 Beneficiaries — larger = more connections)",
            showlegend=False, hovermode="closest",
            dragmode="pan",
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, fixedrange=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, fixedrange=False),
            height=600,
        )
        leg_cols = st.columns(5)
        leg_cols[0].markdown('<span style="color:#636EFA">⬤</span> **Customer**', unsafe_allow_html=True)
        leg_cols[1].markdown('<span style="color:#E84393">⬤</span> **Beneficiary**', unsafe_allow_html=True)
        leg_cols[2].markdown('<span style="color:#FF6B6B">⬤</span> **Shared-Hub Beneficiary (≥2 senders)**', unsafe_allow_html=True)
        leg_cols[3].markdown('Edge width ∝ transfer volume', unsafe_allow_html=True)
        leg_cols[4].markdown('💡 *Drag to pan · toolbar to zoom/reset*', unsafe_allow_html=True)
        st.plotly_chart(fig, width='stretch', config=_PLOTLY_NET_CONFIG)

        # --- Network statistics ---
        components = list(nx.connected_components(G))
        st.markdown(f"**Network stats:** {len(G.nodes)} nodes · {len(G.edges)} edges · "
                    f"{len(components)} connected component(s)")

    # ── Shared Beneficiary Intelligence ──────────────────────────────────
    shared_data = _cached_query(provider, SHARED_BENEFICIARY_CUSTOMERS)
    if shared_data:
        st.markdown("---")
        st.subheader("🔍 Shared Beneficiary Intelligence")
        st.caption("Customers who send money to the **same** beneficiary — a key indicator of mule accounts, collusion, or layering.")

        shared_df = pd.DataFrame(shared_data)

        # Summary metrics
        sm1, sm2, sm3 = st.columns(3)
        sm1.metric("Shared-beneficiary links", len(shared_df))
        sm2.metric("Unique shared beneficiaries", shared_df["shared_beneficiary"].nunique())
        sm3.metric("Customer pairs linked",
                   shared_df.groupby(["customer_1", "customer_2"]).ngroups)

        # Build a customer-customer adjacency through shared beneficiaries
        cust_link_G = nx.Graph()
        for _, row in shared_df.iterrows():
            c1, c2 = str(row["customer_1"]), str(row["customer_2"])
            if cust_link_G.has_edge(c1, c2):
                cust_link_G[c1][c2]["shared_bens"] += 1
                cust_link_G[c1][c2]["volume"] += float(row["shared_volume"])
                cust_link_G[c1][c2]["max_risk"] = max(cust_link_G[c1][c2]["max_risk"], float(row["max_risk"] or 0))
            else:
                cust_link_G.add_edge(c1, c2, shared_bens=1,
                                     volume=float(row["shared_volume"]),
                                     max_risk=float(row["max_risk"] or 0))

        if len(cust_link_G) > 1:
            st.markdown("#### Customer Linkage Graph")
            st.caption("Customers connected through shared beneficiaries — thicker edges mean more shared beneficiaries")

            link_pos = nx.spring_layout(cust_link_G, k=2.0 / math.sqrt(max(len(cust_link_G), 1)),
                                        iterations=50, seed=42)
            link_degree = dict(cust_link_G.degree())

            lnx, lny, lnt, lnc, lns, lnl = [], [], [], [], [], []
            for nid, (x, y) in link_pos.items():
                lnx.append(x); lny.append(y)
                d = link_degree[nid]
                lnt.append(f"👤 Customer {nid}<br>Connected to {d} other customer(s) via shared beneficiaries")
                risk_val = max((cust_link_G[nid][nb]["max_risk"] for nb in cust_link_G.neighbors(nid)), default=0)
                if risk_val >= 70:
                    lnc.append("#EF553B")
                elif risk_val >= 40:
                    lnc.append("#FFA15A")
                else:
                    lnc.append("#636EFA")
                lns.append(16 + d * 6)
                lnl.append(str(nid)[:8])

            max_sb = max((edata["shared_bens"] for _, _, edata in cust_link_G.edges(data=True)), default=1)
            link_edge_traces = []
            for u, v, edata in cust_link_G.edges(data=True):
                x0, y0 = link_pos[u]
                x1, y1 = link_pos[v]
                w = 1 + 4 * (edata["shared_bens"] / max_sb)
                ec = "#EF553B" if edata["max_risk"] >= 70 else "#FFA15A" if edata["max_risk"] >= 40 else "#888"
                link_edge_traces.append(go.Scatter(
                    x=[x0, x1, None], y=[y0, y1, None], mode="lines",
                    line=dict(width=w, color=ec),
                    hoverinfo="text",
                    text=f"{edata['shared_bens']} shared ben(s)<br>Volume: {cur}{edata['volume']:,.0f}<br>Max risk: {edata['max_risk']:.0f}",
                    showlegend=False,
                ))

            link_fig = go.Figure(data=link_edge_traces)
            link_fig.add_trace(go.Scatter(
                x=lnx, y=lny, mode="markers+text",
                marker=dict(size=lns, color=lnc, line=dict(width=1.5, color="white")),
                text=lnl, textposition="top center", textfont=dict(size=10),
                hovertext=lnt, hoverinfo="text", showlegend=False,
            ))
            link_fig.update_layout(
                title="Customer-to-Customer Links via Shared Beneficiaries",
                showlegend=False, hovermode="closest", height=500,
                dragmode="pan",
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, fixedrange=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, fixedrange=False),
            )
            link_leg = st.columns(4)
            link_leg[0].markdown('🔴 High-risk link (≥70)', unsafe_allow_html=True)
            link_leg[1].markdown('🟠 Medium-risk link (≥40)', unsafe_allow_html=True)
            link_leg[2].markdown('🔵 Low-risk link', unsafe_allow_html=True)
            link_leg[3].markdown('💡 *Drag to pan · toolbar to zoom/reset*', unsafe_allow_html=True)
            st.plotly_chart(link_fig, width='stretch', config=_PLOTLY_NET_CONFIG)

        with st.expander("📋 Shared Beneficiary Details", expanded=False):
            display_shared = shared_df.copy()
            display_shared["shared_beneficiary"] = display_shared["shared_beneficiary"].astype(str).str[-8:]
            fmt_cols = {"shared_volume": f"{cur}{{:,.0f}}", "max_risk": "{:.0f}"}
            styled_shared = display_shared.style.format(
                {k: v for k, v in fmt_cols.items() if k in display_shared.columns}
            )
            st.dataframe(styled_shared, width='stretch', height=350)

    # ── Beneficiary Incoming Analysis ────────────────────────────────────
    ben_analysis = _cached_query(provider, BENEFICIARY_INCOMING_ANALYSIS)
    if ben_analysis:
        st.markdown("---")
        st.subheader("📥 Beneficiary Incoming Analysis")
        st.caption("Which beneficiaries receive from the most distinct customers? Potential mule accounts float to the top.")

        ben_df = pd.DataFrame(ben_analysis)
        ben_df["beneficiary_short"] = ben_df["beneficiary"].astype(str).str[-8:]

        ba1, ba2 = st.columns(2)
        with ba1:
            fig = px.bar(
                ben_df.head(20), x="beneficiary_short", y="unique_senders",
                color="avg_risk", color_continuous_scale="OrRd",
                hover_data=["country", "tx_count", "total_received"],
                title="Top 20 Beneficiaries by Unique Senders",
                labels={"unique_senders": "Unique Senders", "beneficiary_short": "Beneficiary"},
            )
            st.plotly_chart(fig, width='stretch')
        with ba2:
            fig = px.scatter(
                ben_df, x="unique_senders", y="total_received",
                size="tx_count", color="avg_risk", color_continuous_scale="OrRd",
                hover_data=["beneficiary_short", "country"],
                title="Senders vs Volume (size = tx count, colour = risk)",
                labels={"unique_senders": "Unique Senders", "total_received": f"Total Received ({cur})"},
            )
            st.plotly_chart(fig, width='stretch')

    # ── Network Degree Metrics ───────────────────────────────────────────
    degree_data = _cached_query(provider, NETWORK_DEGREE_METRICS)
    if degree_data:
        st.markdown("---")
        st.subheader("📈 Customer Network Degree Metrics")
        st.caption("Customers with unusually many beneficiaries may indicate smurfing or structuring.")

        deg_df = pd.DataFrame(degree_data)
        fig = px.scatter(
            deg_df, x="unique_beneficiaries", y="total_sent",
            size="tx_count", color="avg_risk", color_continuous_scale="OrRd",
            hover_data=["customer_id", "max_risk", "avg_tx_size"],
            title="Customer Fan-out: Unique Beneficiaries vs Total Sent",
            labels={"unique_beneficiaries": "Unique Beneficiaries", "total_sent": f"Total Sent ({cur})"},
        )
        st.plotly_chart(fig, width='stretch')

    st.markdown("---")

    if hotspots:
        st.subheader("🎯 Beneficiary Hotspots (Multiple Senders)")
        hs_df = pd.DataFrame(hotspots)

        fig = px.treemap(
            hs_df, path=["country", "beneficiary"], values="total_received",
            color="unique_senders", color_continuous_scale="YlOrRd",
            title="Beneficiaries by Country & Incoming Volume (colour = # unique senders)",
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, width='stretch')

        with st.expander("Hotspot details"):
            st.dataframe(hs_df, width='stretch')

    st.markdown("---")

    if country_flows:
        st.subheader("🌍 Cross-Border Transaction Flows")
        cf_df = pd.DataFrame(country_flows)

        _ISO2_TO_ISO3 = {
            "AF":"AFG","AL":"ALB","DZ":"DZA","AD":"AND","AO":"AGO","AG":"ATG","AR":"ARG","AM":"ARM","AU":"AUS","AT":"AUT",
            "AZ":"AZE","BS":"BHS","BH":"BHR","BD":"BGD","BB":"BRB","BY":"BLR","BE":"BEL","BZ":"BLZ","BJ":"BEN","BT":"BTN",
            "BO":"BOL","BA":"BIH","BW":"BWA","BR":"BRA","BN":"BRN","BG":"BGR","BF":"BFA","BI":"BDI","CV":"CPV","KH":"KHM",
            "CM":"CMR","CA":"CAN","CF":"CAF","TD":"TCD","CL":"CHL","CN":"CHN","CO":"COL","KM":"COM","CG":"COG","CD":"COD",
            "CR":"CRI","CI":"CIV","HR":"HRV","CU":"CUB","CY":"CYP","CZ":"CZE","DK":"DNK","DJ":"DJI","DM":"DMA","DO":"DOM",
            "EC":"ECU","EG":"EGY","SV":"SLV","GQ":"GNQ","ER":"ERI","EE":"EST","SZ":"SWZ","ET":"ETH","FJ":"FJI","FI":"FIN",
            "FR":"FRA","GA":"GAB","GM":"GMB","GE":"GEO","DE":"DEU","GH":"GHA","GR":"GRC","GD":"GRD","GT":"GTM","GN":"GIN",
            "GW":"GNB","GY":"GUY","HT":"HTI","HN":"HND","HU":"HUN","IS":"ISL","IN":"IND","ID":"IDN","IR":"IRN","IQ":"IRQ",
            "IE":"IRL","IL":"ISR","IT":"ITA","JM":"JAM","JP":"JPN","JO":"JOR","KZ":"KAZ","KE":"KEN","KI":"KIR","KP":"PRK",
            "KR":"KOR","KW":"KWT","KG":"KGZ","LA":"LAO","LV":"LVA","LB":"LBN","LS":"LSO","LR":"LBR","LY":"LBY","LI":"LIE",
            "LT":"LTU","LU":"LUX","MG":"MDG","MW":"MWI","MY":"MYS","MV":"MDV","ML":"MLI","MT":"MLT","MH":"MHL","MR":"MRT",
            "MU":"MUS","MX":"MEX","FM":"FSM","MD":"MDA","MC":"MCO","MN":"MNG","ME":"MNE","MA":"MAR","MZ":"MOZ","MM":"MMR",
            "NA":"NAM","NR":"NRU","NP":"NPL","NL":"NLD","NZ":"NZL","NI":"NIC","NE":"NER","NG":"NGA","MK":"MKD","NO":"NOR",
            "OM":"OMN","PK":"PAK","PW":"PLW","PA":"PAN","PG":"PNG","PY":"PRY","PE":"PER","PH":"PHL","PL":"POL","PT":"PRT",
            "QA":"QAT","RO":"ROU","RU":"RUS","RW":"RWA","KN":"KNA","LC":"LCA","VC":"VCT","WS":"WSM","SM":"SMR","ST":"STP",
            "SA":"SAU","SN":"SEN","RS":"SRB","SC":"SYC","SL":"SLE","SG":"SGP","SK":"SVK","SI":"SVN","SB":"SLB","SO":"SOM",
            "ZA":"ZAF","SS":"SSD","ES":"ESP","LK":"LKA","SD":"SDN","SR":"SUR","SE":"SWE","CH":"CHE","SY":"SYR","TW":"TWN",
            "TJ":"TJK","TZ":"TZA","TH":"THA","TL":"TLS","TG":"TGO","TO":"TON","TT":"TTO","TN":"TUN","TR":"TUR","TM":"TKM",
            "TV":"TUV","UG":"UGA","UA":"UKR","AE":"ARE","GB":"GBR","US":"USA","UY":"URY","UZ":"UZB","VU":"VUT","VE":"VEN",
            "VN":"VNM","YE":"YEM","ZM":"ZMB","ZW":"ZWE",
        }
        cf_df["iso3"] = cf_df["country"].map(_ISO2_TO_ISO3)

        v1, v2 = st.columns(2)
        with v1:
            fig = px.choropleth(
                cf_df, locations="iso3", locationmode="ISO-3",
                color="total_amount", hover_data=["country", "tx_count", "avg_amount"],
                title="Transaction Volume by Destination Country",
                color_continuous_scale="Viridis",
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, width='stretch')

        with v2:
            fig = px.bar(
                cf_df.nlargest(15, "total_amount"),
                x="country", y="total_amount", color="tx_count",
                title="Top 15 Destination Countries by Volume",
                color_continuous_scale="Blues",
            )
            st.plotly_chart(fig, width='stretch')

    st.markdown("---")

    if channel_risk:
        st.subheader("📡 Channel Risk Analysis (Graph)")
        ch_df = pd.DataFrame(channel_risk)

        v1, v2 = st.columns(2)
        with v1:
            fig = px.bar(
                ch_df, x="channel", y="avg_risk",
                color="high_risk_count", color_continuous_scale="OrRd",
                title="Average Risk Score by Channel",
            )
            st.plotly_chart(fig, width='stretch')

        with v2:
            fig = px.scatter(
                ch_df, x="tx_count", y="avg_risk", size="high_risk_count",
                text="channel", title="Channel: Volume vs Risk",
                color_discrete_sequence=["#636EFA"],
            )
            fig.update_traces(textposition="top center")
            st.plotly_chart(fig, width='stretch')

    # ── Currency Breakdown ───────────────────────────────────────────────
    currency_data = _cached_query(provider, CURRENCY_BREAKDOWN)
    if currency_data:
        st.markdown("---")
        st.subheader("💱 Currency Breakdown")
        st.caption("Transaction volumes, fraud rates, and risk distribution per currency")

        cur_df = pd.DataFrame(currency_data)
        cur_df["fraud_rate_pct"] = (cur_df["fraud_count"] / cur_df["tx_count"] * 100).round(2)
        cur_df["fraud_volume_pct"] = (cur_df["fraud_volume"] / cur_df["total_volume"].replace(0, 1) * 100).round(2)
        cur_df["symbol"] = cur_df["currency"].map(_CURRENCY_SYMBOLS).fillna(cur_df["currency"])

        # Metrics row
        mc = st.columns(min(len(cur_df), 5))
        for i, row in cur_df.head(5).iterrows():
            sym = row["symbol"]
            mc[i].metric(
                f"{row['currency']} ({sym})",
                f"{row['tx_count']:,} txns",
                delta=f"{row['fraud_rate_pct']:.1f}% fraud rate",
                delta_color="inverse",
            )

        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(
                cur_df, x="currency", y="tx_count",
                color="fraud_count", color_continuous_scale="OrRd",
                title="Transactions per Currency (colour = fraud count)",
                labels={"tx_count": "Transaction Count", "currency": "Currency"},
                text="tx_count",
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, width='stretch')

        with c2:
            fig = px.bar(
                cur_df, x="currency", y="total_volume",
                color="fraud_volume_pct", color_continuous_scale="Reds",
                title="Total Volume per Currency (colour = % fraud volume)",
                labels={"total_volume": "Total Volume", "currency": "Currency"},
                text=cur_df.apply(lambda r: f"{r['symbol']}{r['total_volume']:,.0f}", axis=1),
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, width='stretch')

        c3, c4 = st.columns(2)
        with c3:
            # Fraud vs legitimate stacked bar
            stack_df = cur_df[["currency", "fraud_count", "tx_count"]].copy()
            stack_df["legitimate"] = stack_df["tx_count"] - stack_df["fraud_count"]
            fig = go.Figure()
            fig.add_trace(go.Bar(name="Legitimate", x=stack_df["currency"], y=stack_df["legitimate"], marker_color="#636EFA"))
            fig.add_trace(go.Bar(name="Fraud", x=stack_df["currency"], y=stack_df["fraud_count"], marker_color="#EF553B"))
            fig.update_layout(barmode="stack", title="Fraud vs Legitimate Transactions by Currency",
                              yaxis_title="Count", xaxis_title="Currency")
            st.plotly_chart(fig, width='stretch')

        with c4:
            # Risk category distribution per currency
            risk_stack = cur_df[["currency", "high_risk_count", "medium_risk_count", "tx_count"]].copy()
            risk_stack["low_risk_count"] = risk_stack["tx_count"] - risk_stack["high_risk_count"] - risk_stack["medium_risk_count"]
            fig = go.Figure()
            fig.add_trace(go.Bar(name="High", x=risk_stack["currency"], y=risk_stack["high_risk_count"], marker_color="#EF553B"))
            fig.add_trace(go.Bar(name="Medium", x=risk_stack["currency"], y=risk_stack["medium_risk_count"], marker_color="#FFA15A"))
            fig.add_trace(go.Bar(name="Low", x=risk_stack["currency"], y=risk_stack["low_risk_count"], marker_color="#636EFA"))
            fig.update_layout(barmode="stack", title="Risk Category Distribution by Currency",
                              yaxis_title="Count", xaxis_title="Currency")
            st.plotly_chart(fig, width='stretch')

        with st.expander("📋 Currency Summary Table", expanded=False):
            display_cur = cur_df[["currency", "symbol", "tx_count", "total_volume", "avg_amount",
                                   "max_amount", "fraud_count", "fraud_rate_pct", "fraud_volume",
                                   "fraud_volume_pct", "avg_risk", "high_risk_count", "medium_risk_count"]].copy()
            display_cur.columns = [c.replace("_", " ").title() for c in display_cur.columns]
            st.dataframe(display_cur, width='stretch')

    if risk_data:
        st.markdown("---")
        st.subheader("🚨 High-Risk Transactions (from Graph)")
        risk_df = pd.DataFrame(risk_data)

        def _highlight_risk(val):
            colour = _RISK_COLOURS.get(val, "")
            return f"background-color: {colour}; color: white; font-weight: bold" if colour else ""

        display_cols = [c for c in [
            "transaction_id", "customer_id", "beneficiary", "amount",
            "risk_score", "risk_category", "triggered_rules", "channel",
        ] if c in risk_df.columns]

        styled = risk_df[display_cols].style.map(
            _highlight_risk, subset=["risk_category"]
        )
        st.dataframe(styled, width='stretch', height=400)

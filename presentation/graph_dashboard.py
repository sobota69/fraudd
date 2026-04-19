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
    CHANNEL_RISK,
    CROSS_BORDER_FLOWS,
    CUSTOMER_BENEFICIARY_NETWORK,
    CUSTOMER_RISK_PROFILE,
    CUSTOMER_SUBGRAPH,
    HIGH_RISK_TRANSACTIONS,
)

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


_NODE_COLOURS = {
    "Customer": "#4C8BF5",
    "CustomerAccount": "#7B61FF",
    "Transaction": "#00B894",
    "Transaction_HIGH": "#EF553B",
    "Transaction_MEDIUM": "#FFA15A",
    "Transaction_LOW": "#00B894",
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

    cust_ids = [r["customer_id"] for r in all_custs]

    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        selected_cid = st.selectbox("Select customer", cust_ids, key="graph_explorer_cid")
    with col2:
        physics_enabled = st.checkbox("Enable physics simulation", value=True, key="graph_explorer_physics")
    with col3:
        graph_height = st.slider("Graph height (px)", 400, 900, 620, step=20, key="graph_explorer_h")

    rows = _safe_query(provider, CUSTOMER_SUBGRAPH, customer_id=selected_cid)
    if not rows:
        st.warning("No transactions found for this customer.")
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

        risk_cat = r.get("risk_category") or ""
        tx_colour = _NODE_COLOURS.get(f"Transaction_{risk_cat}", _NODE_COLOURS["Transaction"])
        risk_score = r.get("risk_score") or 0
        amount = r.get("amount") or 0
        tx_size = 15 + min(float(amount) / 2000, 18)
        rules_txt = r.get("triggered_rules") or "none"
        fraud_flag = "⚠️ FRAUD" if r.get("is_fraud") else ""

        tx_title = (
            f"TX: {r['tx_id']}\n"
            f"Amount: £{amount:,.2f} {r.get('currency', '')}\n"
            f"Channel: {r.get('channel', '?')}\n"
            f"Risk: {risk_score} ({risk_cat})\n"
            f"Rules: {rules_txt}\n"
            f"{fraud_flag}"
        )

        if tx_id not in nodes_map:
            nodes_map[tx_id] = Node(
                id=tx_id,
                label=f"💳 £{amount:,.0f}",
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

    network_data = _safe_query(provider, CUSTOMER_BENEFICIARY_NETWORK)
    risk_data = _safe_query(provider, HIGH_RISK_TRANSACTIONS)
    customer_profiles = _safe_query(provider, CUSTOMER_RISK_PROFILE)
    hotspots = _safe_query(provider, BENEFICIARY_HOTSPOTS)
    country_flows = _safe_query(provider, CROSS_BORDER_FLOWS)
    channel_risk = _safe_query(provider, CHANNEL_RISK)

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
                labels={"total_volume": "Total Volume (£)", "avg_risk_score": "Avg Risk Score"},
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
        fmt = {k: v for k, v in {"Avg Risk Score": "{:.2f}", "Max Risk Score": "{:.2f}", "Total Volume": "£{:,.0f}"}.items() if k in table_df.columns}
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
    m4.metric("Total Volume", f"£{net_df['total_amount'].sum():,.0f}")

    st.markdown("---")

    st.subheader("🔗 Customer → Beneficiary Transfer Network")

    customers = net_df["customer_id"].unique().tolist()
    beneficiaries = net_df["beneficiary"].unique().tolist()

    all_nodes = []
    node_x, node_y = [], []
    node_text, node_color, node_size = [], [], []

    for i, cid in enumerate(customers):
        angle = 2 * math.pi * i / max(len(customers), 1)
        x = -2 + math.cos(angle) * 1.5
        y = math.sin(angle) * 1.5
        all_nodes.append(("customer", str(cid), x, y))
        node_x.append(x)
        node_y.append(y)
        cust_total = net_df[net_df["customer_id"] == cid]["total_amount"].sum()
        node_text.append(f"Customer {cid}<br>Volume: £{cust_total:,.0f}")
        node_color.append("#636EFA")
        node_size.append(12 + min(cust_total / 5000, 20))

    for i, bid in enumerate(beneficiaries):
        angle = 2 * math.pi * i / max(len(beneficiaries), 1)
        x = 2 + math.cos(angle) * 1.5
        y = math.sin(angle) * 1.5
        all_nodes.append(("beneficiary", bid, x, y))
        node_x.append(x)
        node_y.append(y)
        ben_total = net_df[net_df["beneficiary"] == bid]["total_amount"].sum()
        node_text.append(f"Beneficiary {bid[:12]}…<br>Received: £{ben_total:,.0f}")
        node_color.append("#EF553B")
        node_size.append(10 + min(ben_total / 5000, 18))

    node_map = {(n[0], n[1]): idx for idx, n in enumerate(all_nodes)}

    edge_x, edge_y = [], []
    for _, row in net_df.iterrows():
        src_idx = node_map.get(("customer", str(row["customer_id"])))
        tgt_idx = node_map.get(("beneficiary", row["beneficiary"]))
        if src_idx is not None and tgt_idx is not None:
            edge_x += [node_x[src_idx], node_x[tgt_idx], None]
            edge_y += [node_y[src_idx], node_y[tgt_idx], None]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y, mode="lines",
        line=dict(width=0.5, color="#888"), hoverinfo="none",
    ))
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y, mode="markers+text",
        marker=dict(size=node_size, color=node_color, line=dict(width=1, color="white")),
        text=[n[1][:8] for n in all_nodes],
        textposition="top center", textfont=dict(size=8),
        hovertext=node_text, hoverinfo="text",
    ))
    fig.update_layout(
        title="Transfer Network (🔵 Customers → 🔴 Beneficiaries)",
        showlegend=False, hovermode="closest",
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=500,
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

# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo>=0.10.0",
#     "pyiceberg[pyarrow,sqlalchemy]>=0.8.0",
#     "s3fs>=2024.1.0",
#     "pandas>=2.0.0",
#     "plotly>=5.0.0",
#     "nbformat>=5.0.0",
#     "requests>=2.31.0",
# ]
# ///

"""
User Activity Flow - Marimo Notebook
=====================================

This notebook queries Iceberg tables using PyIceberg and displays a User Activity Flow graph
showing how users move through different stages of the conversion funnel:
- Page Views
- Cart Events
- Purchases
"""

import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")


@app.cell
def _():

    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # 🔄 User Activity Flow

    This notebook queries Iceberg tables to visualize how users flow through
    the e-commerce conversion funnel: **Page Views → Cart Events → Purchases**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 🔧 Setup PyIceberg Connection
    """)
    return


@app.cell
def _():
    from pyiceberg.catalog import load_catalog
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from datetime import datetime
    import os

    # Load the Iceberg REST catalog (Lakekeeper)
    catalog = load_catalog(
        "lakekeeper",
        **{
            "uri": "http://127.0.0.1:8181/catalog",
            "type": "rest",
            "warehouse": "risingwave-warehouse",
            "s3.endpoint": "http://localhost:9301",
            "s3.access-key-id": "hummockadmin",
            "s3.secret-access-key": "hummockadmin",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        }
    )

    print("✅ PyIceberg catalog loaded successfully")
    print(f"   Catalog: lakekeeper")
    return catalog, go, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📊 Query Iceberg Tables
    """)
    return


@app.cell
def _(catalog):

    # List available namespaces
    print("🔍 Available namespaces:")
    namespaces = catalog.list_namespaces()
    for ns in namespaces:
        print(f"  - {ns}")
    return


@app.cell
def _(catalog):
    # List tables in public namespace
    print("📋 Tables in public namespace:")
    tables = catalog.list_tables("public")
    for table in tables:
        print(f"  - {table}")
    return


@app.cell
def _(catalog, pd):
    class ActivityFlowAnalyzer:
        def __init__(self, catalog):
            self.catalog = catalog
            self.page_views_table = None
            self.cart_events_table = None
            self.purchases_table = None
            self.page_views_df = None
            self.cart_events_df = None
            self.purchases_df = None
            self.metrics = {}

        def load_tables(self):
            """Load data from Iceberg tables."""
            try:
                # Load table references
                self.page_views_table = self.catalog.load_table("public.iceberg_page_views")
                self.cart_events_table = self.catalog.load_table("public.iceberg_cart_events")
                self.purchases_table = self.catalog.load_table("public.iceberg_purchases")

                # Scan tables and convert to pandas
                self.page_views_df = self.page_views_table.scan().to_pandas()
                self.cart_events_df = self.cart_events_table.scan().to_pandas()
                self.purchases_df = self.purchases_table.scan().to_pandas()

                print(f"✅ Page views loaded: {len(self.page_views_df)} rows")
                print(f"✅ Cart events loaded: {len(self.cart_events_df)} rows")
                print(f"✅ Purchases loaded: {len(self.purchases_df)} rows")

                return True
            except Exception as e:
                print(f"❌ Error loading tables: {e}")
                return False

        def compute_funnel_metrics(self):
            """Compute funnel conversion metrics."""
            if self.page_views_df is None or len(self.page_views_df) == 0:
                return None

            # Count unique users at each stage
            viewers = self.page_views_df["user_id"].nunique()
            carters = self.cart_events_df["user_id"].nunique() if self.cart_events_df is not None else 0
            purchasers = self.purchases_df["user_id"].nunique() if self.purchases_df is not None else 0

            # Calculate conversion rates
            view_to_cart_rate = (carters / viewers * 100) if viewers > 0 else 0
            cart_to_buy_rate = (purchasers / carters * 100) if carters > 0 else 0
            overall_conversion = (purchasers / viewers * 100) if viewers > 0 else 0

            self.metrics = {
                "viewers": viewers,
                "carters": carters,
                "purchasers": purchasers,
                "view_to_cart_rate": view_to_cart_rate,
                "cart_to_buy_rate": cart_to_buy_rate,
                "overall_conversion": overall_conversion,
            }

            return self.metrics

        def get_time_series_data(self):
            """Get time series data for each stage."""
            # Convert event_time to datetime if needed
            for df in [self.page_views_df, self.cart_events_df, self.purchases_df]:
                if df is not None and "event_time" in df.columns:
                    df["event_time"] = pd.to_datetime(df["event_time"])

            # Page views by time window (minute)
            if self.page_views_df is not None and len(self.page_views_df) > 0:
                self.page_views_df["time_bucket"] = self.page_views_df["event_time"].dt.floor("min")
                page_views_ts = self.page_views_df.groupby("time_bucket")["user_id"].nunique().reset_index()
                page_views_ts.columns = ["time_bucket", "viewers"]
            else:
                page_views_ts = pd.DataFrame(columns=["time_bucket", "viewers"])

            # Cart events by time window
            if self.cart_events_df is not None and len(self.cart_events_df) > 0:
                self.cart_events_df["time_bucket"] = self.cart_events_df["event_time"].dt.floor("min")
                cart_events_ts = self.cart_events_df.groupby("time_bucket")["user_id"].nunique().reset_index()
                cart_events_ts.columns = ["time_bucket", "carters"]
            else:
                cart_events_ts = pd.DataFrame(columns=["time_bucket", "carters"])

            # Purchases by time window
            if self.purchases_df is not None and len(self.purchases_df) > 0:
                self.purchases_df["time_bucket"] = self.purchases_df["event_time"].dt.floor("min")
                purchases_ts = self.purchases_df.groupby("time_bucket")["user_id"].nunique().reset_index()
                purchases_ts.columns = ["time_bucket", "purchasers"]
            else:
                purchases_ts = pd.DataFrame(columns=["time_bucket", "purchasers"])

            return page_views_ts, cart_events_ts, purchases_ts

        def get_user_flow_data(self):
            """Get data for Sankey diagram showing user flow."""
            # Find users who performed multiple actions
            page_user_ids = set(self.page_views_df["user_id"].unique()) if self.page_views_df is not None else set()
            cart_user_ids = set(self.cart_events_df["user_id"].unique()) if self.cart_events_df is not None else set()
            purchase_user_ids = set(self.purchases_df["user_id"].unique()) if self.purchases_df is not None else set()

            # Calculate flows
            only_page = len(page_user_ids - cart_user_ids - purchase_user_ids)
            page_to_cart = len((page_user_ids & cart_user_ids) - purchase_user_ids)
            page_to_purchase = len((page_user_ids & purchase_user_ids) - cart_user_ids)
            page_to_cart_to_purchase = len(page_user_ids & cart_user_ids & purchase_user_ids)

            return {
                "only_page": only_page,
                "page_to_cart": page_to_cart,
                "page_to_purchase": page_to_purchase,
                "page_to_cart_to_purchase": page_to_cart_to_purchase,
                "page_user_ids": len(page_user_ids),
                "cart_user_ids": len(cart_user_ids),
                "purchase_user_ids": len(purchase_user_ids),
            }

    # Create analyzer instance
    analyzer = ActivityFlowAnalyzer(catalog)
    print("✅ Activity Flow Analyzer initialized")
    return (analyzer,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📈 Load Data and Compute Metrics
    """)
    return


@app.cell
def _(analyzer, mo):
    # Load all tables
    success = analyzer.load_tables()
    metrics = None

    if success:
        # Compute funnel metrics
        metrics = analyzer.compute_funnel_metrics()

        # Display metrics
        if metrics:
            mo.md("### 🎯 Funnel Metrics")
            mo.hstack([
                mo.stat(label="Page Viewers", value=f"{metrics['viewers']:,}", caption="Unique users"),
                mo.stat(label="Cart Adders", value=f"{metrics['carters']:,}", caption=f"{metrics['view_to_cart_rate']:.1f}% conversion"),
                mo.stat(label="Purchasers", value=f"{metrics['purchasers']:,}", caption=f"{metrics['cart_to_buy_rate']:.1f}% conversion"),
                mo.stat(label="Overall Conversion", value=f"{metrics['overall_conversion']:.1f}%", caption="View to Purchase"),
            ])
    return (metrics,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📊 User Activity Flow Visualization
    """)
    return


@app.cell
def _(analyzer, go):
    flow_data = analyzer.get_user_flow_data()

    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        arrangement="snap",
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=[
                "Page Views",
                "Cart Events",
                "Purchases",
                "Drop-off",
                "Drop-off",
            ],
            color=["#3b82f6", "#f59e0b", "#10b981", "#ef4444", "#ef4444"]
        ),
        link=dict(
            source=[0, 0, 1, 1],
            target=[1, 3, 2, 4],
            value=[
                flow_data["page_to_cart"] + flow_data["page_to_cart_to_purchase"],
                flow_data["only_page"],
                flow_data["page_to_cart_to_purchase"],
                flow_data["page_to_cart"],
            ],
            color=[
                "rgba(59, 130, 246, 0.4)",
                "rgba(239, 68, 68, 0.4)",
                "rgba(16, 185, 129, 0.4)",
                "rgba(239, 68, 68, 0.4)",
            ]
        )
    )])

    fig.update_layout(
        title_text="User Activity Flow",
        font_size=12,
        height=500
    )

    fig.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📈 Funnel Conversion Chart
    """)
    return


@app.cell
def _(go, metrics):
    if metrics:
        # Create funnel chart
        funnel_fig = go.Figure(go.Funnel(
            y=["Page Views", "Cart Events", "Purchases"],
            x=[metrics["viewers"], metrics["carters"], metrics["purchasers"]],
            textposition="inside",
            textinfo="value+percent initial",
            opacity=0.65,
            marker=dict(
                color=["#3b82f6", "#f59e0b", "#10b981"],
                line=dict(width=[4, 2, 2], color=["#1d4ed8", "#d97706", "#059669"])
            ),
            connector=dict(line=dict(color="royalblue", dash="dot", width=3))
        ))

        funnel_fig.update_layout(
            title="Conversion Funnel",
            showlegend=False,
            height=400
        )

        funnel_fig.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📊 Time Series Activity
    """)
    return


@app.cell
def _(analyzer, go):
    page_ts, cart_ts, purchase_ts = analyzer.get_time_series_data()

    # Merge time series data
    if not page_ts.empty:
        time_series_df = page_ts.merge(
            cart_ts, on="time_bucket", how="outer"
        ).merge(
            purchase_ts, on="time_bucket", how="outer"
        ).fillna(0)

        time_series_df = time_series_df.sort_values("time_bucket")

        # Create line chart
        line_fig = go.Figure()

        line_fig.add_trace(go.Scatter(
            x=time_series_df["time_bucket"],
            y=time_series_df["viewers"],
            mode='lines+markers',
            name='Page Views',
            line=dict(color='#3b82f6', width=2)
        ))

        if "carters" in time_series_df.columns:
            line_fig.add_trace(go.Scatter(
                x=time_series_df["time_bucket"],
                y=time_series_df["carters"],
                mode='lines+markers',
                name='Cart Events',
                line=dict(color='#f59e0b', width=2)
            ))

        if "purchasers" in time_series_df.columns:
            line_fig.add_trace(go.Scatter(
                x=time_series_df["time_bucket"],
                y=time_series_df["purchasers"],
                mode='lines+markers',
                name='Purchases',
                line=dict(color='#10b981', width=2)
            ))

        line_fig.update_layout(
            title="User Activity Over Time",
            xaxis_title="Time",
            yaxis_title="Unique Users",
            height=400,
            hovermode='x unified'
        )

        line_fig.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📋 Detailed Breakdown
    """)
    return


@app.cell
def _(analyzer, pd):
    flow_results = analyzer.get_user_flow_data()

    # Create breakdown table
    breakdown_rows = [
        ["Page Views Only", flow_results["only_page"], f"{flow_results['only_page'] / flow_results['page_user_ids'] * 100:.1f}%"],
        ["Page → Cart", flow_results["page_to_cart"], f"{flow_results['page_to_cart'] / flow_results['page_user_ids'] * 100:.1f}%"],
        ["Page → Purchase (direct)", flow_results["page_to_purchase"], f"{flow_results['page_to_purchase'] / flow_results['page_user_ids'] * 100:.1f}%"],
        ["Page → Cart → Purchase", flow_results["page_to_cart_to_purchase"], f"{flow_results['page_to_cart_to_purchase'] / flow_results['page_user_ids'] * 100:.1f}%"],
    ]

    breakdown_df = pd.DataFrame(breakdown_rows, columns=["Stage", "Users", "Percentage"])
    breakdown_df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## 📝 Summary

    This notebook analyzed user activity flow through the e-commerce funnel:

    1. **Page Views**: Initial user engagement
    2. **Cart Events**: Product interest signals
    3. **Purchases**: Conversion events

    The visualizations show:
    - Sankey diagram of user flow between stages
    - Funnel chart showing conversion rates
    - Time series of activity over time
    - Detailed breakdown table

    Data source: Iceberg tables via PyIceberg (Lakekeeper catalog)
    """)
    return


if __name__ == "__main__":
    app.run()

import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc
from sqlalchemy import create_engine


# Database connection with connection pooling
def get_db_connection():
    return create_engine(
        "postgresql://root:root@localhost:4566/dev", connect_args={"connect_timeout": 5}
    )


# Fetch funnel data from database
def get_funnel_data():
    """Fetch funnel data from the database"""
    try:
        engine = get_db_connection()
        query = """
            SELECT 
                window_start,
                viewers,
                carters,
                purchasers,
                view_to_cart_rate,
                cart_to_buy_rate
            FROM funnel_summary
            ORDER BY window_start DESC
            LIMIT 100
        """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Database error: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error


# Create funnel chart using Plotly's dedicated funnel trace
def create_funnel_chart(df):
    if df.empty:
        return go.Figure()

    latest = df.iloc[0]

    # Funnel data - stages with proper order (Viewers at top)
    stages = ["Viewers", "Carters", "Purchasers"]
    values = [
        float(latest["viewers"]),
        float(latest["carters"]),
        float(latest["purchasers"]),
    ]

    # Calculate conversion rates
    cart_rate = (values[1] / values[0]) * 100 if values[0] > 0 else 0
    buy_rate = (values[2] / values[1]) * 100 if values[1] > 0 else 0

    fig = go.Figure(
        go.Funnel(
            y=stages,
            x=values,  # Use full values - text positioning controls display
            textinfo="value+percent initial",
            textposition="auto",  # Let Plotly automatically position text optimally
            marker=dict(
                color=["#636EFA", "#00CC96", "#FF6692"],
                line=dict(width=2, color="white"),
            ),
            connector=dict(line=dict(color="gray", width=1, dash="dot")),
            hovertemplate="<b>%{y}</b><br>Count: %{x}<br>Percent of initial: %{percent initial}<extra></extra>",
        )
    )

    # Add custom annotations for conversion rates - positioned further to the left
    max_val = max(values) if values else 1
    annotation_x = (
        max_val * -0.35
    )  # Position annotation further to the left of the bars

    fig.add_annotation(
        x=annotation_x,
        y=1,
        text=f"Cart Rate: {cart_rate:.1f}%",
        showarrow=False,
        font=dict(size=10, color="#333"),
        bgcolor="rgba(255,255,255,0.95)",
        bordercolor="#ddd",
        borderwidth=1,
    )

    fig.add_annotation(
        x=annotation_x,
        y=2,
        text=f"Buy Rate: {buy_rate:.1f}%",
        showarrow=False,
        font=dict(size=10, color="#333"),
        bgcolor="rgba(255,255,255,0.95)",
        bordercolor="#ddd",
        borderwidth=1,
    )

    fig.update_layout(
        title="Conversion Funnel",
        title_x=0.5,
        font=dict(size=14, family="Arial, sans-serif"),
        height=500,
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(
            l=0, r=0, t=120, b=150
        ),  # Zero left/right margins to use full width
        paper_bgcolor="rgba(0,0,0,0)",
    )

    return fig


# Create time series chart
def create_timeseries_chart(df):
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    # Add traces for each metric
    fig.add_trace(
        go.Scatter(
            x=df["window_start"],
            y=df["viewers"],
            mode="lines+markers",
            name="Viewers",
            line=dict(color="#636EFA", width=2),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["window_start"],
            y=df["carters"],
            mode="lines+markers",
            name="Carters",
            line=dict(color="#00CC96", width=2),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["window_start"],
            y=df["purchasers"],
            mode="lines+markers",
            name="Purchasers",
            line=dict(color="#FF6692", width=2),
        )
    )

    fig.update_layout(
        title="Funnel Metrics Over Time",
        title_x=0.5,
        xaxis_title="Time",
        yaxis_title="Count",
        hovermode="x unified",
        height=400,  # Set to 400
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )

    return fig


# Create conversion rates chart
def create_conversion_rates_chart(df):
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["window_start"],
            y=df["view_to_cart_rate"],
            mode="lines+markers",
            name="View to Cart Rate",
            line=dict(color="#FECB52", width=2),
            yaxis="y",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["window_start"],
            y=df["cart_to_buy_rate"],
            mode="lines+markers",
            name="Cart to Buy Rate",
            line=dict(color="#B6E880", width=2),
            yaxis="y",
        )
    )

    fig.update_layout(
        title="Conversion Rates Over Time",
        title_x=0.5,
        xaxis_title="Time",
        yaxis_title="Conversion Rate",
        hovermode="x unified",
        height=400,
        yaxis=dict(tickformat=".1%"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )

    return fig


# Create KPI cards
def create_kpi_cards(df):
    if df.empty:
        return html.Div("No data available")

    latest = df.iloc[0]
    prev = df.iloc[1] if len(df) > 1 else latest

    def calculate_change(current, previous):
        if previous == 0:
            return 0
        return ((current - previous) / previous) * 100

    viewers_change = calculate_change(latest["viewers"], prev["viewers"])
    carters_change = calculate_change(latest["carters"], prev["carters"])
    purchasers_change = calculate_change(latest["purchasers"], prev["purchasers"])
    cart_rate_change = calculate_change(
        latest["view_to_cart_rate"], prev["view_to_cart_rate"]
    )
    buy_rate_change = calculate_change(
        latest["cart_to_buy_rate"], prev["cart_to_buy_rate"]
    )

    cards = [
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4(f"{int(latest['viewers']):,}", className="card-title"),
                    html.P("Viewers", className="card-text"),
                    html.Small(
                        f"{viewers_change:+.1f}%",
                        className=f"text-{'success' if viewers_change >= 0 else 'danger'}",
                    ),
                ]
            ),
            color="primary",
            inverse=True,
        ),
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4(f"{int(latest['carters']):,}", className="card-title"),
                    html.P("Carters", className="card-text"),
                    html.Small(
                        f"{carters_change:+.1f}%",
                        className=f"text-{'success' if carters_change >= 0 else 'danger'}",
                    ),
                ]
            ),
            color="info",
            inverse=True,
        ),
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4(
                        f"{int(latest['purchasers']):,}",
                        className="card-title text-white",
                    ),
                    html.P("Purchasers", className="card-text text-white"),
                    html.Small(
                        f"{purchasers_change:+.1f}%",
                        className=f"text-{'light' if purchasers_change >= 0 else 'dark'}",
                    ),
                ]
            ),
            color="success",
        ),
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4(
                        f"{latest['view_to_cart_rate']:.1%}", className="card-title"
                    ),
                    html.P("View→Cart Rate", className="card-text"),
                    html.Small(
                        f"{cart_rate_change:+.1f}%",
                        className=f"text-{'success' if cart_rate_change >= 0 else 'danger'}",
                    ),
                ]
            ),
            color="warning",
            inverse=True,
        ),
        dbc.Card(
            dbc.CardBody(
                [
                    html.H4(
                        f"{latest['cart_to_buy_rate']:.1%}",
                        className="card-title text-white",
                    ),
                    html.P("Cart→Buy Rate", className="card-text text-white"),
                    html.Small(
                        f"{buy_rate_change:+.1f}%",
                        className=f"text-{'light' if buy_rate_change >= 0 else 'dark'}",
                    ),
                ]
            ),
            color="danger",
        ),
    ]

    return dbc.Row([dbc.Col(card, width=12, lg=2) for card in cards])


# Initialize app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Real-time E-commerce Funnel Dashboard"

# Layout
app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div(
                            [
                                html.Div(
                                    html.H1(
                                        "Real-time E-commerce Conversion Funnel",
                                        className="text-center mb-4",
                                    ),
                                    className="text-center",
                                ),
                                html.Div(
                                    id="datetime-display",
                                    className="text-muted text-center mb-4",
                                    style={"font-size": "18px", "font-weight": "bold"},
                                ),
                            ]
                        )
                    ],
                    width=12,
                )
            ]
        ),
        dbc.Row(
            [
                # Left side - Main dashboard content
                dbc.Col(
                    [
                        # KPI Cards
                        html.Div(id="kpi-cards", className="mb-4"),
                        # Auto-refresh indicator
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Div(
                                            [
                                                html.Span(
                                                    "Auto-refresh: ", className="me-2"
                                                ),
                                                html.Span(
                                                    id="last-update",
                                                    className="badge bg-success",
                                                ),
                                            ],
                                            className="text-end",
                                        )
                                    ],
                                    width=12,
                                )
                            ],
                            className="mb-3",
                        ),
                        # Charts
                        dbc.Row(
                            [
                                dbc.Col(dcc.Graph(id="funnel-chart"), width=12, lg=4),
                                dbc.Col(
                                    dcc.Graph(id="timeseries-chart"), width=12, lg=8
                                ),
                            ],
                            className="mb-4",
                            style={"margin-left": "0", "padding-left": "0"},
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    dcc.Graph(id="conversion-rates-chart"),
                                    width=12,
                                    lg=12,
                                )
                            ],
                            className="mb-4",
                            style={"margin-left": "0", "padding-left": "0"},
                        ),
                    ],
                    width=12,
                ),
            ]
        ),
        # Interval component for auto-refresh
        dcc.Interval(
            id="interval-component",
            interval=1 * 1000,  # 1 second
            n_intervals=0,
        ),
    ],
    fluid=True,
)


# Callbacks
@app.callback(
    [
        Output("kpi-cards", "children"),
        Output("funnel-chart", "figure"),
        Output("timeseries-chart", "figure"),
        Output("conversion-rates-chart", "figure"),
        Output("last-update", "children"),
        Output("datetime-display", "children"),
    ],
    [
        Input("interval-component", "n_intervals"),
    ],
)
def update_dashboard(n):
    try:
        df = get_funnel_data()

        kpi_cards = create_kpi_cards(df)
        funnel_fig = create_funnel_chart(df)
        timeseries_fig = create_timeseries_chart(df)
        conversion_rates_fig = create_conversion_rates_chart(df)
        last_update_time = datetime.now().strftime("%H:%M:%S")

        # Format datetime display
        datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return (
            kpi_cards,
            funnel_fig,
            timeseries_fig,
            conversion_rates_fig,
            last_update_time,
            datetime_str,
        )

    except Exception as e:
        error_msg = f"Error updating dashboard: {str(e)}"
        return (
            html.Div(error_msg, className="alert alert-danger"),
            go.Figure().add_annotation(text=error_msg, x=0.5, y=0.5, showarrow=False),
            go.Figure().add_annotation(text=error_msg, x=0.5, y=0.5, showarrow=False),
            go.Figure().add_annotation(text=error_msg, x=0.5, y=0.5, showarrow=False),
            "Error",
            "",
        )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)

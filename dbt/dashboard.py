import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc
from sqlalchemy import create_engine


# Producer state management
producer_running = True  # Start producer automatically
producer_process = None
producer_output = [
    f"[{datetime.now().strftime('%H:%M:%S')}] Dashboard initialized - producer will start automatically"
]


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
            FROM funnel
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
    values = [latest["viewers"], latest["carters"], latest["purchasers"]]

    # Calculate conversion rates
    cart_rate = (values[1] / values[0]) * 100 if values[0] > 0 else 0
    buy_rate = (values[2] / values[1]) * 100 if values[1] > 0 else 0

    fig = go.Figure(
        go.Funnel(
            y=stages,
            x=values,
            textinfo="value+percent initial",
            marker=dict(
                color=["#636EFA", "#00CC96", "#FF6692"],
                line=dict(width=2, color="white"),
            ),
            connector=dict(line=dict(color="gray", width=1, dash="dot")),
            hovertemplate="<b>%{y}</b><br>Count: %{x}<br>Percent of initial: %{percent initial}<extra></extra>",
        )
    )

    # Add custom annotations for conversion rates on the far left side
    fig.add_annotation(
        x=-20,
        y=1,
        text=f"Cart Rate: {cart_rate:.1f}%",
        showarrow=False,
        font=dict(size=11, color="#333"),
        bgcolor="rgba(255,255,255,0.95)",
        bordercolor="#ddd",
        borderwidth=1,
    )

    fig.add_annotation(
        x=-20,
        y=2,
        text=f"Buy Rate: {buy_rate:.1f}%",
        showarrow=False,
        font=dict(size=11, color="#333"),
        bgcolor="rgba(255,255,255,0.95)",
        bordercolor="#ddd",
        borderwidth=1,
    )

    fig.update_layout(
        title="Conversion Funnel",
        title_x=0.5,
        font=dict(size=14, family="Arial, sans-serif"),
        height=400,
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=150, r=50, t=120, b=50),  # Reduced left margin to center funnel
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
        height=400,
        legend=dict(x=0.02, y=0.98),
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
        legend=dict(x=0.02, y=0.98),
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
                    html.H4(f"{int(latest['purchasers']):,}", className="card-title"),
                    html.P("Purchasers", className="card-text"),
                    html.Small(
                        f"{purchasers_change:+.1f}%",
                        className=f"text-{'success' if purchasers_change >= 0 else 'danger'}",
                    ),
                ]
            ),
            color="success",
            inverse=True,
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
                        f"{latest['cart_to_buy_rate']:.1%}", className="card-title"
                    ),
                    html.P("Cart→Buy Rate", className="card-text"),
                    html.Small(
                        f"{buy_rate_change:+.1f}%",
                        className=f"text-{'success' if buy_rate_change >= 0 else 'danger'}",
                    ),
                ]
            ),
            color="danger",
            inverse=True,
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
                    lg=8,
                ),
                # Right side - Producer controls and output
                dbc.Col(
                    [
                        html.H4("Producer Controls", className="mb-3"),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.Button(
                                            "Start Producer",
                                            id="start-producer-btn",
                                            color="success",
                                            n_clicks=0,
                                            className="me-2",
                                        ),
                                        dbc.Button(
                                            "Stop Producer",
                                            id="stop-producer-btn",
                                            color="danger",
                                            n_clicks=0,
                                            disabled=True,
                                        ),
                                    ],
                                    width=12,
                                ),
                            ]
                        ),
                        html.Hr(),
                        html.H4("Producer Output", className="mb-3"),
                        html.Div(
                            id="producer-output",
                            className="producer-output-scroll",
                            style={
                                "height": "400px",
                                "overflow-y": "auto",
                                "font-family": "monospace",
                                "font-size": "11px",
                                "scroll-behavior": "smooth",
                            },
                        ),
                    ],
                    width=12,
                    lg=4,
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
        Output("start-producer-btn", "disabled"),
        Output("stop-producer-btn", "disabled"),
        Output("producer-output", "children"),
    ],
    [
        Input("interval-component", "n_intervals"),
        Input("start-producer-btn", "n_clicks"),
        Input("stop-producer-btn", "n_clicks"),
    ],
)
def update_dashboard(n, start_clicks, stop_clicks):
    global producer_running, producer_process, producer_output

    # Handle producer controls
    ctx = dash.callback_context
    if ctx.triggered:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if button_id == "start-producer-btn" and start_clicks > 0:
            if not producer_running:
                producer_running = True
                producer_output.append(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Starting producer..."
                )
                # Start producer in background
                import threading

                producer_thread = threading.Thread(target=run_producer)
                producer_thread.daemon = True
                producer_thread.start()

        elif button_id == "stop-producer-btn" and stop_clicks > 0:
            if producer_running:
                producer_running = False
                producer_output.append(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Stopping producer..."
                )

    try:
        df = get_funnel_data()

        kpi_cards = create_kpi_cards(df)
        funnel_fig = create_funnel_chart(df)
        timeseries_fig = create_timeseries_chart(df)
        conversion_rates_fig = create_conversion_rates_chart(df)
        last_update_time = datetime.now().strftime("%H:%M:%S")

        # Format producer output for display - exactly 25 lines, no scrolling
        output_lines = producer_output[-25:]  # Get last 25 lines
        # Pad with empty lines if we have fewer than 25 lines
        while len(output_lines) < 25:
            output_lines.insert(0, "")

        output_display = html.Div(
            [
                html.P(
                    line,
                    style={
                        "margin": "1px 0",
                        "color": "darkgreen"
                        if "Starting" in line or "Stopping" in line
                        else "black",
                    },
                )
                for line in output_lines
            ],
            id="producer-output",
            style={
                "height": "300px",  # Increased height to fit all 25 lines comfortably
                "overflow": "hidden",  # No scrollbar
                "font-family": "monospace",
                "font-size": "10px",
                "line-height": "1.1",
                "white-space": "pre",
            },
        )

        # Format datetime display
        datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return (
            kpi_cards,
            funnel_fig,
            timeseries_fig,
            conversion_rates_fig,
            last_update_time,
            datetime_str,  # Add datetime display
            producer_running,  # Disable start button when running
            not producer_running,  # Disable stop button when not running
            output_display,
        )

    except Exception as e:
        error_msg = f"Error updating dashboard: {str(e)}"
        return (
            html.Div(error_msg, className="alert alert-danger"),
            go.Figure().add_annotation(text=error_msg, x=0.5, y=0.5, showarrow=False),
            go.Figure().add_annotation(text=error_msg, x=0.5, y=0.5, showarrow=False),
            go.Figure().add_annotation(text=error_msg, x=0.5, y=0.5, showarrow=False),
            "Error",
            producer_running,
            not producer_running,
            html.Div("Error loading dashboard", className="alert alert-danger"),
        )


def run_producer():
    """Run the producer process and capture output"""
    global producer_output, producer_running

    import json
    import time
    import random
    from datetime import datetime
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=["localhost:19092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    TOPICS = ["page_views", "cart_events", "purchases"]

    producer_output.append(
        f"[{datetime.now().strftime('%H:%M:%S')}] Producer started successfully"
    )

    try:
        while producer_running:
            user_id = random.randint(1, 100)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 1. Page Views (High volume)
            view_data = {
                "user_id": user_id,
                "page_id": f"page_{random.randint(1, 20)}",
                "event_time": current_time,
            }
            producer.send("page_views", value=view_data)

            # 2. Add to Cart (Medium volume)
            if random.random() < 0.3:
                cart_data = {
                    "user_id": user_id,
                    "item_id": f"item_{random.randint(100, 200)}",
                    "event_time": current_time,
                }
                producer.send("cart_events", value=cart_data)

            # 3. Purchases (Low volume)
            if random.random() < 0.1:
                purchase_data = {
                    "user_id": user_id,
                    "amount": round(random.uniform(10, 500), 2),
                    "event_time": current_time,
                }
                producer.send("purchases", value=purchase_data)

            producer_output.append(f"Sent events for User {user_id} at {current_time}")
            time.sleep(1)

    except Exception as e:
        producer_output.append(f"[ERROR] {str(e)}")
    finally:
        producer.close()
        producer_output.append(
            f"[{datetime.now().strftime('%H:%M:%S')}] Producer stopped"
        )


# Add auto-scrolling JavaScript
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <script>
            function scrollToBottom() {
                const output = document.getElementById('producer-output');
                if (output) {
                    output.scrollTop = output.scrollHeight;
                    output.scrollIntoView({ behavior: 'smooth', block: 'end' });
                }
            }
            
            // MutationObserver to detect content changes and scroll
            function setupAutoScroll() {
                const output = document.getElementById('producer-output');
                if (output) {
                    const observer = new MutationObserver(function(mutations) {
                        mutations.forEach(function(mutation) {
                            if (mutation.type === 'childList') {
                                scrollToBottom();
                            }
                        });
                    });
                    
                    observer.observe(output, {
                        childList: true,
                        subtree: true,
                        characterData: true
                    });
                }
            }
        </script>
    </head>
    <body>
        {%app_entry%}
        <script>
            // Initialize auto-scroll when DOM is loaded
            document.addEventListener('DOMContentLoaded', function() {
                setupAutoScroll();
            });
        </script>
        {%config%}
        {%scripts%}
        {%renderer%}
    </body>
</html>
"""


def start_producer_on_init():
    """Start the producer when the app initializes"""
    global producer_running, producer_output
    if producer_running:
        producer_output.append(
            f"[{datetime.now().strftime('%H:%M:%S')}] Auto-starting producer..."
        )
        import threading

        producer_thread = threading.Thread(target=run_producer)
        producer_thread.daemon = True
        producer_thread.start()


# Start producer automatically when app initializes
start_producer_on_init()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)

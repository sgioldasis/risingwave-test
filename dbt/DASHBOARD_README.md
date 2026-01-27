# Real-time E-commerce Conversion Funnel Dashboard

A Plotly Dash dashboard for monitoring real-time e-commerce conversion funnel data from RisingWave.

## Features

- **Real-time Monitoring**: Auto-refreshes every 10 seconds
- **KPI Cards**: Displays key metrics with period-over-period changes
- **Funnel Visualization**: Interactive funnel chart showing conversion stages
- **Time Series Charts**: Historical trends for all funnel metrics
- **Conversion Rate Tracking**: View-to-cart and cart-to-buy conversion rates over time

## Dashboard Components

### KPI Cards
- Viewers (total users who viewed products)
- Carters (users who added items to cart)
- Purchasers (users who completed purchases)
- View→Cart Rate (percentage of viewers who added to cart)
- Cart→Buy Rate (percentage of carters who purchased)

### Charts
1. **Funnel Chart**: Visual representation of conversion stages
2. **Time Series Chart**: Historical trends for viewers, carters, and purchasers
3. **Conversion Rates Chart**: View-to-cart and cart-to-buy rates over time

## Setup

### Prerequisites
- RisingWave database running on localhost:4566
- Python 3.13+
- Virtual environment (recommended)

### Installation

1. **Clone/Download this project**
2. **Create and activate virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Database Setup

Ensure your RisingWave database has the `funnel` table with the following structure:
```sql
CREATE TABLE funnel (
    window_start TIMESTAMP,
    viewers INTEGER,
    carters INTEGER,
    purchasers INTEGER,
    view_to_cart_rate FLOAT,
    cart_to_buy_rate FLOAT
);
```

## Running the Dashboard

### Option 1: Use the startup script
```bash
./run_dashboard.sh
```

### Option 2: Run directly
```bash
python dashboard.py
```

The dashboard will be available at: **http://localhost:8050**

## Database Query

The dashboard uses this query to fetch funnel data:
```sql
SELECT * FROM funnel ORDER BY window_start DESC LIMIT 20;
```

## Customization

### Refresh Interval
To change the auto-refresh interval, modify this line in `dashboard.py`:
```python
interval=10*1000,  # Change 10 to desired seconds
```

### Database Connection
Update the database connection settings in the `get_db_connection()` function:
```python
return psycopg2.connect(
    host="your_host",
    port="your_port",
    database="your_database",
    user="your_user"
)
```

### Styling
The dashboard uses Bootstrap theme. You can modify the theme by changing:
```python
external_stylesheets=[dbc.themes.BOOTSTRAP]
```

Available themes: BOOTSTRAP, CERULEAN, COSMO, CYBORG, DARKLY, FLATLY, JOURNAL, LITERA, LUMEN, LUX, MATERIA, MINTY, PULSE, SANDSTONE, SIMPLEX, SKETCHY, SLATE, SOLAR, SPACELAB, UNITED, YETI

## Troubleshooting

### Common Issues

1. **Database connection error**:
   - Ensure RisingWave is running on localhost:4566
   - Check database credentials
   - Verify the `funnel` table exists

2. **Import errors**:
   - Make sure all dependencies are installed: `pip install -r requirements.txt`
   - Activate the virtual environment

3. **Port already in use**:
   - Change the port in `dashboard.py`: `app.run_server(port=8051)`

### Debug Mode

To run in debug mode with more detailed error messages:
```python
app.run_server(debug=True, host="0.0.0.0", port=8050)
```

## Data Format

The dashboard expects data in this format:
```
window_start     | viewers | carters | purchasers | view_to_cart_rate | cart_to_buy_rate
---------------------+---------+---------+------------+-------------------+------------------
2026-01-27 09:51:00 |      18 |       9 |          1 |              0.50 |             0.11
```

- `window_start`: Timestamp for the data window
- `viewers`: Count of users who viewed products
- `carters`: Count of users who added items to cart
- `purchasers`: Count of users who completed purchases
- `view_to_cart_rate`: Ratio of carters to viewers (0-1)
- `cart_to_buy_rate`: Ratio of purchasers to carters (0-1)
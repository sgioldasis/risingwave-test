# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "trino[sqlalchemy]",
#     "pandas",
# ]
# ///

import marimo

__generated_with = "0.23.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from sqlalchemy import create_engine, text
    import time

    return create_engine, mo, pd, text, time


@app.cell
def _(create_engine):
    # SQLAlchemy engines for clean DataFrame reads
    iceberg_engine = create_engine("trino://trino@localhost:9080/datalake/public")
    risingwave_engine = create_engine("trino://trino@localhost:9080/risingwave/public")
    return iceberg_engine, risingwave_engine


@app.cell
def _(iceberg_engine, mo, pd, text):
    # EDIT THIS QUERY to update Iceberg data
    # Then run this cell (Shift+Enter or click the run button)

    country_name = 'Hellas'

    # Execute UPDATE using SQLAlchemy connection (cleaner - uses same engine!)
    with iceberg_engine.connect() as conn:
        conn.execute(text(f"UPDATE iceberg_countries SET country_name = '{country_name}' WHERE country = 'GR'"))
        conn.commit()

    # View current Iceberg countries using SQLAlchemy (cleaner!)
    ref_df = pd.read_sql(
        text("SELECT country, country_name FROM iceberg_countries WHERE country = 'GR'"),
        iceberg_engine
    )

    mo.ui.table(ref_df)
    return


@app.cell
def _(mo):
    # Simple button to manually refresh data
    refresh_button = mo.ui.button(label="🔄 Refresh data")
    return (refresh_button,)


@app.cell
def _(mo, pd, refresh_button, risingwave_engine, text, time):
    # Click the refresh button above to update this data

    # This reference makes the cell re-run when button is clicked
    refresh_button.value

    # Query RisingWave
    rw_df = pd.read_sql(
        text("""
            SELECT window_start, country, country_name, viewers, carters, purchasers
            FROM funnel_summary_with_country
            ORDER BY window_start DESC
            LIMIT 50
        """),
        risingwave_engine
    )

    # Show last refresh time and data
    mo.vstack([
        mo.hstack([refresh_button, mo.md(f"Last updated: {time.strftime('%H:%M:%S')}")]),
        mo.ui.table(rw_df)
    ])
    return


@app.cell
def _(mo):
    # Click to re-query the current data file count (useful for demoing compaction)
    files_refresh_button = mo.ui.button(label="🔄 Refresh data file count")
    return (files_refresh_button,)


@app.cell
def _(files_refresh_button, iceberg_engine, mo, pd, text, time):
    # Re-runs whenever the button is clicked
    files_refresh_button.value

    # Trino's Iceberg connector exposes a "$files" metadata table with
    # one row per data file currently referenced by the table snapshot.
    # rw_managed_funnel is the streaming sink from RisingWave — its file
    # count grows as the producer writes and shrinks as RisingWave's
    # managed compaction kicks in.
    files_df = pd.read_sql(
        text('SELECT file_path, record_count, file_size_in_bytes FROM "rw_managed_funnel$files"'),
        iceberg_engine,
    )

    mo.vstack([
        mo.hstack([
            files_refresh_button,
            mo.md(f"Last checked: {time.strftime('%H:%M:%S')}"),
        ]),
        mo.md(f"**`rw_managed_funnel` data file count:** {len(files_df)}"),
        mo.ui.table(files_df),
    ])
    return


if __name__ == "__main__":
    app.run()

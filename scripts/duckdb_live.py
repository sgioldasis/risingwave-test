"""Interactive DuckDB-over-Flight session for the live StarRocks demo
(companion to scripts/starrocks_duckdb_demo.py -- reuse its endpoint logic).

Launch from the repo root:

    python scripts/duckdb_live.py        # devbox shell (venv already active)
    .venv/bin/python scripts/duckdb_live.py   # plain host shell

(no -i: the script hosts its own SQL-aware console; -i would chain the
interpreter's plain PyREPL on top once this session ends)

That drops you into a Python REPL with the federated Delta source already
pulled over Flight SQL and registered zero-copy in DuckDB (as delta_source),
plus three helpers for running the demo live:

    star("<sql>")   push a query DOWN to StarRocks over Flight SQL; the Arrow
                    result is registered in DuckDB (zero-copy) under an
                    auto name (duck_1, duck_2, ... -- or pass name="x") and
                    returned as a DuckDB relation
    duck("<sql>")   run DuckDB-LOCAL SQL over whatever is registered
    tables()        list what is registered right now

Type SQL, get answers -- Ctrl-D (or exit()) to leave.
"""

import base64
import code
import os
import re
import select
import sys

import adbc_driver_flightsql.dbapi as flight_sql
import duckdb

from starrocks_duckdb_demo import FEDERATED_TABLE, flight_uri

_auth = base64.b64encode(b"root:").decode()
_uri = flight_uri()
ddb = duckdb.connect()
_flight = flight_sql.connect(
    uri=_uri,
    db_kwargs={"adbc.flight.sql.authorization_header": f"Basic {_auth}"},
)
_ordinal = [0]


def star(sql: str, name: str | None = None):
    """Run <sql> on StarRocks (Flight SQL), register the Arrow result in
    DuckDB zero-copy, and return it as a queryable relation."""
    with _flight.cursor() as cur:
        cur.execute(sql)
        arrow = cur.fetch_arrow_table()
    if name is None:
        _ordinal[0] += 1
        name = f"duck_{_ordinal[0]}"
    ddb.register(name, arrow)
    print(f"registered {arrow.num_rows:,} rows ({arrow.nbytes / 1024 / 1024:.1f} MiB) as {name!r}")
    return ddb.table(name)


def duck(sql: str):
    """Run DuckDB-local SQL over the registered (Arrow) tables."""
    return ddb.sql(sql)


def tables() -> None:
    """Show what is currently registered in DuckDB."""
    for (name,) in ddb.sql(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY 1"
    ).fetchall():
        n = ddb.sql(f'SELECT COUNT(*) FROM "{name}"').fetchall()[0][0]
        print(f"  {name:<20} {n:,} rows")


_SQL_STARTERS = {"select", "show", "describe", "desc", "explain", "summarize"}

_PASTE_START = "\x1b[200~"
_PASTE_END = "\x1b[201~"


class _SqlConsole(code.InteractiveConsole):
    """A REPL that runs bare SQL statements on DuckDB -- type SQL directly
    at the prompt (no duck() wrapper needed), or Python as usual. Lines
    starting with a SQL keyword are executed on DuckDB and their result
    printed; everything else goes through the normal Python REPL. (Words
    that collide with Python -- with/from -- are deliberately excluded, so
    plain Python entry is never hijacked.)

    Multi-line SQL works: a statement starting with a SQL keyword stays open
    and gathers continuation lines until it ends with ';', a blank line is
    typed, or a new SQL statement starts."""

    def __init__(self, **kwargs):
        super().__init__(local_exit=True, **kwargs)
        self._sql_buffer = None

    def _run_sql(self, sql: str) -> None:
        try:
            print(duck(sql))
        except Exception as exc:
            print(f"SQL error: {exc}")

    def runsource(self, source, filename="<console>", symbol="single"):
        # Copying an example out of the banner (or any transcript) usually
        # includes the `>>> ` / `... ` prompt prefixes -- strip a leading one
        # so the paste compiles as the statement it shows.
        source = re.sub(r"^(\s*)(>>>|\.\.\.) ?", "", source, count=1)
        stripped = source.strip()
        first = stripped.split(maxsplit=1)[0].lower() if stripped else ""

        if self._sql_buffer is not None:
            if not stripped:                       # blank line = run the statement
                self._run_sql(self._sql_buffer)
                self._sql_buffer = None
                return False
            if first in _SQL_STARTERS:             # a new statement began -- run what we have
                self._run_sql(self._sql_buffer)
                self._sql_buffer = None
            elif stripped.startswith(("exit(", "quit(")):   # never trap the way out
                self._sql_buffer = None
                return super().runsource(source, filename, symbol)
            else:                                  # continuation line
                self._sql_buffer += "\n" + stripped
                if stripped.endswith(";"):
                    self._run_sql(self._sql_buffer)
                    self._sql_buffer = None
                return False

        if first in _SQL_STARTERS and not stripped.endswith(":"):
            if stripped.endswith(";"):
                self._run_sql(stripped)
            else:
                # Open a multi-line statement: runs on ';' or a blank line
                # (SQL has no completeness marker, so a pasted statement's
                # last line needs one extra Enter -- same as psql/DuckDB CLI).
                print("   ...continuing -- end with ';' or a blank line to run")
                self._sql_buffer = stripped
            return False
        return super().runsource(source, filename, symbol)

    @staticmethod
    def _input_ready(timeout: float = 2.0) -> bool:
        # Used to read a bracketed-paste atomically: keep pulling lines
        # while the stream keeps feeding them.
        ready, _, _ = select.select([sys.stdin], [], [], timeout)
        return bool(ready)

    def raw_input(self, prompt=""):
        """Read one line in canonical mode -- the tty driver itself handles
        backspace (PyREPL's raw-mode line editor is what mangles it), and
        after a paste the buffer holds only visible characters, so backspace
        always erases what you see. Continuation lines are read silently:
        a pasted statement must never collect a stray prompt between its
        lines (no suppression window can guarantee that, since the prompt
        prints before we know whether a line is coming), so neither ps2 nor
        the ps1 of an open SQL statement is ever printed -- the opening line
        already announced the continuation. If a terminal emits
        bracketed-paste markers anyway, the whole paste is read atomically
        and the markers stripped."""
        if prompt == sys.ps2 or self._sql_buffer is not None:
            prompt = ""                    # continuation line -- no prompt, ever
        if prompt:
            sys.stdout.write(prompt)
            sys.stdout.flush()
        line = sys.stdin.readline()
        if not line:
            raise EOFError
        if _PASTE_START not in line and _PASTE_END not in line:
            return line.rstrip("\n")
        chunks = [line]
        while _PASTE_END not in chunks[-1]:
            if not self._input_ready(2.0):
                break
            nxt = sys.stdin.readline()
            if not nxt:
                break
            chunks.append(nxt)
        return (
            "".join(chunks)
            .replace(_PASTE_START, "")
            .replace(_PASTE_END, "")
            .replace("\x1b[200", "")
            .replace("\x1b[201", "")
            .rstrip("\n")
        )


_UC_WEEKLY_ROLLUP = """
    SELECT DATE_TRUNC('week', window_date) AS week,
           SUM(viewers) AS viewers, SUM(purchasers) AS purchasers,
           ROUND(SUM(purchasers) / NULLIF(SUM(viewers), 0) * 100, 2) AS conv_pct
    FROM databricks_uc.sr_poc_external.funnel_summary_historical
    GROUP BY 1
"""


def _setup() -> None:
    star(f"SELECT * FROM {FEDERATED_TABLE}", name="delta_source")
    try:
        star(_UC_WEEKLY_ROLLUP, name="uc_funnel")
    except Exception as exc:
        # UC token expiry is the known cause -- fix live with:
        #   REFRESH EXTERNAL TABLE databricks_uc.sr_poc_external.funnel_summary_historical
        # then re-run beat 4a (the star(...) call below) by hand.
        print(f"  uc_funnel NOT pre-loaded: {exc}\n")
    print(
        "\n"
        "DuckDB over Arrow Flight SQL -- LIVE SESSION (build 2026-11-17.5)\n"
        "==================================================================\n"
        f"Flight endpoint: {_uri}\n"
        f"Pre-loaded: delta_source  ({FEDERATED_TABLE}, pushed down over Flight)\n"
        "            uc_funnel     (databricks_uc funnel_summary_historical, weekly rollup)\n"
        "\n"
        "  star(\"<sql>\")   run on StarRocks; Arrow result registered in DuckDB (zero-copy)\n"
        "  duck(\"<sql>\")   DuckDB-local SQL over what is registered\n"
        "  tables()        what is registered right now\n"
        "\n"
        "At the >>> prompt type SQL DIRECTLY (it runs on DuckDB) or Python.\n"
        "Multi-line SQL is fine: end it with ';' or a blank line to run.\n"
        "Multi-line examples paste cleanly -- each line continues the statement;\n"
        "it runs when the last line lands, with no prompts between lines.\n"
        "Backspace edits exactly what you see.\n"
        "Stuck at a ... prompt (incomplete statement)? Ctrl-C aborts back to >>>.\n"
        "\n"
        "  # profile the federation's make-up -- DuckDB-local over the Arrow buffers\n"
        "  >>> SELECT description, COUNT(*) n FROM delta_source GROUP BY 1 ORDER BY n DESC LIMIT 5\n"
        "\n"
        "  # query a kafka-fed StarRocks table live -- pushed DOWN to StarRocks over Flight SQL\n"
        "  >>> star(\"SELECT country, SUM(viewers) AS v FROM sr_local_db.funnel_summary_direct GROUP BY 1\")\n"
        "\n"
        "  # re-cut that StarRocks result locally in DuckDB -- both directions of the pairing\n"
        "  >>> SELECT country, v FROM duck_1 ORDER BY v DESC LIMIT 3\n"
        "\n"
        "A fourth beat -- a query over a Databricks Unity Catalog table (the shape to\n"
        "use when the table is LARGE; here it is demo-sized at ~2k rows, but the\n"
        "mechanics are identical at hundreds of millions):\n"
        "\n"
        "  # uc_funnel is ALREADY REGISTERED (4a ran at startup), so beat 4b below\n"
        "  # works immediately. Re-type 4a only to show the mechanism:\n"
        "  # StarRocks federates the UC Iceberg table IN PLACE on ADLS (zero DBU)\n"
        "  # and does the heavy part: partition pruning, column pruning, MPP rollup.\n"
        "  # ONLY the aggregate crosses Flight -- 6 rows, a fraction of a KiB. Filter\n"
        "  # the partition column, name the columns, aggregate server-side; never\n"
        "  # SELECT * a big table (that streams every row into this laptop's RAM).\n"
        "  >>> star(\"\"\"\n"
        "        SELECT DATE_TRUNC('week', window_date) AS week,\n"
        "               SUM(viewers) AS viewers, SUM(purchasers) AS purchasers,\n"
        "               ROUND(SUM(purchasers) / NULLIF(SUM(viewers), 0) * 100, 2) AS conv_pct\n"
        "        FROM databricks_uc.sr_poc_external.funnel_summary_historical\n"
        "        GROUP BY 1\n"
        "      \"\"\", name=\"uc_funnel\")\n"
        "\n"
        "  # 4b: DuckDB re-cuts locally -- no lake, no wire.\n"
        "  >>> SELECT ROUND(AVG(conv_pct), 2) AS avg_conv, MIN(conv_pct) AS worst, MAX(conv_pct) AS best FROM uc_funnel\n"
        "\n"
        "Ctrl-D (or exit()) to leave."
    )


if __name__ == "__main__":
    _setup()
    _SqlConsole(locals=dict(globals(), **locals())).interact(
        banner="", exitmsg="session ended\n"
    )
    # Without this, `python -i` would chain the interpreter's own plain
    # PyREPL after this console ends (bare SQL would hit Python SyntaxError
    # there, filename <python-input-N>). os._exit instead of SystemExit so
    # the -i session can't resurrect a REPL and shows no traceback -- but
    # flush stdout first, os._exit discards buffered output.
    sys.stdout.flush()
    os._exit(0)

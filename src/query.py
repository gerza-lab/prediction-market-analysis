"""Natural language query interface for NBA prediction market data."""

import os
import re
from pathlib import Path

import anthropic
import duckdb


# NBA-related event ticker patterns
NBA_PATTERNS = [
    "MVENBASINGLEGAME",  # Single-Game Props
    "NBAGAME",           # Games
    "NBASERIES",         # Series
    "NBATOTAL",          # Totals
    "NBASPREAD",         # Spreads
    "NBAEAST",           # Eastern Conference
    "NBAWEST",           # Western Conference
    "NBAFINALSMVP",      # Finals MVP
    "NBAFINALSEXACT",    # Finals Exact
    "NBAMVP",            # MVP
    "NBAWINS",           # Win Totals
]

# SQL filter for NBA markets
NBA_FILTER = " OR ".join([f"event_ticker LIKE '{p}%'" for p in NBA_PATTERNS])
NBA_FILTER = f"({NBA_FILTER} OR event_ticker LIKE 'NBA%')"

SCHEMA_DESCRIPTION = """
You have access to two tables of NBA prediction market data from Kalshi:

## Table: nba_markets
Markets (prediction contracts) for NBA basketball events.

Columns:
- ticker (VARCHAR): Unique market identifier (e.g., 'NBAGAME-25JAN24-LAL-BOS')
- event_ticker (VARCHAR): Category grouping (e.g., 'NBAGAME', 'NBASPREAD', 'NBASERIES')
- market_type (VARCHAR): Usually 'binary'
- title (VARCHAR): Human-readable market title (e.g., 'Will the Lakers beat the Celtics?')
- yes_sub_title (VARCHAR): Label for YES outcome
- no_sub_title (VARCHAR): Label for NO outcome
- status (VARCHAR): 'open', 'closed', or 'finalized'
- yes_bid (INTEGER): Current YES bid price in cents (1-99)
- yes_ask (INTEGER): Current YES ask price in cents (1-99)
- no_bid (INTEGER): Current NO bid price in cents (1-99)
- no_ask (INTEGER): Current NO ask price in cents (1-99)
- last_price (INTEGER): Last traded price in cents (1-99)
- volume (INTEGER): Total contracts traded
- volume_24h (INTEGER): Contracts traded in last 24 hours
- open_interest (INTEGER): Outstanding contracts
- result (VARCHAR): 'yes', 'no', or '' (empty if not settled)
- created_time (TIMESTAMP): When market was created
- open_time (TIMESTAMP): When trading opened
- close_time (TIMESTAMP): When trading closes/closed

## Table: nba_trades
Individual trades executed on NBA markets.

Columns:
- trade_id (VARCHAR): Unique trade identifier
- ticker (VARCHAR): Which market (joins to nba_markets.ticker)
- count (INTEGER): Number of contracts traded
- yes_price (INTEGER): Price paid for YES side in cents (1-99)
- no_price (INTEGER): Price paid for NO side in cents (always 100 - yes_price)
- taker_side (VARCHAR): 'yes' or 'no' - which side the taker bought
- created_time (TIMESTAMP): When the trade occurred

## Market Types (event_ticker prefixes):
- NBAGAME: Game outcome markets (will team X beat team Y?)
- NBASPREAD: Point spread markets
- NBATOTAL: Over/under total points
- NBASERIES: Playoff series markets
- NBAMVP: MVP award markets
- NBAWINS: Season win total markets
- NBAEAST/NBAWEST: Conference championship markets
- NBAFINALSMVP: Finals MVP markets

## Notes:
- Prices are in cents (1-99 range), representing probability percentage
- A yes_price of 65 means 65 cents = 65% implied probability
- volume represents total contracts, multiply by average price for approximate USD
- result shows final outcome for settled markets
"""

SYSTEM_PROMPT = f"""You are a SQL query generator for NBA prediction market data. You translate natural language questions into DuckDB SQL queries.

{SCHEMA_DESCRIPTION}

IMPORTANT RULES:
1. ONLY output a single valid DuckDB SQL query - no explanations, no markdown, just the raw SQL
2. Use standard SQL syntax compatible with DuckDB
3. For text searches, use ILIKE for case-insensitive matching
4. Limit results to 25 rows unless the user asks for more or asks for aggregations
5. When showing monetary values, prices are in cents - you can divide by 100 for dollars
6. For time-based analysis, use DuckDB date functions like DATE_TRUNC, EXTRACT, etc.
7. Always include relevant columns that help answer the question
8. If asked about teams, search the 'title' column which contains team names
9. For win rate calculations: result = 'yes' means YES won, result = 'no' means NO won
10. Join tables on ticker when you need both market info and trade data

Example questions and queries:

Q: "What are the highest volume NBA markets?"
A: SELECT ticker, title, volume, result FROM nba_markets ORDER BY volume DESC LIMIT 10

Q: "How many trades happened on Lakers games?"
A: SELECT COUNT(*) as trade_count, SUM(count) as total_contracts FROM nba_trades t JOIN nba_markets m ON t.ticker = m.ticker WHERE m.title ILIKE '%lakers%'

Q: "What's the average trade size for NBA spread markets?"
A: SELECT AVG(count) as avg_size, COUNT(*) as num_trades FROM nba_trades t JOIN nba_markets m ON t.ticker = m.ticker WHERE m.event_ticker LIKE 'NBASPREAD%'
"""


class NBAQueryEngine:
    """Natural language query interface for NBA prediction market data."""

    def __init__(self, data_dir: Path | None = None):
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = data_dir or self.base_dir / "data"
        self.markets_dir = self.data_dir / "markets"
        self.trades_dir = self.data_dir / "trades"
        self.client = anthropic.Anthropic()
        self._con = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection with NBA data views."""
        if self._con is not None:
            return self._con

        self._con = duckdb.connect()

        # Create view for NBA markets
        self._con.execute(f"""
            CREATE VIEW nba_markets AS
            SELECT * FROM '{self.markets_dir}/*.parquet'
            WHERE {NBA_FILTER}
        """)

        # Get list of NBA market tickers for filtering trades
        nba_tickers = self._con.execute(
            "SELECT DISTINCT ticker FROM nba_markets"
        ).fetchall()
        nba_ticker_list = [t[0] for t in nba_tickers]

        if nba_ticker_list:
            # Create view for NBA trades using the ticker list
            ticker_filter = ", ".join([f"'{t}'" for t in nba_ticker_list])
            self._con.execute(f"""
                CREATE VIEW nba_trades AS
                SELECT * FROM '{self.trades_dir}/*.parquet'
                WHERE ticker IN ({ticker_filter})
            """)
        else:
            # Empty trades view if no NBA markets found
            self._con.execute(f"""
                CREATE VIEW nba_trades AS
                SELECT * FROM '{self.trades_dir}/*.parquet'
                WHERE 1=0
            """)

        return self._con

    def _generate_sql(self, question: str) -> str:
        """Use Claude to generate SQL from natural language."""
        message = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": question}
            ]
        )
        sql = message.content[0].text.strip()
        # Remove markdown code blocks if present
        sql = re.sub(r'^```sql?\n?', '', sql)
        sql = re.sub(r'\n?```$', '', sql)
        return sql.strip()

    def query(self, question: str) -> tuple[str, list[tuple], list[str]]:
        """
        Execute a natural language query against NBA data.

        Returns:
            tuple of (sql, results, column_names)
        """
        con = self._get_connection()
        sql = self._generate_sql(question)

        try:
            result = con.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return sql, rows, columns
        except Exception as e:
            raise ValueError(f"Query failed: {e}\n\nGenerated SQL:\n{sql}")

    def get_stats(self) -> dict:
        """Get basic statistics about the NBA data."""
        con = self._get_connection()

        stats = {}

        # Market counts
        market_counts = con.execute("""
            SELECT
                COUNT(*) as total_markets,
                COUNT(CASE WHEN result != '' THEN 1 END) as settled_markets,
                COUNT(CASE WHEN status = 'open' THEN 1 END) as open_markets
            FROM nba_markets
        """).fetchone()
        stats['total_markets'] = market_counts[0]
        stats['settled_markets'] = market_counts[1]
        stats['open_markets'] = market_counts[2]

        # Trade counts
        trade_counts = con.execute("""
            SELECT COUNT(*) as total_trades, SUM(count) as total_contracts
            FROM nba_trades
        """).fetchone()
        stats['total_trades'] = trade_counts[0]
        stats['total_contracts'] = trade_counts[1] or 0

        # Market types
        market_types = con.execute("""
            SELECT
                CASE
                    WHEN event_ticker LIKE 'NBAGAME%' THEN 'Games'
                    WHEN event_ticker LIKE 'NBASPREAD%' THEN 'Spreads'
                    WHEN event_ticker LIKE 'NBATOTAL%' THEN 'Totals'
                    WHEN event_ticker LIKE 'NBASERIES%' THEN 'Series'
                    WHEN event_ticker LIKE 'NBAMVP%' THEN 'MVP'
                    WHEN event_ticker LIKE 'NBAWINS%' THEN 'Win Totals'
                    ELSE 'Other'
                END as market_type,
                COUNT(*) as count
            FROM nba_markets
            GROUP BY market_type
            ORDER BY count DESC
        """).fetchall()
        stats['market_types'] = {row[0]: row[1] for row in market_types}

        return stats

    def close(self):
        """Close the database connection."""
        if self._con:
            self._con.close()
            self._con = None


def format_results(rows: list[tuple], columns: list[str], max_width: int = 120) -> str:
    """Format query results as a nice table."""
    if not rows:
        return "No results found."

    # Calculate column widths
    col_widths = []
    for i, col in enumerate(columns):
        max_val_width = max(len(str(row[i])) for row in rows) if rows else 0
        col_widths.append(max(len(col), min(max_val_width, 50)))

    # Build header
    header = " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(columns))
    separator = "-+-".join("-" * w for w in col_widths)

    # Build rows
    formatted_rows = []
    for row in rows:
        formatted_row = " | ".join(
            str(val)[:50].ljust(col_widths[i]) for i, val in enumerate(row)
        )
        formatted_rows.append(formatted_row)

    return f"{header}\n{separator}\n" + "\n".join(formatted_rows)


def interactive_query():
    """Run an interactive query session."""
    print("NBA Prediction Market Query Interface")
    print("=" * 40)
    print("Ask questions about NBA prediction markets in plain English.")
    print("Type 'quit' or 'exit' to stop, 'stats' for data overview.\n")

    engine = NBAQueryEngine()

    # Check if data exists
    if not engine.markets_dir.exists() or not engine.trades_dir.exists():
        print("Error: Data not found. Run 'uv run main.py setup' first.")
        return

    try:
        # Show initial stats
        stats = engine.get_stats()
        print(f"Loaded {stats['total_markets']:,} NBA markets with {stats['total_trades']:,} trades")
        print(f"Market types: {stats['market_types']}\n")

        while True:
            try:
                question = input("Question: ").strip()
            except EOFError:
                break

            if not question:
                continue

            if question.lower() in ('quit', 'exit', 'q'):
                break

            if question.lower() == 'stats':
                stats = engine.get_stats()
                print(f"\nNBA Data Statistics:")
                print(f"  Total markets: {stats['total_markets']:,}")
                print(f"  Settled markets: {stats['settled_markets']:,}")
                print(f"  Open markets: {stats['open_markets']:,}")
                print(f"  Total trades: {stats['total_trades']:,}")
                print(f"  Total contracts: {stats['total_contracts']:,}")
                print(f"  Market types: {stats['market_types']}\n")
                continue

            try:
                sql, rows, columns = engine.query(question)
                print(f"\nSQL: {sql}\n")
                print(format_results(rows, columns))
                print(f"\n({len(rows)} rows)\n")
            except ValueError as e:
                print(f"\nError: {e}\n")
            except Exception as e:
                print(f"\nUnexpected error: {e}\n")

    finally:
        engine.close()


def single_query(question: str):
    """Execute a single query and print results."""
    engine = NBAQueryEngine()

    if not engine.markets_dir.exists() or not engine.trades_dir.exists():
        print("Error: Data not found. Run 'uv run main.py setup' first.")
        return

    try:
        sql, rows, columns = engine.query(question)
        print(f"SQL: {sql}\n")
        print(format_results(rows, columns))
        print(f"\n({len(rows)} rows)")
    except ValueError as e:
        print(f"Error: {e}")
    finally:
        engine.close()

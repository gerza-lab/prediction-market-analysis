"""Natural language query interface for prediction market data."""

import re
from pathlib import Path

import anthropic
import duckdb


# Sports-related event ticker patterns organized by sport
SPORTS_PATTERNS = {
    "NFL": [
        "MVENFLMULTIGAME", "MVENFLSINGLEGAME", "NFLGAME", "NFLSPREAD", "NFLTOTAL",
        "NFLANYTD", "NFL2TD", "NFLFIRSTTD", "NFLMVP", "NFLRSHYDS", "NFLRECYDS",
        "NFLPASSYDS", "NFLOROTY", "NFLOPOTY", "NFLCOTY", "NFLWINS", "NFLPLAYOFF",
        "NFLNFCCHAMP", "NFLAFCCHAMP", "NFLNFCWEST", "NFLNFCEAST", "NFLNFCNORTH",
        "NFLAFCWEST", "NFLAFCNORTH", "NFLAFCSOUTH", "NFL", "NFC", "AFC", "SB",
    ],
    "NBA": [
        "MVENBASINGLEGAME", "NBAGAME", "NBASERIES", "NBATOTAL", "NBASPREAD",
        "NBAEAST", "NBAWEST", "NBAFINALSMVP", "NBAFINALSEXACT", "NBAMVP", "NBAWINS", "NBA",
    ],
    "MLB": [
        "MLBGAME", "MLBSERIES", "MLBSERIESEXACT", "MLBTOTAL", "MLBSPREAD",
        "MLBHRDERBY", "MLBASGAME", "MLBAL", "MLBNL", "MLBALEAST", "MLBALMVP",
        "MLBNLROTY", "MLB",
    ],
    "NHL": [
        "NHLGAME", "NHLSERIES", "NHLTOTAL", "NHLSPREAD", "NHLEAST", "NHLWEST",
        "NHL4NATIONS", "NHL",
    ],
    "NCAA Football": [
        "NCAAFGAME", "NCAAFSPREAD", "NCAAFTOTAL", "NCAAFPLAYOFF", "NCAAFB12",
        "NCAAFB10", "NCAAFACC", "NCAAFSEC", "NCAAF", "HEISMAN",
    ],
    "NCAA Basketball": [
        "NCAAMBGAME", "NCAAMBTOTAL", "NCAAMBSPREAD", "NCAAMBACHAMP", "NCAAMB",
        "MARMAD", "WMARMAD",
    ],
    "WNBA": ["WNBAGAME", "WNBA"],
    "Tennis": [
        "ATPMATCH", "WTAMATCH", "ATPFINALS", "ATPDOUBLES", "ATPIT", "ATP",
        "WTAIT", "WTA", "WMENSINGLES", "WWOMENSINGLES", "USOMENSINGLES",
        "USOWOMENSINGLES", "FOMENSINGLES", "FOWOMENSINGLES", "FOMEN", "FOWOMEN",
        "DAVISCUPMATCH",
    ],
    "Golf": [
        "PGATOUR", "PGARYDERMATCH", "PGARYDER", "PGARYDERCUPD1", "PGARYDERTOP",
        "PGA", "MASTERS", "USOPEN", "THEOPEN", "GENESISINVITATIONAL", "LIVTOUR",
    ],
    "Soccer": [
        "EPLGAME", "EPLTOP4", "EPL", "PREMIERLEAGUE", "UCLGAME", "UCLROUND",
        "UCL", "UEFACL", "LALIGAGAME", "SERIEAGAME", "BUNDESLIGAGAME",
        "LIGUE1GAME", "MLSGAME", "FIFAGAME", "CLUBWCGAME", "CLUBWC",
        "EFLCHAMPIONSHIPGAME", "EFLCUPGAME", "UELGAME", "EUROLEAGUEGAME",
        "SUPERLIGGAME", "EREDIVISIEGAME", "LIGAPORTUGALGAME", "BRASILEIROGAME",
        "MENWORLDCUP", "BALLONDOR",
    ],
    "UFC/Boxing": ["UFCFIGHT", "UFC", "BOXING"],
    "Racing": [
        "F1RACE", "F1RACEPODIUM", "F1", "NASCARRACE", "NASCAR", "INDY500",
    ],
}

# Flatten all sports patterns
ALL_SPORTS_PATTERNS = []
for patterns in SPORTS_PATTERNS.values():
    ALL_SPORTS_PATTERNS.extend(patterns)


def build_filter(patterns: list[str]) -> str:
    """Build SQL filter from pattern list."""
    conditions = [f"event_ticker LIKE '{p}%'" for p in patterns]
    return "(" + " OR ".join(conditions) + ")"


SPORTS_SCHEMA = """
You have access to two tables of sports prediction market data from Kalshi:

## Table: markets
Markets (prediction contracts) for sports events.

Columns:
- ticker (VARCHAR): Unique market identifier (e.g., 'NFLGAME-25JAN24-KC-BUF')
- event_ticker (VARCHAR): Category grouping (e.g., 'NFLGAME', 'NBASPREAD', 'MLBSERIES')
- market_type (VARCHAR): Usually 'binary'
- title (VARCHAR): Human-readable market title (e.g., 'Will the Chiefs beat the Bills?')
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

## Table: trades
Individual trades executed on sports markets.

Columns:
- trade_id (VARCHAR): Unique trade identifier
- ticker (VARCHAR): Which market (joins to markets.ticker)
- count (INTEGER): Number of contracts traded
- yes_price (INTEGER): Price paid for YES side in cents (1-99)
- no_price (INTEGER): Price paid for NO side in cents (always 100 - yes_price)
- taker_side (VARCHAR): 'yes' or 'no' - which side the taker bought
- created_time (TIMESTAMP): When the trade occurred

## Sports Categories (event_ticker prefixes):

### NFL (Football)
- NFLGAME: Game outcomes
- NFLSPREAD: Point spreads
- NFLTOTAL: Over/under totals
- NFLMVP, NFLOROTY, NFLOPOTY: Awards
- SB: Super Bowl

### NBA (Basketball)
- NBAGAME: Game outcomes
- NBASPREAD: Point spreads
- NBATOTAL: Over/under totals
- NBASERIES: Playoff series
- NBAMVP: MVP award
- NBAEAST/NBAWEST: Conference winners

### MLB (Baseball)
- MLBGAME: Game outcomes
- MLBSERIES: Playoff series
- MLBTOTAL/MLBSPREAD: Totals and spreads

### NHL (Hockey)
- NHLGAME: Game outcomes
- NHLSERIES: Playoff series
- NHLTOTAL/NHLSPREAD: Totals and spreads

### NCAA (College)
- NCAAFGAME/NCAAFSPREAD/NCAAFTOTAL: College football
- NCAAMBGAME/NCAAMBSPREAD: College basketball
- MARMAD/WMARMAD: March Madness

### Other Sports
- Tennis: ATP*, WTA*, *SINGLES
- Golf: PGA*, MASTERS, USOPEN, THEOPEN
- Soccer: EPL*, UCL*, MLS*, FIFA*
- UFC/Boxing: UFC*, BOXING
- Racing: F1*, NASCAR*, INDY500

## Notes:
- Prices are in cents (1-99 range), representing probability percentage
- A yes_price of 65 means 65 cents = 65% implied probability
- volume represents total contracts traded
- result shows final outcome for settled markets ('yes' or 'no')
"""

SPORTS_SYSTEM_PROMPT = f"""You are a SQL query generator for sports prediction market data. You translate natural language questions into DuckDB SQL queries.

{SPORTS_SCHEMA}

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
11. To filter by sport, use event_ticker patterns (e.g., LIKE 'NFL%' for NFL, LIKE 'NBA%' for NBA)

Example questions and queries:

Q: "What are the highest volume sports markets?"
A: SELECT ticker, title, volume, result FROM markets ORDER BY volume DESC LIMIT 10

Q: "Which sport has the most trading volume?"
A: SELECT CASE WHEN event_ticker LIKE 'NFL%' OR event_ticker LIKE 'NFC%' OR event_ticker LIKE 'AFC%' OR event_ticker LIKE 'SB%' THEN 'NFL' WHEN event_ticker LIKE 'NBA%' THEN 'NBA' WHEN event_ticker LIKE 'MLB%' THEN 'MLB' WHEN event_ticker LIKE 'NHL%' THEN 'NHL' WHEN event_ticker LIKE 'NCAAF%' OR event_ticker LIKE 'HEISMAN%' THEN 'NCAA Football' WHEN event_ticker LIKE 'NCAAMB%' OR event_ticker LIKE '%MARMAD%' THEN 'NCAA Basketball' ELSE 'Other' END as sport, SUM(volume) as total_volume, COUNT(*) as num_markets FROM markets GROUP BY sport ORDER BY total_volume DESC

Q: "What's the average trade size for NFL games?"
A: SELECT AVG(count) as avg_size, COUNT(*) as num_trades FROM trades t JOIN markets m ON t.ticker = m.ticker WHERE m.event_ticker LIKE 'NFLGAME%'

Q: "Show me Lakers vs Celtics games"
A: SELECT ticker, title, volume, result FROM markets WHERE title ILIKE '%lakers%' AND title ILIKE '%celtics%' ORDER BY volume DESC LIMIT 10
"""


class SportsQueryEngine:
    """Natural language query interface for sports prediction market data."""

    def __init__(self, data_dir: Path | None = None, sport: str | None = None):
        """
        Initialize the query engine.

        Args:
            data_dir: Path to data directory (default: ./data)
            sport: Optional sport filter (e.g., 'NBA', 'NFL'). If None, queries all sports.
        """
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = data_dir or self.base_dir / "data"
        self.markets_dir = self.data_dir / "markets"
        self.trades_dir = self.data_dir / "trades"
        self.sport = sport.upper() if sport else None
        self.client = anthropic.Anthropic()
        self._con = None

    def _get_filter(self) -> str:
        """Get the SQL filter for the selected sport(s)."""
        if self.sport and self.sport in SPORTS_PATTERNS:
            return build_filter(SPORTS_PATTERNS[self.sport])
        return build_filter(ALL_SPORTS_PATTERNS)

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection with data views."""
        if self._con is not None:
            return self._con

        self._con = duckdb.connect()
        filter_sql = self._get_filter()

        # Create view for markets
        self._con.execute(f"""
            CREATE VIEW markets AS
            SELECT * FROM '{self.markets_dir}/*.parquet'
            WHERE {filter_sql}
        """)

        # Get list of market tickers for filtering trades
        tickers = self._con.execute(
            "SELECT DISTINCT ticker FROM markets"
        ).fetchall()
        ticker_list = [t[0] for t in tickers]

        if ticker_list:
            ticker_filter = ", ".join([f"'{t}'" for t in ticker_list])
            self._con.execute(f"""
                CREATE VIEW trades AS
                SELECT * FROM '{self.trades_dir}/*.parquet'
                WHERE ticker IN ({ticker_filter})
            """)
        else:
            self._con.execute(f"""
                CREATE VIEW trades AS
                SELECT * FROM '{self.trades_dir}/*.parquet'
                WHERE 1=0
            """)

        return self._con

    def _generate_sql(self, question: str) -> str:
        """Use Claude to generate SQL from natural language."""
        message = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=SPORTS_SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": question}
            ]
        )
        sql = message.content[0].text.strip()
        sql = re.sub(r'^```sql?\n?', '', sql)
        sql = re.sub(r'\n?```$', '', sql)
        return sql.strip()

    def query(self, question: str) -> tuple[str, list[tuple], list[str]]:
        """
        Execute a natural language query against sports data.

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
        """Get basic statistics about the sports data."""
        con = self._get_connection()
        stats = {}

        market_counts = con.execute("""
            SELECT
                COUNT(*) as total_markets,
                COUNT(CASE WHEN result != '' THEN 1 END) as settled_markets,
                COUNT(CASE WHEN status = 'open' THEN 1 END) as open_markets
            FROM markets
        """).fetchone()
        stats['total_markets'] = market_counts[0]
        stats['settled_markets'] = market_counts[1]
        stats['open_markets'] = market_counts[2]

        trade_counts = con.execute("""
            SELECT COUNT(*) as total_trades, SUM(count) as total_contracts
            FROM trades
        """).fetchone()
        stats['total_trades'] = trade_counts[0]
        stats['total_contracts'] = trade_counts[1] or 0

        # Sports breakdown
        sports_breakdown = con.execute("""
            SELECT
                CASE
                    WHEN event_ticker LIKE 'NFL%' OR event_ticker LIKE 'NFC%' OR event_ticker LIKE 'AFC%' OR event_ticker LIKE 'SB%' THEN 'NFL'
                    WHEN event_ticker LIKE 'NBA%' THEN 'NBA'
                    WHEN event_ticker LIKE 'MLB%' THEN 'MLB'
                    WHEN event_ticker LIKE 'NHL%' THEN 'NHL'
                    WHEN event_ticker LIKE 'NCAAF%' OR event_ticker LIKE 'HEISMAN%' THEN 'NCAA Football'
                    WHEN event_ticker LIKE 'NCAAMB%' OR event_ticker LIKE '%MARMAD%' THEN 'NCAA Basketball'
                    WHEN event_ticker LIKE 'WNBA%' THEN 'WNBA'
                    WHEN event_ticker LIKE 'ATP%' OR event_ticker LIKE 'WTA%' OR event_ticker LIKE '%SINGLES%' THEN 'Tennis'
                    WHEN event_ticker LIKE 'PGA%' OR event_ticker LIKE 'MASTERS%' OR event_ticker LIKE '%OPEN%' OR event_ticker LIKE 'LIV%' THEN 'Golf'
                    WHEN event_ticker LIKE 'EPL%' OR event_ticker LIKE 'UCL%' OR event_ticker LIKE 'MLS%' OR event_ticker LIKE 'FIFA%' OR event_ticker LIKE '%LIGA%' OR event_ticker LIKE '%SERIE%' OR event_ticker LIKE 'BUNDESLIGA%' THEN 'Soccer'
                    WHEN event_ticker LIKE 'UFC%' OR event_ticker LIKE 'BOXING%' THEN 'UFC/Boxing'
                    WHEN event_ticker LIKE 'F1%' OR event_ticker LIKE 'NASCAR%' OR event_ticker LIKE 'INDY%' THEN 'Racing'
                    ELSE 'Other'
                END as sport,
                COUNT(*) as count,
                SUM(volume) as total_volume
            FROM markets
            GROUP BY sport
            ORDER BY total_volume DESC
        """).fetchall()
        stats['sports'] = {row[0]: {'markets': row[1], 'volume': row[2]} for row in sports_breakdown}

        return stats

    def close(self):
        """Close the database connection."""
        if self._con:
            self._con.close()
            self._con = None


# Backward compatibility alias
NBAQueryEngine = SportsQueryEngine


def format_results(rows: list[tuple], columns: list[str], max_width: int = 120) -> str:
    """Format query results as a nice table."""
    if not rows:
        return "No results found."

    col_widths = []
    for i, col in enumerate(columns):
        max_val_width = max(len(str(row[i])) for row in rows) if rows else 0
        col_widths.append(max(len(col), min(max_val_width, 50)))

    header = " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(columns))
    separator = "-+-".join("-" * w for w in col_widths)

    formatted_rows = []
    for row in rows:
        formatted_row = " | ".join(
            str(val)[:50].ljust(col_widths[i]) for i, val in enumerate(row)
        )
        formatted_rows.append(formatted_row)

    return f"{header}\n{separator}\n" + "\n".join(formatted_rows)


def interactive_query(sport: str | None = None):
    """Run an interactive query session."""
    sport_label = sport.upper() if sport else "Sports"
    print(f"{sport_label} Prediction Market Query Interface")
    print("=" * 45)
    print(f"Ask questions about {sport_label.lower()} prediction markets in plain English.")
    print("Type 'quit' or 'exit' to stop, 'stats' for data overview.\n")

    engine = SportsQueryEngine(sport=sport)

    if not engine.markets_dir.exists() or not engine.trades_dir.exists():
        print("Error: Data not found. Run 'uv run main.py setup' first.")
        return

    try:
        stats = engine.get_stats()
        print(f"Loaded {stats['total_markets']:,} markets with {stats['total_trades']:,} trades")
        if stats['sports']:
            sports_summary = ", ".join([f"{k}: {v['markets']}" for k, v in list(stats['sports'].items())[:5]])
            print(f"Sports: {sports_summary}\n")

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
                print(f"\n{sport_label} Data Statistics:")
                print(f"  Total markets: {stats['total_markets']:,}")
                print(f"  Settled markets: {stats['settled_markets']:,}")
                print(f"  Open markets: {stats['open_markets']:,}")
                print(f"  Total trades: {stats['total_trades']:,}")
                print(f"  Total contracts: {stats['total_contracts']:,}")
                print(f"\n  By Sport:")
                for sport_name, data in stats['sports'].items():
                    print(f"    {sport_name}: {data['markets']:,} markets, {data['volume']:,} volume")
                print()
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


def single_query(question: str, sport: str | None = None):
    """Execute a single query and print results."""
    engine = SportsQueryEngine(sport=sport)

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

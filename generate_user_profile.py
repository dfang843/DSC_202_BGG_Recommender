import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------
# DB Connection
# ----------------------------
engine = create_engine("postgresql://username:password@localhost:<host port>/<db name>")            # Update with actual credentials

# ----------------------------
# Helper: Validate game names
# ----------------------------
def validate_games(game_names: list[str]) -> None:
    """
    Checks all provided game names exist in the DB (case-insensitive).
    Raises ValueError for any name not found.
    """
    with engine.connect() as conn:
        for name in game_names:
            result = conn.execute(text("""
                SELECT 1 FROM games_base
                WHERE LOWER(name) = LOWER(:name)
            """), {"name": name}).fetchone()

            if result is None:
                raise ValueError(
                    f"Game '{name}' not found in the database. "
                    f"Please check the spelling and try again."
                )

# ----------------------------
# Helper: Get user game inputs
# ----------------------------
def get_game_inputs() -> tuple[list[str], list[str]]:
    """
    Interactively collect liked and disliked game names from the user.
    Validates each batch against the DB before accepting.
    """
    def collect_games(label: str) -> list[str]:
        while True:
            raw = input(f"\nEnter {label} games (comma-separated, 0–5):\n> ").strip()
            names = [n.strip() for n in raw.split(",") if n.strip()]
            if not 0 <= len(names) <= 5:
                print(f"  Please enter between 0 and 5 {label} games.")
                continue
            try:
                validate_games(names)
                return names
            except ValueError as e:
                print(f"  Error: {e}")

    liked    = collect_games("LIKED")
    disliked = collect_games("DISLIKED")
    return liked, disliked

# ----------------------------
# Helper: Get optional constraints
# ----------------------------
CONSTRAINT_COMPONENT_MAP = {
    "max_playtime":   "playtime",
    "min_rating":     "rating",
    "min_players":    "players",
    "max_players":    "players",
    "max_complexity": "complexity",
}

def get_constraints() -> dict:
    """
    Interactively prompt user for optional hard constraints.
    Returns a dict of active constraints e.g. {"max_playtime": 90, "min_rating": 7.0}
    """
    print("\nOptional constraints (press Enter to skip any):")
    constraints = {}

    prompts = {
        "max_playtime":   ("Max playtime in minutes (e.g. 90): ",  int),
        "min_rating":     ("Min BGG rating (e.g. 7.0): ",          float),
        "min_players":    ("Min player count (e.g. 2): ",          int),
        "max_players":    ("Max player count (e.g. 4): ",          int),
        "max_complexity": ("Max complexity 1.0–5.0 (e.g. 3.0): ", float),
    }

    for key, (prompt_text, cast) in prompts.items():
        while True:
            val = input(f"  {prompt_text}").strip()
            if val == "":
                break
            try:
                constraints[key] = cast(val)
                break
            except ValueError:
                print("    Invalid input — expected a number.")

    return constraints

# ----------------------------
# Helper: Compute active weights
# ----------------------------
BASE_WEIGHTS = {
    "mechanics":  0.40,
    "graph":      0.20,
    "complexity": 0.15,
    "players":    0.10,
    "rating":     0.10,
    "playtime":   0.05,
}

def compute_active_weights(constraints: dict) -> dict[str, float]:
    """
    Remove scoring components covered by hard constraints,
    redistribute their weights evenly across remaining components.
    """
    removed = {
        CONSTRAINT_COMPONENT_MAP[k]
        for k in constraints
        if k in CONSTRAINT_COMPONENT_MAP
    }
    active = {k: v for k, v in BASE_WEIGHTS.items() if k not in removed}
    removed_weight = sum(BASE_WEIGHTS[k] for k in removed)

    if active and removed_weight > 0:
        redistribution = removed_weight / len(active)
        active = {k: round(v + redistribution, 6) for k, v in active.items()}

    return active

def get_complexity_bin(complexity: float) -> str:
    if complexity <= 1.5:
        return 'Very Low'
    elif complexity <= 2.0:
        return 'Low'
    elif complexity <= 2.5:
        return 'Medium'
    elif complexity <= 3.0:
        return 'High'
    else:
        return 'Very High'

# ----------------------------
# SQL: Mechanic weights
# ----------------------------
def query_mechanic_weights(
    liked_games: list[str],
    disliked_games: list[str]
) -> dict[str, float]:
    total = len(liked_games) + len(disliked_games)
    all_games = liked_games + disliked_games

    # If no games at all, return empty
    if not all_games:
        return {}

    query = text("""
        SELECT
            gm.mechanic_name,
            SUM(CASE WHEN LOWER(g.name) IN :liked    THEN 1 ELSE 0 END) AS count_l,
            SUM(CASE WHEN LOWER(g.name) IN :disliked THEN 1 ELSE 0 END) AS count_d
        FROM game_mechanics gm
        JOIN games_base g ON gm.game_id = g.id
        WHERE LOWER(g.name) IN :all_games
        GROUP BY gm.mechanic_name
    """)

    # Use a placeholder tuple for disliked if empty to avoid IN () SQL error
    disliked_param = tuple(n.lower() for n in disliked_games) or ('__none__',)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            "liked":     tuple(n.lower() for n in liked_games),
            "disliked":  disliked_param,
            "all_games": tuple(n.lower() for n in all_games),
        })

    return {
        row.mechanic_name: round((row.count_l - row.count_d) / total, 6)
        for row in df.itertuples()
    }

# ----------------------------
# SQL: Domain weights
# ----------------------------
def query_domain_weights(
    liked_games: list[str],
    disliked_games: list[str]
) -> dict[str, float]:
    total = len(liked_games) + len(disliked_games)
    all_games = liked_games + disliked_games

    # If no games at all, return empty
    if not all_games:
        return {}

    query = text("""
        SELECT
            gd.domain_name,
            SUM(CASE WHEN LOWER(g.name) IN :liked    THEN 1 ELSE 0 END) AS count_l,
            SUM(CASE WHEN LOWER(g.name) IN :disliked THEN 1 ELSE 0 END) AS count_d
        FROM game_domains gd
        JOIN games_base g ON gd.game_id = g.id
        WHERE LOWER(g.name) IN :all_games
        GROUP BY gd.domain_name
    """)

    # Use a placeholder tuple for disliked if empty to avoid IN () SQL error
    disliked_param = tuple(n.lower() for n in disliked_games) or ('__none__',)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            "liked":     tuple(n.lower() for n in liked_games),
            "disliked":  disliked_param,
            "all_games": tuple(n.lower() for n in all_games),
        })

    return {
        row.domain_name: round((row.count_l - row.count_d) / total, 6)
        for row in df.itertuples()
    }

# ----------------------------
# SQL: Numeric feature preferences
# ----------------------------
def query_numeric_preferences(liked_games: list[str]) -> dict[str, float]:
    with engine.connect() as conn:
        # Numeric averages — unchanged
        result = conn.execute(text("""
            SELECT
                AVG(g.complexity_average)                    AS preferred_complexity,
                AVG((g.min_players + g.max_players) / 2.0)   AS preferred_player_midpoint,
                AVG(gs.play_time)                            AS preferred_playtime,
                AVG(g.rating_average)                        AS preferred_rating
            FROM games_base g
            JOIN boardgames gs ON g.id = gs.id
            WHERE LOWER(g.name) IN :liked_games
        """), {"liked_games": tuple(n.lower() for n in liked_games)})
        row = result.fetchone()

        # Most common complexity bin among liked games (deduplicated by game)
        bin_result = conn.execute(text("""
            SELECT complexity_bin, COUNT(*) AS freq
            FROM (
                SELECT DISTINCT name, complexity_bin
                FROM games_base
                WHERE LOWER(name) IN :liked_games
            ) AS deduped
            GROUP BY complexity_bin
            ORDER BY freq DESC
            LIMIT 1
        """), {"liked_games": tuple(n.lower() for n in liked_games)})
        bin_row = bin_result.fetchone()

    return {
        "preferred_complexity":      round(float(row.preferred_complexity), 4),
        "preferred_player_midpoint": round(float(row.preferred_player_midpoint), 4),
        "preferred_playtime":        round(float(row.preferred_playtime), 4),
        "preferred_rating":          round(float(row.preferred_rating), 4),
        "preferred_complexity_bin":  get_complexity_bin(float(row.preferred_complexity)),
    }

# ----------------------------
# Core: Build user profile
# ----------------------------
def build_user_profile(
    liked_games: list[str],
    disliked_games: list[str],
    constraints: dict
) -> dict:
    """
    Assembles the full user profile dict from SQL queries + lightweight Python.
    """
    mechanic_weights  = query_mechanic_weights(liked_games, disliked_games)
    domain_weights    = query_domain_weights(liked_games, disliked_games)
    numeric_prefs     = query_numeric_preferences(liked_games)
    active_weights    = compute_active_weights(constraints)

    return {
        "liked_games":               liked_games,
        "disliked_games":            disliked_games,
        "constraints":               constraints,
        "active_weights":            active_weights,
        "mechanic_weights":          mechanic_weights,
        "domain_weights":            domain_weights,
        **numeric_prefs,
    }

def print_profile(profile: dict, verbose: bool = False) -> None:
    """
    Nicely prints the user profile summary.
    If verbose=True, also prints mechanic and domain weights.
    """
    print("\n--- User Profile Summary ---")
    print(f"Liked games:               {profile['liked_games']}")
    print(f"Disliked games:            {profile['disliked_games']}")
    print(f"Constraints:               {profile['constraints']}")
    print(f"Active weights:            {profile['active_weights']}")

    if verbose:
        print(f"Preferred complexity:      {profile['preferred_complexity']} ({profile['preferred_complexity_bin']})")
        print(f"Preferred player midpoint: {profile['preferred_player_midpoint']}")
        print(f"Preferred playtime:        {profile['preferred_playtime']}")
        print(f"Preferred rating:          {profile['preferred_rating']}")

        print(f"\nMechanic weights ({len(profile['mechanic_weights'])} mechanics tracked):")
        for m, w in sorted(profile['mechanic_weights'].items(), key=lambda x: -x[1]):
            print(f"  {m:<45} {w:+.4f}")

        print(f"\nDomain weights:")
        for d, w in sorted(profile['domain_weights'].items(), key=lambda x: -x[1]):
            print(f"  {d:<45} {w:+.4f}")

# ----------------------------
# Main entry point
# ----------------------------
def main():
    print("=== Board Game Recommendation System ===")
    print("Let's build your preference profile.\n")

    liked, disliked  = get_game_inputs()
    constraints      = get_constraints()

    try:
        profile = build_user_profile(liked, disliked, constraints)
    except ValueError as e:
        print(f"\nError: {e}")
        return None

    print("\n--- User Profile Summary ---")
    print_profile(profile, verbose=True)

    return profile

if __name__ == "__main__":
    profile = main()
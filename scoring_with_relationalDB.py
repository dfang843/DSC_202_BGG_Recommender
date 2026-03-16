import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://username:password@localhost:<host port>/<db name>")            # Update with actual credentials


# Global ranges for normalization — computed once from full dataset
def get_global_ranges() -> dict:
    """
    Fetch min/max values for each numeric feature across all games.
    Used as denominators in normalization formulas.
    """
    with engine.connect() as conn:
        # Full range for complexity and rating
        result = conn.execute(text("""
            SELECT
                MAX(complexity_average) - MIN(complexity_average) AS complexity_range,
                MAX(rating_average) - MIN(rating_average)         AS rating_range
            FROM boardgames
        """))
        row = result.fetchone()

        # Percentile-based range for playtime only
        playtime_result = conn.execute(text("""
            SELECT
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY play_time) -
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY play_time) AS playtime_range
            FROM boardgames
        """))
        pt_row = playtime_result.fetchone()

        # Percentile-based range for players
        player_result = conn.execute(text("""
            SELECT
                PERCENTILE_CONT(0.99) WITHIN GROUP 
                    (ORDER BY (min_players + max_players) / 2.0) -
                PERCENTILE_CONT(0.01) WITHIN GROUP 
                    (ORDER BY (min_players + max_players) / 2.0) AS player_range
            FROM boardgames
        """))
        pl_row = player_result.fetchone()

    return {
        "complexity_range": float(row.complexity_range),
        "rating_range":     float(row.rating_range),
        "playtime_range":   float(pt_row.playtime_range),
        "player_range":     round(float(pl_row.player_range)),
    }


def fetch_candidate_features(candidate_names: list[str]) -> pd.DataFrame:
    """
    Fetch numeric features for all candidate games from the relational DB.
    Also fetches mechanics per game for S_mechanics computation.
    """
    with engine.connect() as conn:
        # Numeric features
        features = pd.read_sql(text("""
            SELECT
                g.name,
                g.complexity_average,
                g.min_players,
                g.max_players,
                (g.min_players + g.max_players) / 2.0 AS player_midpoint,
                g.rating_average,
                gs.play_time
            FROM games_base g
            JOIN boardgames gs ON g.id = gs.id
            WHERE LOWER(g.name) IN :candidates
        """), conn, params={
            "candidates": tuple(n.lower() for n in candidate_names)
        })

        # Mechanics per game
        mechanics = pd.read_sql(text("""
            SELECT LOWER(g.name) AS name, gm.mechanic_name
            FROM games_base g
            JOIN game_mechanics gm ON g.id = gm.game_id
            WHERE LOWER(g.name) IN :candidates
        """), conn, params={
            "candidates": tuple(n.lower() for n in candidate_names)
        })

    return features, mechanics


def apply_hard_constraints(
    candidates_df: pd.DataFrame,
    features_df: pd.DataFrame,
    constraints: dict
) -> pd.DataFrame:
    """
    Filter out candidates that violate any hard constraints.
    Returns filtered features DataFrame.
    """
    df = features_df.copy()

    if "max_playtime" in constraints:
        df = df[df["play_time"] <= constraints["max_playtime"]]
    if "min_rating" in constraints:
        df = df[df["rating_average"] >= constraints["min_rating"]]
    if "min_players" in constraints:
        df = df[df["min_players"] >= constraints["min_players"]]
    if "max_players" in constraints:
        df = df[df["max_players"] <= constraints["max_players"]]
    if "max_complexity" in constraints:
        df = df[df["complexity_average"] <= constraints["max_complexity"]]

    # Keep only candidates that passed constraints
    passing_names = set(df["name"].str.lower())
    candidates_df = candidates_df[
        candidates_df["GameRecommendation"].str.lower().isin(passing_names)
    ]
    return candidates_df, df


def compute_s_mechanics(
    candidates_df: pd.DataFrame,
    mechanics_df: pd.DataFrame,
    mechanic_weights: dict
) -> pd.Series:
    """
    S_mechanics(g) = sum of w_m for mechanics in game g / |M_g|
    Normalized by theoretical max: the highest possible average weight
    given the profile's mechanic weights.
    """
    scores = {}
    grouped = mechanics_df.groupby("name")["mechanic_name"].apply(list)

    for game_name, mechanics in grouped.items():
        if not mechanics:
            scores[game_name] = 0.0
            continue
        total_weight = sum(mechanic_weights.get(m, 0.0) for m in mechanics)
        scores[game_name] = total_weight / len(mechanics)

    # Theoretical max: average of top-k weights where k = median mechanic count
    if mechanic_weights:
        median_k = int(pd.Series([
            len(v) for v in grouped
        ]).median())
        top_k_weights = sorted(mechanic_weights.values(), reverse=True)[:median_k]
        theoretical_max = sum(top_k_weights) / median_k if top_k_weights else 1.0
    else:
        theoretical_max = 1.0

    # Clip to [0, 1] — negative scores stay negative until clipped at 0
    return pd.Series({
        name: max(0.0, min(1.0, score / theoretical_max))
        for name, score in scores.items()
    })


def compute_proximity_score(
    values: pd.Series,
    preferred: float,
    max_range: float
) -> pd.Series:
    """
    Generic proximity score: 1 - |value - preferred| / max_range
    Used for S_complexity, S_players, S_rating, S_playtime.
    """
    if max_range == 0:
        return pd.Series([1.0] * len(values), index=values.index)
    return (1 - (values - preferred).abs() / max_range).clip(0, 1)


def get_final_recommendations(
    profile: dict,
    candidates_df: pd.DataFrame,
    top_n: int = 10
) -> pd.DataFrame:
    """
    Takes the Neo4j candidate DataFrame and user profile,
    applies hard constraints, computes all scoring components,
    and returns the top N ranked recommendations.
    """
    ranges        = get_global_ranges()
    candidate_names = candidates_df["GameRecommendation"].tolist()
    features_df, mechanics_df = fetch_candidate_features(candidate_names)

    # Apply hard constraints
    candidates_df, features_df = apply_hard_constraints(
        candidates_df, features_df, profile["constraints"]
    )

    if features_df.empty:
        print("No candidates passed the hard constraints.")
        return pd.DataFrame()

    # Merge graph scores into features
    graph_scores = candidates_df.set_index(
        candidates_df["GameRecommendation"].str.lower()
    )["GraphSimilarity"]

    features_df = features_df.copy()
    features_df["name_lower"] = features_df["name"].str.lower()
    features_df["s_graph"] = features_df["name_lower"].map(graph_scores)

    # S_mechanics
    mech_scores = compute_s_mechanics(
        features_df, mechanics_df, profile["mechanic_weights"]
    )
    features_df["s_mechanics"] = features_df["name_lower"].map(mech_scores)

    # Normalize s_mechanics to [0, 1]
    mech_min = features_df["s_mechanics"].min()
    mech_max = features_df["s_mechanics"].max()
    mech_range = mech_max - mech_min
    if mech_range > 0:
        features_df["s_mechanics"] = (
            (features_df["s_mechanics"] - mech_min) / mech_range
        )

    # Proximity scores
    features_df["s_complexity"] = compute_proximity_score(
        features_df["complexity_average"],
        profile["preferred_complexity"],
        ranges["complexity_range"]
    )
    features_df["s_players"] = compute_proximity_score(
        features_df["player_midpoint"],
        profile["preferred_player_midpoint"],
        ranges["player_range"]
    )
    features_df["s_rating"] = compute_proximity_score(
        features_df["rating_average"],
        profile["preferred_rating"],
        ranges["rating_range"]
    )
    features_df["s_playtime"] = compute_proximity_score(
        features_df["play_time"],
        profile["preferred_playtime"],
        ranges["playtime_range"]
    )

    # Final weighted score
    w = profile["active_weights"]
    features_df["final_score"] = (
        w.get("mechanics",  0) * features_df["s_mechanics"]  +
        w.get("graph",      0) * features_df["s_graph"]      +
        w.get("complexity", 0) * features_df["s_complexity"] +
        w.get("players",    0) * features_df["s_players"]    +
        w.get("rating",     0) * features_df["s_rating"]     +
        w.get("playtime",   0) * features_df["s_playtime"]
    ).round(4)

    # Sort and return top N
    result = features_df.sort_values("final_score", ascending=False).head(top_n)
    # Build output columns dynamically based on active weights
    score_columns = ["s_" + k for k in profile["active_weights"].keys()]
    output = result[["name", "final_score"] + score_columns].reset_index(drop=True)

    # Round all numeric columns to 4 decimal places
    numeric_cols = ["final_score"] + score_columns
    output[numeric_cols] = output[numeric_cols].round(4)

    return output
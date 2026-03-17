import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://username:password@localhost:<port>/<db name>")            # Update with actual credentials


# Global ranges for normalization — computed once from full dataset
def get_global_ranges() -> dict:
    """
    Single SQL query computes all ranges and total game count.
    Rounding of player_range now done in SQL via ROUND().
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                MAX(complexity_average) - MIN(complexity_average) AS complexity_range,
                ROUND((
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY play_time) -
                    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY play_time)
                )::numeric) AS playtime_range,
                ROUND((
                    PERCENTILE_CONT(0.99) WITHIN GROUP 
                        (ORDER BY (min_players + max_players) / 2.0) -
                    PERCENTILE_CONT(0.01) WITHIN GROUP 
                        (ORDER BY (min_players + max_players) / 2.0)
                )::numeric) AS player_range,
                COUNT(*) AS total_games
            FROM games
        """))
        row = result.fetchone()

    return {
        "complexity_range": float(row.complexity_range),
        "playtime_range":   float(row.playtime_range),
        "player_range":     float(row.player_range),
        "total_games":      int(row.total_games),
    }


def fetch_candidate_features(
    candidate_names: list[str],
    profile: dict,
    ranges: dict
) -> tuple:
    """
    SQL now computes proximity scores directly.
    Python just reads s_complexity, s_playtime, s_rank from the results.
    """
    with engine.connect() as conn:
        features = pd.read_sql(text("""
            SELECT
                g.name,
                g.complexity_average,
                g.bgg_rank,
                gs.play_time,
                -- S_complexity
                GREATEST(0.0, LEAST(1.0,
                    1.0 - ABS(g.complexity_average - :preferred_complexity)
                          / :complexity_range
                )) AS s_complexity,
                -- S_playtime
                GREATEST(0.0, LEAST(1.0,
                    1.0 - ABS(gs.play_time - :preferred_playtime)
                          / :playtime_range
                )) AS s_playtime,
                -- S_rank
                GREATEST(0.0, LEAST(1.0,
                    1.0 - g.bgg_rank::float / :total_games
                )) AS s_rank
            FROM games_base g
            JOIN games gs ON g.id = gs.id
            WHERE LOWER(g.name) IN :candidates
        """), conn, params={
            "candidates":           tuple(n.lower() for n in candidate_names),
            "preferred_complexity": profile["preferred_complexity"],
            "preferred_playtime":   profile["preferred_playtime"],
            "complexity_range":     ranges["complexity_range"],
            "playtime_range":       ranges["playtime_range"],
            "total_games":          ranges["total_games"],
        })

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
) -> tuple:
    df = features_df.copy()

    if "max_playtime" in constraints:
        df = df[df["play_time"] <= constraints["max_playtime"]]
    if "min_rating" in constraints:
        # need to join rating back in just for filtering
        with engine.connect() as conn:
            ratings = pd.read_sql(text("""
                SELECT LOWER(name) AS name, rating_average
                FROM games
                WHERE LOWER(name) IN :candidates
            """), conn, params={
                "candidates": tuple(df["name"].str.lower().tolist())
            })
        df = df.merge(ratings, left_on=df["name"].str.lower(), right_on="name", how="left")
        df = df[df["rating_average"] >= constraints["min_rating"]]
        df = df.drop(columns=["rating_average", "key_0"], errors="ignore")
    if "min_players" in constraints:
        with engine.connect() as conn:
            players = pd.read_sql(text("""
                SELECT LOWER(name) AS name, min_players, max_players
                FROM games
                WHERE LOWER(name) IN :candidates
            """), conn, params={
                "candidates": tuple(df["name"].str.lower().tolist())
            })
        df = df.merge(players, left_on=df["name"].str.lower(), right_on="name", how="left")
        df = df[df["min_players"] >= constraints["min_players"]]
        df = df.drop(columns=["min_players", "max_players", "key_0"], errors="ignore")
    if "max_players" in constraints:
        with engine.connect() as conn:
            players = pd.read_sql(text("""
                SELECT LOWER(name) AS name, min_players, max_players
                FROM games
                WHERE LOWER(name) IN :candidates
            """), conn, params={
                "candidates": tuple(df["name"].str.lower().tolist())
            })
        df = df.merge(players, left_on=df["name"].str.lower(), right_on="name", how="left")
        df = df[df["max_players"] <= constraints["max_players"]]
        df = df.drop(columns=["min_players", "max_players", "key_0"], errors="ignore")
    if "max_complexity" in constraints:
        df = df[df["complexity_average"] <= constraints["max_complexity"]]

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
    ranges = get_global_ranges()
    candidate_names = candidates_df["GameRecommendation"].tolist()
    features_df, mechanics_df = fetch_candidate_features(
        candidate_names, profile, ranges
    )

    candidates_df, features_df = apply_hard_constraints(
        candidates_df, features_df, profile["constraints"]
    )

    if features_df.empty:
        print("No candidates passed the hard constraints.")
        return pd.DataFrame()

    features_df = features_df.copy()
    features_df["name_lower"] = features_df["name"].str.lower()

    # S_graph
    graph_scores = candidates_df.set_index(
        candidates_df["GameRecommendation"].str.lower()
    )["GraphSimilarity"]
    features_df["s_graph"] = features_df["name_lower"].map(graph_scores)

    # Complexity bin from candidates_df
    complexity_bins = candidates_df.set_index(
        candidates_df["GameRecommendation"].str.lower()
    )["ComplexityBin"]
    features_df["complexity_bin"] = features_df["name_lower"].map(complexity_bins)

    # S_mechanics
    mech_scores = compute_s_mechanics(
        features_df, mechanics_df, profile["mechanic_weights"]
    )
    features_df["s_mechanics"] = features_df["name_lower"].map(mech_scores)

    # Normalize s_mechanics
    if profile["mechanic_weights"]:
        median_k = int(pd.Series([
            len(v) for v in mechanics_df.groupby("name")["mechanic_name"].apply(list)
        ]).median())
        top_k_weights = sorted(profile["mechanic_weights"].values(), reverse=True)[:median_k]
        theoretical_max = sum(top_k_weights) / median_k if top_k_weights else 1.0
    else:
        theoretical_max = 1.0
    features_df["s_mechanics"] = features_df["s_mechanics"].apply(
        lambda x: max(0.0, min(1.0, x / theoretical_max)) if theoretical_max > 0 else 0.0
    )

    # S_complexity
    if "complexity" in profile["active_weights"]:
        features_df["s_complexity"] = compute_proximity_score(
            features_df["complexity_average"],
            profile["preferred_complexity"],
            ranges["complexity_range"]
        )

    # S_rank: normalized rank score = 1 - (bgg_rank / total_games)
    if "rank" in profile["active_weights"]:
        features_df["s_rank"] = (
            1 - features_df["bgg_rank"] / ranges["total_games"]
        ).clip(0, 1)

    # S_playtime
    if "playtime" in profile["active_weights"]:
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
        w.get("complexity", 0) * features_df.get("s_complexity", 0) +
        w.get("rank",       0) * features_df.get("s_rank", 0) +
        w.get("playtime",   0) * features_df.get("s_playtime", 0)
    ).round(6)

    # Build output columns dynamically
    score_columns = ["s_" + k for k in profile["active_weights"].keys()]
    result = features_df.sort_values("final_score", ascending=False).head(top_n)
    output = result[["name", "complexity_bin", "final_score"] + score_columns].reset_index(drop=True)
    numeric_cols = ["final_score"] + score_columns
    output[numeric_cols] = output[numeric_cols].round(4)

    return output

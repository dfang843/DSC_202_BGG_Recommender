import argparse
import pandas as pd
from setup_graphDB import setup_graph, CSV_PATH
from generate_user_profile import get_game_inputs, get_constraints, build_user_profile, print_profile
from graph_candidates import get_graph_candidates
from scoring_with_relationalDB import get_final_recommendations

pd.set_option("display.float_format", "{:.4f}".format)

def run_pipeline(setup: bool = False) -> pd.DataFrame:

    # Optional graph setup
    if setup:
        print("=== Setting Up Graph ===")
        setup_graph(CSV_PATH)
        print("\n")

    print("=== Board Game Recommendation System ===\n")

    # Stage 1: Build user profile
    liked, disliked = get_game_inputs()
    constraints     = get_constraints()

    try:
        profile = build_user_profile(liked, disliked, constraints)
    except ValueError as e:
        print(f"\nError building profile: {e}")
        return None

    print(f"\nProfile built — {len(profile['mechanic_weights'])} mechanics tracked.")
    print_profile(profile, verbose=False)
    

    # Stage 2: Graph similarity candidates
    print("\nFetching graph candidates from Neo4j...")
    candidates_df = get_graph_candidates(profile)
    print(f"{len(candidates_df)} total candidates returned from Neo4j.")

    # Stage 3: Relational scoring
    print("\nScoring and ranking candidates...")
    recommendations = get_final_recommendations(profile, candidates_df)

    print("\n=== Your Top Recommendations ===")
    print(recommendations.to_string(index=False))

    return recommendations


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Board game recommendation pipeline."
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Rebuild the Neo4j graph from scratch before running recommendations."
    )
    args = parser.parse_args()

    run_pipeline(setup=args.setup)
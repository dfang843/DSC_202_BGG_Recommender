from generate_user_profile import get_game_inputs, get_constraints, build_user_profile, print_profile
from graph_candidates import get_graph_candidates
from scoring_with_relationalDB import *

pd.set_option("display.float_format", "{:.4f}".format)

# Build profile directly without interactive input for testing
liked       = ["Gloomhaven", "Pandemic", "Terraforming Mars", "Catan", "Ra"]
disliked    = ["Cockroach Poker", "Fluxx",]
constraints = {"max_playtime": 120, "min_rating": 7.0, "max_complexity": 3.5}

profile       = build_user_profile(liked, disliked, constraints)
print_profile(profile)
candidates_df = get_graph_candidates(profile)
recommendations = get_final_recommendations(profile, candidates_df, top_n=10)

print("\n=== Final Recommendations ===")
print(recommendations.to_string(index=False))
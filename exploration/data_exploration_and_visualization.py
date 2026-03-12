# data_exploration_and_visualization.py

import pandas as pd
import psycopg2 as pg
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ---------------------------
# Database connection setup
# ---------------------------
DB_PARAMS = {
    'dbname': 'postgres',   
    'user': 'database_username',      # change this on local end
    'password': 'database_password',  # change this on local end
    'host': 'localhost',
    'port': '5432'
}

def get_connection():
    """Create and return a database connection"""
    try:
        conn = pg.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"✗ Error connecting to database: {e}")
        raise

def execute_query(query, conn=None):
    """Execute SQL query and return results as pandas DataFrame"""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        raise
    finally:
        if close_conn:
            conn.close()

# ---------------------------
# Load data from PostgreSQL
# ---------------------------
games = execute_query("SELECT * FROM games;")
mechanics = execute_query("SELECT * FROM mechanics;")
gamemechanics = execute_query("SELECT * FROM gamemechanics;")
domains = execute_query("SELECT * FROM domains;")
gamedomains = execute_query("SELECT * FROM gamedomains;")

# ---------------------------
# Basic info / statistics
# ---------------------------
print("Games table shape:", games.shape)
print("Mechanics table shape:", mechanics.shape)
print("GameMechanics table shape:", gamemechanics.shape)
print("Domains table shape:", domains.shape)
print("GameDomains table shape:", gamedomains.shape)

print("\nGames columns info:")
print(games.info())
print("\nGames sample:")
print(games.head())

# ---------------------------
# EDA Visualizations
# ---------------------------
# Complexity Distribution
plt.figure(figsize=(8,5))
sns.histplot(games['complexity_avg'], bins=20, kde=True)
plt.title("Distribution of Game Complexity")
plt.xlabel("Complexity Average")
plt.ylabel("Number of Games")
plt.tight_layout()
plt.savefig("./complexity_distribution.png")
plt.close()

# Ratings Distribution
plt.figure(figsize=(8,5))
sns.histplot(games['rating_avg'], bins=20, kde=True, color='orange')
plt.title("Distribution of Game Ratings")
plt.xlabel("Average Rating")
plt.ylabel("Number of Games")
plt.tight_layout()
plt.savefig("./rating_distribution.png")
plt.close()

# Min/Max Player Distributions
plt.figure(figsize=(8,5))
sns.histplot(games['min_players'], bins=range(1, 11), color='green', label='Min Players', alpha=0.6)
sns.histplot(games['max_players'], bins=range(1, 11), color='blue', label='Max Players', alpha=0.6)
plt.title("Distribution of Min and Max Players per Game")
plt.xlabel("Number of Players")
plt.ylabel("Number of Games")
plt.legend()
plt.tight_layout()
plt.savefig("./players_distribution.png")
plt.close()

# Top 20 Mechanics by Game Count
mechanics_freq = execute_query("""
SELECT m.mechanic_name, COUNT(gm.game_id) AS game_count
FROM mechanics m
JOIN gamemechanics gm ON m.mechanic_id = gm.mechanic_id
GROUP BY m.mechanic_name
ORDER BY game_count DESC
LIMIT 20;
""")

plt.figure(figsize=(10,6))
sns.barplot(x='game_count', y='mechanic_name', data=mechanics_freq, palette='viridis')
plt.title("Top 20 Mechanics by Number of Games")
plt.xlabel("Number of Games")
plt.ylabel("Mechanic")
plt.tight_layout()
plt.savefig("./top20_mechanics.png")
plt.close()


# Top 10 Domains by Game Count
domains_count = execute_query("""
SELECT d.domain_name, COUNT(gd.game_id) AS game_count
FROM domains d
JOIN gamedomains gd ON d.domain_id = gd.domain_id
GROUP BY d.domain_name
ORDER BY game_count DESC;
""")

plt.figure(figsize=(8,5))
sns.barplot(x='game_count', y='domain_name', data=domains_count, palette='coolwarm')
plt.title("Number of Games per Domain")
plt.xlabel("Number of Games")
plt.ylabel("Domain")
plt.tight_layout()
plt.savefig("./domain_counts.png")
plt.close()

# Rating as function of complexity
plt.figure(figsize=(8,5))
sns.scatterplot(x='complexity_avg', y='rating_avg', data=games, alpha=0.6)
plt.title("Game Complexity vs Average Rating")
plt.xlabel("Complexity Average")
plt.ylabel("Average Rating")
plt.tight_layout()
plt.savefig("./complexity_vs_rating.png")
plt.close()

print("All EDA figures saved in the current folder.")

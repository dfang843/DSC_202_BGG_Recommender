# DSC_202_BGG_Recommender
Final Project for DSC 202 Winter Quarter 2026, focusing on recommending board games based on favorite and least favorite games and their mechanics.


## Project Structure

- `bgg_processed.csv` – Cleaned and processed board game dataset used to build the database.  
- `boardgames_tables_dump.sql` – SQL dump containing the relational database schema and preloaded data (games, mechanics, game-mechanic relationships, etc.).  
- `preprocessing/` – SQL files to set up tables and load data into said tables
- `exploration/` - folder containing on Python script that uses `psycopg2` for some quick EDA on the tables

## Setting Up the Relational Database

The relational database is provided as a `.sql` dump to make it easy for team members to replicate the environment.

### Steps to Load the SQL Dump

1. **Install PostgreSQL** if you don’t already have it:  
   [https://www.postgresql.org/download/](https://www.postgresql.org/download/)

2. **Create a new database** (you can choose any name, e.g., `boardgames_project`):  

   ```bash
   createdb boardgames_project
   ```

3. **Load SQL dump into databse**:
    ```bash
    psql -U <postgres_username> -d boardgames_project -f boardgames_tables_dump.sql
    ```
    Replace `<your_postgres_username>` with the actual username for the databse.

4. **Verify outputs** When running a command like `SELECT COUNT(*) FROM games`, you should see total number of games listed. The database contains 10,000 games, 182 mechanics, and 35,000+ game-mechanic entries.



Note: Database names can differ across the three of us, as long as the table names, content, and data are identical.

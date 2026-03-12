CREATE TABLE Games (
    game_id BIGINT PRIMARY KEY,
    name TEXT,
    year_published INT,
    min_players INT,
    max_players INT,
    play_time INT,
    min_age INT,
    users_rated INT,
    rating_avg FLOAT,
    bgg_rank INT,
    complexity_avg FLOAT,
    owned_users INT
);

CREATE TABLE Mechanics (
    mechanic_id SERIAL PRIMARY KEY,
    mechanic_name TEXT UNIQUE
);

CREATE TABLE GameMechanics (
    game_id BIGINT REFERENCES Games(game_id),
    mechanic_id INT REFERENCES Mechanics(mechanic_id),
    PRIMARY KEY (game_id, mechanic_id)
);

CREATE TABLE Domains (
    domain_id SERIAL PRIMARY KEY,
    domain_name TEXT UNIQUE
);

CREATE TABLE GameDomains (
    game_id BIGINT REFERENCES Games(game_id),
    domain_id INT REFERENCES Domains(domain_id),
    PRIMARY KEY (game_id, domain_id)
);

CREATE TABLE staging_games (
    id FLOAT,
    name TEXT,
    year_published FLOAT,
    min_players INT,
    max_players INT,
    play_time INT,
    min_age INT,
    users_rated INT,
    rating_average FLOAT,
    bgg_rank INT,
    complexity_average FLOAT,
    owned_users FLOAT,
    mechanics TEXT,
    domains TEXT
);
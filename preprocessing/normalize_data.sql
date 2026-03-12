INSERT INTO Games
SELECT
    id::BIGINT,
    name,
    year_published::INT,
    min_players,
    max_players,
    play_time,
    min_age,
    users_rated,
    rating_average,
    bgg_rank,
    complexity_average,
    owned_users::INT
FROM staging_games;

INSERT INTO Mechanics (mechanic_name)
SELECT DISTINCT TRIM(mech)
FROM staging_games,
LATERAL unnest(string_to_array(mechanics, ',')) AS mech
ON CONFLICT DO NOTHING;

INSERT INTO GameMechanics (game_id, mechanic_id)
SELECT DISTINCT
    s.id::BIGINT,
    m.mechanic_id
FROM staging_games s
CROSS JOIN LATERAL unnest(string_to_array(s.mechanics, ',')) AS mech
JOIN Mechanics m
ON TRIM(mech) = m.mechanic_name
ON CONFLICT DO NOTHING;

INSERT INTO Domains (domain_name)
SELECT DISTINCT TRIM(dom)
FROM staging_games,
LATERAL unnest(string_to_array(domains, ',')) AS dom
ON CONFLICT DO NOTHING;

INSERT INTO GameDomains (game_id, domain_id)
SELECT
    s.id::BIGINT,
    d.domain_id
FROM staging_games s
CROSS JOIN LATERAL unnest(string_to_array(s.domains, ',')) AS dom
JOIN Domains d
ON TRIM(dom) = d.domain_name
ON CONFLICT DO NOTHING;
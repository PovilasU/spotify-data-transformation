-- View 1: track_summary
CREATE OR REPLACE VIEW track_summary AS
SELECT
  t.id AS track_id,
  t.name,
  t.popularity,
  t.energy,
  t.danceability_level AS danceability,
  COALESCE(SUM(CAST(NULLIF(a.followers, '') AS NUMERIC)), 0) AS artist_followers
FROM tracks t
LEFT JOIN LATERAL (
  SELECT TRIM(unnest(string_to_array(regexp_replace(t.id_artists, E'[\\[\\]\'"]', '', 'g'), ','))) AS artist_id
) AS u ON true
LEFT JOIN artists a ON a.id = u.artist_id
GROUP BY t.id, t.name, t.popularity, t.energy, t.danceability_level;
 
-- View 2: most_energising_track_per_year
CREATE OR REPLACE VIEW most_energising_track_per_year AS
WITH ranked_tracks AS (
  SELECT
    release_year,
    id AS track_id,
    name,
    energy,
    ROW_NUMBER() OVER (PARTITION BY release_year ORDER BY energy DESC) AS rn
  FROM tracks
)
SELECT
  release_year,
  track_id,
  name,
  energy
FROM ranked_tracks
WHERE rn = 1;
 
-- View 3: artist_track_summary
CREATE OR REPLACE VIEW artist_track_summary AS
SELECT
  a.id AS artist_id,
  a.name AS artist_name,
  t.id AS track_id,
  t.name AS track_name
FROM tracks t
JOIN LATERAL (
  SELECT TRIM(unnest(string_to_array(regexp_replace(t.id_artists, E'[\\[\\]\'"]', '', 'g'), ','))) AS artist_id
) AS u ON true
JOIN artists a ON a.id = u.artist_id
WHERE COALESCE(NULLIF(a.followers, ''), '0')::numeric > 0;

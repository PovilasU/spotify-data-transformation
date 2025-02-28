
-- Return track id, name, popularity, energy, danceability (Low, Medium, High) and number of artist followers;

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




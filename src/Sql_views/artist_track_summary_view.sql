
-- Return track id, name, popularity, energy, danceability (Low, Medium, High) and number of artist followers;
CREATE OR REPLACE VIEW artist_track_summary AS
SELECT
  a.id AS artist_id,
  a.name AS artist_name,
  t.id AS track_id,
  t.name AS track_name
FROM tracks t
JOIN LATERAL (
  -- Remove brackets and quotes then split the string into individual artist ids
  SELECT TRIM(unnest(string_to_array(regexp_replace(t.id_artists, E'[\\[\\]\'"]', '', 'g'), ','))) AS artist_id
) AS u ON true
JOIN artists a ON a.id = u.artist_id
WHERE COALESCE(NULLIF(a.followers, ''), '0')::numeric > 0;

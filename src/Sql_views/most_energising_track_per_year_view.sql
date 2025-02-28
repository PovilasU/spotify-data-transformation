
-- Pick the most energising track of each release year. Return release year, track id, name and its energy

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

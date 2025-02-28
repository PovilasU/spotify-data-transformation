// Data transformation script
import { Track, Artist } from "./ingestion";

export class Transformation {
  public filterAndTransformTracks(tracks: Track[], artists: Artist[]) {
    // Filter out tracks with no name or tracks shorter than 1 minute (assuming energy as proxy for track length)
    const filteredTracks = tracks.filter(
      (track) => track.name && track.energy >= 0.5
    );

    // Filter artists that have at least one track after filtering
    const artistIdsWithTracks = new Set(
      filteredTracks.map((track) => track.track_id)
    );
    const filteredArtists = artists.filter((artist) =>
      artistIdsWithTracks.has(artist.artist_id)
    );

    // Transform the data
    const transformedTracks = filteredTracks.map((track) => {
      // Split release_date into year, month, day
      const releaseDate = new Date(track.release_date);
      const year = releaseDate.getFullYear();
      const month = releaseDate.getMonth() + 1; // Months are 0-indexed
      const day = releaseDate.getDate();

      // Transform danceability
      let danceability: string;
      if (track.danceability < 0.5) {
        danceability = "Low";
      } else if (track.danceability < 0.6) {
        danceability = "Medium";
      } else {
        danceability = "High";
      }

      return {
        ...track,
        year,
        month,
        day,
        danceability,
      };
    });

    return { transformedTracks, filteredArtists };
  }
}

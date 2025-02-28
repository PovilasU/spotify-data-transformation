// tests/transformer.test.ts

import {
  parseReleaseDate,
  transformDanceability,
  FilterTransform,
  ArtistFilterTransform,
  TrackRow,
  ArtistRow,
} from "../src/transformer";

// Tests for parseReleaseDate
describe("parseReleaseDate", () => {
  it("parses YYYY-MM-DD format correctly", () => {
    const result = parseReleaseDate("1929-01-12");
    expect(result).toEqual({ year: "1929", month: "01", day: "12" });
  });

  it("parses DD/MM/YYYY format correctly", () => {
    const result = parseReleaseDate("22/02/1929");
    expect(result).toEqual({ year: "1929", month: "02", day: "22" });
  });

  it("parses YYYY format correctly", () => {
    const result = parseReleaseDate("1929");
    expect(result).toEqual({ year: "1929", month: "", day: "" });
  });

  it("returns empty strings for invalid format", () => {
    const result = parseReleaseDate("invalid-date");
    expect(result).toEqual({ year: "", month: "", day: "" });
  });
});

// Tests for transformDanceability
describe("transformDanceability", () => {
  it('returns "Low" for values less than 0.5', () => {
    expect(transformDanceability("0.4")).toBe("Low");
  });

  it('returns "Medium" for values between 0.5 and 0.6 (inclusive)', () => {
    expect(transformDanceability("0.5")).toBe("Medium");
    expect(transformDanceability("0.6")).toBe("Medium");
  });

  it('returns "High" for values greater than 0.6', () => {
    expect(transformDanceability("0.8")).toBe("High");
  });

  it("returns empty string for non-numeric input", () => {
    expect(transformDanceability("abc")).toBe("");
  });

  it("returns empty string for undefined input", () => {
    expect(transformDanceability(undefined)).toBe("");
  });
});

// Tests for FilterTransform
describe("FilterTransform", () => {
  it("skips tracks with duration less than minDuration", (done) => {
    const filter = new FilterTransform({ minDuration: 60000 });
    const track: TrackRow = {
      name: "Short Track",
      duration_ms: "50000", // Less than 60000
      id_artists: "['artist1']",
    };

    filter._transform(track, "utf8", (err, data) => {
      expect(data).toBeUndefined();
      done();
    });
  });

  it("processes valid track and collects unique artist IDs", (done) => {
    const filter = new FilterTransform({ minDuration: 60000 });
    const track: TrackRow = {
      name: "Valid Track",
      duration_ms: "70000", // Valid duration
      id_artists: "['artist1','artist2']",
      release_date: "2020-01-01",
      danceability: "0.8",
    };

    filter._transform(track, "utf8", (err, data) => {
      expect(err).toBeNull();
      // Check that the uniqueArtistIds set includes artist1 and artist2.
      expect(filter.uniqueArtistIds.has("artist1")).toBe(true);
      expect(filter.uniqueArtistIds.has("artist2")).toBe(true);
      // Check that release date was parsed correctly.
      expect(data.release_year).toBe("2020");
      // Check that danceability_level is transformed to "High"
      expect(data.danceability_level).toBe("High");
      done();
    });
  });
});

// Tests for ArtistFilterTransform
describe("ArtistFilterTransform", () => {
  it("passes through only allowed artists", (done) => {
    const allowed = new Set<string>(["artist1", "artist2"]);
    const transform = new ArtistFilterTransform(allowed);

    const allowedArtist: ArtistRow = { id: "artist1", name: "Artist One" };
    transform._transform(allowedArtist, "utf8", (err, data) => {
      expect(err).toBeNull();
      expect(data).toEqual(allowedArtist);

      const disallowedArtist: ArtistRow = {
        id: "artist3",
        name: "Artist Three",
      };
      // Remove explicit types from the callback if TypeScript complains.
      transform._transform(disallowedArtist, "utf8", (err2, data2) => {
        expect(data2).toBeUndefined();
        done();
      });
    });
  });
});

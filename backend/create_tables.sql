-- Création de la table MAIN_ARTIST
CREATE TABLE IF NOT EXISTS MAIN_ARTIST (
    main_artist_id TEXT PRIMARY KEY,
    main_artist_name TEXT NOT NULL,
    main_artist_popularity INTEGER,
    main_artist_genre JSONB
);

-- Création de la table TRACK
CREATE TABLE IF NOT EXISTS TRACK (
    track_id TEXT PRIMARY KEY,
    main_artist_id TEXT REFERENCES MAIN_ARTIST(main_artist_id),
    released_date DATE,
    track_market JSONB,
    album_type TEXT,
    track_duration_ms INTEGER,
    total_tracks INTEGER,
    track_name TEXT NOT NULL,
    track_popularity INTEGER,
    other_artists TEXT
);

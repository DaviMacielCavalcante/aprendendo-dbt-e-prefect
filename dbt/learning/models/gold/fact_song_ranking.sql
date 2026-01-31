SELECT 
    s.rank,
    s.song_title,
    s.release_date,
    s.spotify_streams_millions,
    s.popularity_score,
    s.duration_seconds,
    s.explicit, 
    a.artist_id,
    g.genre_id
FROM {{ ref('stg_songs') }} AS s
INNER JOIN {{ ref('dim_artist') }} AS a
ON s.artist = a.artist_name
INNER JOIN {{ ref('dim_genre') }} AS g
ON s.genre = g.genre_name
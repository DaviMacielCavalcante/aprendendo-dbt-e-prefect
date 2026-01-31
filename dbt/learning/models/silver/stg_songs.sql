SELECT DISTINCT
    Rank AS rank, 
    Song_Title AS song_title, 
    Artist AS artist, 
    Genre AS genre, 
    CAST(Release_Date AS DATE) AS release_date, 
    Spotify_Streams_Millions AS spotify_streams_millions, 
    Popularity_Score AS popularity_score, 
    Duration_Seconds AS duration_seconds, 
    CASE WHEN Explicit = 'Yes' THEN true
        ELSE false
    END AS explicit
FROM {{ source('bronze', 'songs_bronze')}}


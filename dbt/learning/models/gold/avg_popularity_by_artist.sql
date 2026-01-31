SELECT 
    a.artist_name,
    ROUND(AVG(f.popularity_score), 2) AS avg_popularity
FROM {{ ref('fact_song_ranking') }} AS f
INNER JOIN {{ ref('dim_artist') }} AS a 
ON f.artist_id = a.artist_id
GROUP BY a.artist_name
WITH unique_artists AS (
    SELECT 
    DISTINCT artist AS artist_name
    FROM {{ ref('stg_songs') }} 
)

SELECT 
    ROW_NUMBER() OVER() AS artist_id,
    artist_name
FROM unique_artists
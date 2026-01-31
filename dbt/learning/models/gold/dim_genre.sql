WITH unique_genres AS (
    SELECT 
    DISTINCT genre AS genre_name
    FROM {{ ref('stg_songs') }} 
)

SELECT 
    ROW_NUMBER() OVER() AS genre_id,
    genre_name
FROM unique_genres
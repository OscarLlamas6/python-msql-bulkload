/* LLENAR TABLA ARTIST */   
INSERT INTO artist(name)
SELECT DISTINCT(artist) FROM TablaTemporal
ORDER BY artist;


/* LLENAR TABLA GENRE */     
INSERT INTO genre(name)     
SELECT
  DISTINCT(SUBSTRING_INDEX(SUBSTRING_INDEX(TablaTemporal.genre, ',', numbers.n), ',', -1)) genre
FROM
  (SELECT 1 n UNION ALL SELECT 2
   UNION ALL SELECT 3 UNION ALL SELECT 4) numbers INNER JOIN TablaTemporal
  ON CHAR_LENGTH(TablaTemporal.genre)
     -CHAR_LENGTH(REPLACE(TablaTemporal.genre, ',', ''))>=numbers.n-1;

/* LLENAR TABLA SONG */     
INSERT INTO song(`name`, duration_ms, explicit, `year`, popularity, danceability, 
				energy, `key`, loudness, `mode`, speechiness, acousticness,
                instrumentalness, liveness, valence, tempo, artistID)
SELECT song, duration_ms, explicit, `year`, popularity, danceability, energy, 
		`key`, loudness, `mode`, speechiness, acousticness, instrumentalness, liveness, 
        valence, tempo, (SELECT artistID FROM artist WHERE artist.name = TablaTemporal.artist) AS artistID                
FROM TablaTemporal
GROUP BY artist, song, duration_ms, explicit, `year`;

/* LLENAR TABLA Record */  

INSERT INTO record(popularity, danceability, energy, `key`, loudness, `mode`, 
		speechiness, acousticness, instrumentalness, liveness, valence, tempo, songID)
SELECT popularity, danceability, energy, `key`, loudness, `mode`, 
		speechiness, acousticness, instrumentalness, liveness, valence, tempo, 
        (SELECT songID FROM (
			SELECT song.songID, a.`name` AS artist, song.`name` AS song, duration_ms, explicit, `year` FROM song
			INNER JOIN artist AS a ON song.artistID = a.artistID
		) AS SongsWithArtistName 
		WHERE TablaTemporal.artist = SongsWithArtistName.artist AND TablaTemporal.song = SongsWithArtistName.song
		AND TablaTemporal.duration_ms = SongsWithArtistName.duration_ms
		AND TablaTemporal.explicit = SongsWithArtistName.explicit
		AND TablaTemporal.`year` = SongsWithArtistName.`year`) AS songID         
FROM TablaTemporal;


/* LLENAR TABLA SongGenre */     

INSERT INTO SongGenre(genreID, songID)
SELECT (SELECT genreID FROM genre WHERE SongsGenres.genre = genre.`name`) AS genreID,
(SELECT songID FROM (
	SELECT song.songID, a.`name` AS artist, song.`name` AS song, duration_ms, explicit, `year` FROM song
	INNER JOIN artist AS a ON song.artistID = a.artistID
) AS SongsWithArtistName 
WHERE SongsGenres.artist = SongsWithArtistName.artist AND SongsGenres.song = SongsWithArtistName.song
AND SongsGenres.duration_ms = SongsWithArtistName.duration_ms
AND SongsGenres.explicit = SongsWithArtistName.explicit
AND SongsGenres.`year` = SongsWithArtistName.`year`) AS songID
FROM (
	SELECT artist, song, duration_ms, explicit, `year`, 
    SUBSTRING_INDEX(SUBSTRING_INDEX(TablaTemporal.genre, ',', numbers.n), ',', -1) genre
	FROM
	  (SELECT 1 n UNION ALL SELECT 2
	   UNION ALL SELECT 3 UNION ALL SELECT 4) numbers INNER JOIN TablaTemporal
	  ON CHAR_LENGTH(TablaTemporal.genre)
		 -CHAR_LENGTH(REPLACE(TablaTemporal.genre, ',', ''))>=numbers.n-1
) AS SongsGenres
GROUP BY songID, genreID;

/* REPORTE 1: TOP 10 ARTISTAS CON MAYOR DEPRODUCION */

SELECT artist.name, COUNT(SongsPlays.name) AS Plays
FROM (
SELECT song.artistID AS artistID, song.name AS name FROM record
INNER JOIN song ON record.songID = song.songID
) AS SongsPlays
INNER JOIN artist ON SongsPlays.artistID = artist.artistID
GROUP BY artist.name
ORDER BY Plays DESC
LIMIT 10;

/* REPORTE 2: TOP 10 CANCIONES MAS REPRODUCIDAS */

SELECT artist.name, SongsPlays.name, SongsPlays.Plays
FROM (
SELECT song.artistID AS artistID, song.name AS name,  COUNT(record.songID) AS Plays FROM record
INNER JOIN song ON record.songID = song.songID
GROUP BY record.songID
) AS SongsPlays
INNER JOIN artist ON SongsPlays.artistID = artist.artistID
ORDER BY Plays DESC
LIMIT 10;

/* REPORTE 3: TOP 5 GENEROS MAS REPRODUCIDOS */

SELECT `name`, COUNT(`name`) AS Plays FROM (
	SELECT record.recordID, record.songID, SongGenreName.`name` AS `name` FROM (SELECT SongGenre.songID, Genre.genreID, Genre.name FROM SongGenre
	INNER JOIN Genre ON SongGenre.genreID = Genre.genreID) AS SongGenreName
	INNER JOIN record ON record.songID = SongGenreName.songID
) AS RecordGenre
GROUP BY `name` 
ORDER BY Plays DESC 
LIMIT 5;

/* REPORTE 4: El artista mas reproducido por cada genero */

SELECT artist.name, IDArtistPlays.GenreName, IDArtistPlays.TotalPlays
FROM (
	SELECT ArtistPlaysGenre.artistID, MAX(ArtistPlaysGenre.Plays) AS TotalPlays, ArtistPlaysGenre.genre AS GenreName
FROM (
	SELECT ArtistGenre.artistID, COUNT(ArtistGenre.artistID) AS Plays, ArtistGenre.genre
			FROM (
				SELECT song.artistID, RecordGenre.name AS genre FROM (
				SELECT record.recordID AS recordID, record.songID AS songID, SongGenreName.`name` AS `name`
                 FROM (SELECT SongGenre.songID, Genre.genreID, Genre.name FROM SongGenre
				INNER JOIN Genre ON SongGenre.genreID = Genre.genreID) AS SongGenreName
				INNER JOIN record ON record.songID = SongGenreName.songID
				) AS RecordGenre
				INNER JOIN song ON song.songID = RecordGenre.songID 
			) AS ArtistGenre
			GROUP BY ArtistGenre.artistID, ArtistGenre.genre
	) ArtistPlaysGenre
	GROUP BY GenreName
	ORDER BY TotalPlays DESC
) AS IDArtistPlays
INNER JOIN artist ON artist.artistID = IDArtistPlays.artistID;

/* REPORTE 5: LA CANCION MAS REPRODUCIDA POR CADA GENERO */

SELECT artist.name, IDArtistPlays.name, IDArtistPlays.GenreName, IDArtistPlays.TotalPlays
FROM (
	SELECT SongGenreGroup.artistID, SongGenreGroup.name AS `name`, MAX(SongGenreGroup.Plays) 
    AS TotalPlays, SongGenreGroup.GenreName
	FROM (
		SELECT song.songID AS songID , song.artistID, song.name, COUNT(SongsPlaysGenre.songID) 
        AS Plays, SongsPlaysGenre.genre AS GenreName
		FROM (
			SELECT record.recordID AS recordID, record.songID AS songID, SongGenreName.`name` AS genre 
			FROM (SELECT SongGenre.songID, Genre.genreID, Genre.name 
				FROM SongGenre
				INNER JOIN Genre 
				ON SongGenre.genreID = Genre.genreID) AS SongGenreName
			INNER JOIN record ON record.songID = SongGenreName.songID
		) AS SongsPlaysGenre
		INNER JOIN song ON song.songID = SongsPlaysGenre.songID
		GROUP BY song.songID, GenreName
	) AS SongGenreGroup
GROUP BY GenreName
ORDER BY TotalPlays DESC
) AS IDArtistPlays
INNER JOIN artist ON artist.artistID = IDArtistPlays.artistID;

/* REPORTE 6: LA CANCION MAS REPRODUCIDA POR CADA AÑO DE LANZAMIENTO*/

SELECT IDArtistPlays.`year`, artist.name, IDArtistPlays.name,  IDArtistPlays.TotalPlays
FROM (
	SELECT SongYearGroup.artistID, SongYearGroup.name AS `name`, MAX(SongYearGroup.Plays) 
    AS TotalPlays, SongYearGroup.`year` AS `year`
	FROM (
		SELECT song.songID AS songID , song.artistID, song.name, COUNT(SongsPlaysYear.songID)
         AS Plays, SongsPlaysYear.`year` AS `year`
		FROM (
			SELECT record.recordID AS recordID, record.songID AS songID, song.`year` AS `year`
			FROM song
			INNER JOIN record ON record.songID = song.songID
		) AS SongsPlaysYear
		INNER JOIN song ON song.songID = SongsPlaysYear.songID
		GROUP BY song.songID, SongsPlaysYear.`year`
	) AS SongYearGroup
	GROUP BY SongYearGroup.`year`
	ORDER BY SongYearGroup.`year` ASC
) AS IDArtistPlays
INNER JOIN artist ON artist.artistID = IDArtistPlays.artistID;

/* REPORTE 7: TOP 10 ARTISTAS MAS POPULARES */

SELECT artist.name, ArtistPopularity.popularityAVG AS popularityAVG
	FROM (
		SELECT song.artistID AS artistID, AVG(RecordPopularity.popularityAVG) AS popularityAVG
		FROM (
			SELECT songID, AVG(popularity) AS popularityAVG
			FROM record 
			GROUP BY  songID
		) AS RecordPopularity
		INNER JOIN song ON RecordPopularity.songID = song.songID
		GROUP BY artistID
	) AS ArtistPopularity
	INNER JOIN artist ON artist.artistID = ArtistPopularity.artistID
	ORDER BY ArtistPopularity.popularityAVG DESC
    LIMIT 10;

/* REPORTE 8: TOP 10 CANCIONES MAS POPULARES */

SELECT artist.name AS Artist, SongPopularity.name AS `Song`, SongPopularity.popularityAVG AS `Popularity Average`
FROM (
	SELECT song.songID, song.artistID, song.name, AVG(popularity) AS popularityAVG
	FROM record 
    INNER JOIN song
    ON record.songID = song.songID
	GROUP BY songID
) AS SongPopularity
INNER JOIN artist ON artist.artistID = SongPopularity.artistID
ORDER BY SongPopularity.popularityAVG DESC
LIMIT 10;

/* REPORTE 9: TOP 5 GENERO MAS POPULARES */

SELECT GenrePopularityGroup.GenreName, GenrePopularityGroup.popularityAVG AS `Popularity Average`
FROM (
	SELECT song.songID AS songID , song.artistID, song.name, AVG(SongsPopularityGenre.popularity) 
    AS popularityAVG, SongsPopularityGenre.genre AS GenreName
	FROM (
		SELECT record.recordID AS recordID, record.songID AS songID, record.popularity 
        AS popularity, SongGenreName.`name` AS genre 
		FROM (SELECT SongGenre.songID, Genre.genreID, Genre.name 
			FROM SongGenre
			INNER JOIN Genre 
			ON SongGenre.genreID = Genre.genreID) AS SongGenreName
		INNER JOIN record ON record.songID = SongGenreName.songID
	) AS SongsPopularityGenre
	INNER JOIN song ON song.songID = SongsPopularityGenre.songID
	GROUP BY song.songID, GenreName
) AS GenrePopularityGroup
GROUP BY GenrePopularityGroup.GenreName
ORDER BY GenrePopularityGroup.popularityAVG DESC
LIMIT 5;	

/* REPORTE 10: */

SELECT artist.name, IDArtistPlays.name, IDArtistPlays.GenreName, IDArtistPlays.TotalPlays
FROM (
	SELECT SongGenreGroup.artistID, SongGenreGroup.name AS `name`, MAX(SongGenreGroup.Plays) 
    AS TotalPlays, SongGenreGroup.GenreName
	FROM (
		SELECT song.songID AS songID , song.artistID, song.name, COUNT(SongsPlaysGenre.songID) 
        AS Plays, SongsPlaysGenre.genre AS GenreName
		FROM (
			SELECT record.recordID AS recordID, record.songID AS songID, SongGenreName.`name` AS genre 
			FROM (SELECT SongGenre.songID, Genre.genreID, Genre.name 
				FROM SongGenre
				INNER JOIN Genre 
				ON SongGenre.genreID = Genre.genreID) AS SongGenreName
			INNER JOIN record ON record.songID = SongGenreName.songID
		) AS SongsPlaysGenre
		INNER JOIN song ON song.songID = SongsPlaysGenre.songID
        WHERE song.explicit = True
		GROUP BY song.songID, GenreName
	) AS SongGenreGroup
GROUP BY GenreName
ORDER BY TotalPlays DESC
) AS IDArtistPlays
INNER JOIN artist ON artist.artistID = IDArtistPlays.artistID;
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
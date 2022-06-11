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
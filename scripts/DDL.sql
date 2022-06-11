
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';


CREATE SCHEMA IF NOT EXISTS Semi2DB DEFAULT CHARACTER SET utf8 ;
USE Semi2DB ;


CREATE TABLE IF NOT EXISTS Semi2DB.`TablaTemporal` (
  `artist` VARCHAR(200) NULL,
  `song` VARCHAR(200) NULL,
  `duration_ms` INT NULL,
  `explicit` VARCHAR(45) NULL,
  `year` INT NULL,
  `popularity` DECIMAL(15,6) NULL,
  `danceability` DECIMAL(15,6) NULL,
  `energy` DECIMAL(15,6) NULL,
  `key` DECIMAL(15,6) NULL,
  `loudness` DECIMAL(15,6) NULL,
  `mode` DECIMAL(15,6) NULL,
  `speechiness` DECIMAL(15,6) NULL,
  `acousticness` DECIMAL(15,6) NULL,
  `instrumentalness` DECIMAL(15,6) NULL,
  `liveness` DECIMAL(15,6) NULL,
  `valence` DECIMAL(15,6) NULL,
  `tempo` DECIMAL(15,6) NULL,
  `genre` VARCHAR(200) NULL);



CREATE TABLE IF NOT EXISTS Semi2DB.`artist` (
  `artistID` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(200) NULL,
  PRIMARY KEY (`artistID`));



CREATE TABLE IF NOT EXISTS Semi2DB.`genre` (
  `genreID` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(200) NULL,
  PRIMARY KEY (`genreID`));



CREATE TABLE IF NOT EXISTS Semi2DB.`song` (
  `songID` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(200) NULL,
  `duration_ms` INT NULL,
  `explicit` VARCHAR(45) NULL,
  `year` INT NULL,
  `artistID` INT NOT NULL,
  PRIMARY KEY (`songID`),
  INDEX `fk_song_artist1_idx` (`artistID` ASC) VISIBLE,
  CONSTRAINT `fk_song_artist1`
    FOREIGN KEY (`artistID`)
    REFERENCES Semi2DB.`artist` (`artistID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);



CREATE TABLE IF NOT EXISTS Semi2DB.`record` (
  `recordID` INT NOT NULL AUTO_INCREMENT,
  `popularity` DECIMAL(15,6) NULL,
  `danceability` DECIMAL(15,6) NULL,
  `energy` DECIMAL(15,6) NULL,
  `key` DECIMAL(15,6) NULL,
  `loudness` DECIMAL(15,6) NULL,
  `mode` DECIMAL(15,6) NULL,
  `speechiness` DECIMAL(15,6) NULL,
  `acousticness` DECIMAL(15,6) NULL,
  `instrumentalness` DECIMAL(15,6) NULL,
  `liveness` DECIMAL(15,6) NULL,
  `valence` DECIMAL(15,6) NULL,
  `tempo` DECIMAL(15,6) NULL,
  `songID` INT NOT NULL,
  PRIMARY KEY (`recordID`),
  INDEX `fk_record_song1_idx` (`songID` ASC) VISIBLE,
  CONSTRAINT `fk_record_song1`
    FOREIGN KEY (`songID`)
    REFERENCES Semi2DB.`song` (`songID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);



CREATE TABLE IF NOT EXISTS Semi2DB.`SongGenre` (
  `idSongGenre` INT NOT NULL AUTO_INCREMENT,
  `genre_genreID` INT NOT NULL,
  `songID` INT NOT NULL,
  PRIMARY KEY (`idSongGenre`),
  INDEX `fk_SongGenre_genre1_idx` (`genre_genreID` ASC) VISIBLE,
  INDEX `fk_SongGenre_song1_idx` (`songID` ASC) VISIBLE,
  CONSTRAINT `fk_SongGenre_genre1`
    FOREIGN KEY (`genre_genreID`)
    REFERENCES Semi2DB.`genre` (`genreID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_SongGenre_song1`
    FOREIGN KEY (`songID`)
    REFERENCES Semi2DB.`song` (`songID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

import mysql.connector
from dotenv import load_dotenv
import traceback
import pandas
import time
import os
from datetime import datetime

# Setting env variables
load_dotenv()
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']

myDB = None

try:
    myDB = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    auth_plugin='mysql_native_password'
    )
except Exception:
    traceback.print_exc()

class CLI():
    
    def __init__(self):    
        while(True):
            os.system('cls||clear')
            print("\x1b[1;31m"+"------------------ SEMINARIO DE SISTEMAS 2: PRACTICA 1 -----------------")
            print("\x1b[1;36m"+"---------------- OSCAR ALFREDO LLAMAS LEMUS - 201602625 ----------------")
            menu()        
            keyInput = input("\x1b[1;37m"+"")    
            if keyInput == "1":
                createModel()
            if keyInput == "2":
                loadData()
            if keyInput == "3":
                selectQuery()
            if keyInput == "4":
                dropModel()
            if keyInput == "5" or keyInput.lower() == "exit":
                print("\x1b[1;31m"+"\nHASTA LA PROXIMA :D")
                exit()

def selectQuery():
    while(True):
        os.system('cls||clear')
        print("\x1b[1;31m"+"------------------ SEMINARIO DE SISTEMAS 2: PRACTICA 1 -----------------")
        print("\x1b[1;36m"+"---------------- OSCAR ALFREDO LLAMAS LEMUS - 201602625 ----------------")
        queriesMenu()        
        keyInput = input("\x1b[1;37m"+"")    
        if keyInput == "1":
            Query1()
        if keyInput == "2":
            Query2()
        if keyInput == "3":
            Query3()
        if keyInput == "11" or keyInput.lower() == "exit":
            break            
                
def menu():
    print("\x1b[1;34m"+"\n---------------------------- ELIGE UNA OPCION ----------------------------")
    print("\x1b[1;32m"+"1) CREAR MODELO (DDL)")
    print("\x1b[1;33m"+"2) CARGAR INFORMACION (DML)")
    print("\x1b[1;35m"+"3) REALIZAR CONSULTAS (DML)")
    print("\x1b[1;31m"+"4) ELIMINAR MODELO (DDL)")
    print("\x1b[1;36m"+"5) SALIR\n")
    print("\x1b[1;32m"+"USAC ", end='')
    print("\x1b[1;33m"+"> ", end='')
    
def queriesMenu():
    print("\x1b[1;34m"+"\n---------------------------- ELIGE UNA CONSULTA ----------------------------")
    print("\x1b[1;35m"+"1) TOP 10 ARTISTAS CON MAYOR REPRODUCCIONES")
    print("\x1b[1;32m"+"2) TOP 10 CANCIONES MAS REPRODUCIDAS") 
    print("\x1b[1;33m"+"3) TOP 5 GENEROS MAS REPRODUCIDOS")
    print("\x1b[1;36m"+"11) SALIR\n")
    print("\x1b[1;32m"+"USAC ", end='')
    print("\x1b[1;33m"+"> ", end='')

def createModel():
    try:
        # Creating Schema
        myCursor = myDB.cursor()
        myQuery = "CREATE SCHEMA IF NOT EXISTS Semi2DB DEFAULT CHARACTER SET utf8"
        myCursor.execute(myQuery)
        # Select new Schema
        myQuery = "USE Semi2DB"
        myCursor.execute(myQuery)
        
        # Creating temporal table to data from .csv file
        myQuery = """CREATE TABLE IF NOT EXISTS Semi2DB.`TablaTemporal` (
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
                    `genre` VARCHAR(200) NULL)"""
        myCursor.execute(myQuery)
        # Creating artist table
        myQuery = """CREATE TABLE IF NOT EXISTS Semi2DB.`artist` (
                    `artistID` INT NOT NULL AUTO_INCREMENT,
                    `name` VARCHAR(200) NULL,
                    PRIMARY KEY (`artistID`))"""
        myCursor.execute(myQuery)
        # Creating genre table
        myQuery = """CREATE TABLE IF NOT EXISTS Semi2DB.`genre` (
                    `genreID` INT NOT NULL AUTO_INCREMENT,
                    `name` VARCHAR(200) NULL,
                    PRIMARY KEY (`genreID`))"""
        myCursor.execute(myQuery)
        # Creating genre song
        myQuery = """CREATE TABLE IF NOT EXISTS Semi2DB.`song` (
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
                        ON UPDATE NO ACTION)"""
        myCursor.execute(myQuery)
        # Creating genre record
        myQuery = """CREATE TABLE IF NOT EXISTS Semi2DB.`record` (
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
                        ON UPDATE NO ACTION)"""
        myCursor.execute(myQuery)
         # Creating SongGenre Table
        myQuery = """CREATE TABLE IF NOT EXISTS Semi2DB.`SongGenre` (
                    `idSongGenre` INT NOT NULL AUTO_INCREMENT,
                    `songID` INT NOT NULL,
                    `genreID` INT NOT NULL,
                    PRIMARY KEY (`idSongGenre`),
                    INDEX `fk_SongGenre_song1_idx` (`songID` ASC) VISIBLE,
                    INDEX `fk_SongGenre_genre1_idx` (`genreID` ASC) VISIBLE,
                    CONSTRAINT `fk_SongGenre_song1`
                        FOREIGN KEY (`songID`)
                        REFERENCES Semi2DB.`song` (`songID`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                    CONSTRAINT `fk_SongGenre_genre1`
                        FOREIGN KEY (`genreID`)
                        REFERENCES Semi2DB.`genre` (`genreID`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION)"""
        myCursor.execute(myQuery)
        myCursor.close()
        myDB.commit()
        print("\x1b[1;33m"+'Modelo en MySQL creado :D')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al crear modelo :o')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    
def dropModel():
    try:
        # Droping Schema
        myCursor = myDB.cursor()
        myQuery = "DROP SCHEMA IF EXISTS Semi2DB"
        myCursor.execute(myQuery)   
        myCursor.close()  
        myDB.commit()
        print("\x1b[1;33m"+'Modelo en MySQL eliminado :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al eliminar modelo :o')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

def loadData():
    try:
        print("\x1b[1;34m"+"\n++++++++++++++++++++++++ CARGANDO INFORMACION ++++++++++++++++++++++++")
        csvData = pandas.read_csv(r'data/songs_normalize.csv')
        df = pandas.DataFrame(csvData)
        df = df.fillna(value=0)
        myCursor = myDB.cursor()
        # Select new Schema
        myQuery = "USE Semi2DB"
        myCursor.execute(myQuery)
        for row in df.itertuples(index=False):        
            myQuery = '''INSERT INTO TablaTemporal (artist, song, duration_ms, explicit, `year`,
            popularity, danceability, energy, `key`, loudness, `mode`, speechiness, acousticness,
            instrumentalness, liveness, valence, tempo, genre) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values = (row.artist, row.song, row.duration_ms, row.explicit, row.year, 
                    row.popularity, row.danceability, row.energy, row.key, row.loudness, 
                    row.mode, row.speechiness, row.acousticness, row.instrumentalness, 
                    row.liveness, row.valence, row.tempo, row.genre)
            myCursor.execute(myQuery, values)
        myDB.commit()
        # Data to artist table
        myQuery = '''INSERT INTO artist(name)
                    SELECT DISTINCT(artist) FROM TablaTemporal
                    ORDER BY artist'''  
        myCursor.execute(myQuery)
        myDB.commit()
        # Data to genre table
        myQuery = '''INSERT INTO genre(name)     
                    SELECT
                    DISTINCT(SUBSTRING_INDEX(SUBSTRING_INDEX(TablaTemporal.genre, ',', numbers.n), ',', -1)) genre
                    FROM
                    (SELECT 1 n UNION ALL SELECT 2
                    UNION ALL SELECT 3 UNION ALL SELECT 4) numbers INNER JOIN TablaTemporal
                    ON CHAR_LENGTH(TablaTemporal.genre)
                        -CHAR_LENGTH(REPLACE(TablaTemporal.genre, ',', ''))>=numbers.n-1'''  
        myCursor.execute(myQuery)
        myDB.commit()
        # Data to song table
        myQuery = '''INSERT INTO song(`name`, duration_ms, explicit, `year`, artistID)
                    SELECT song, duration_ms, explicit, `year`, 
                    (SELECT artistID FROM artist WHERE artist.name = TablaTemporal.artist) AS artistID                
                    FROM TablaTemporal
                    GROUP BY artist, song, duration_ms, explicit, `year`'''  
        myCursor.execute(myQuery)
        myDB.commit()
        # Data to record table
        myQuery = '''INSERT INTO record(popularity, danceability, energy, `key`, loudness, `mode`, 
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
                    FROM TablaTemporal'''  
        myCursor.execute(myQuery)
        myDB.commit()
        # Data to songGenre table
        myQuery = '''INSERT INTO SongGenre(genreID, songID)
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
                    GROUP BY songID, genreID'''  
        myCursor.execute(myQuery)
        myCursor.close()
        myDB.commit()
        print("\x1b[1;33m"+"SE HAN CARGADO LOS DATOS EXITOSAMENTE :D")
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al cargar informacion :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    
def Query1():
    try:
        # Top 10 most played artist
        myCursor = myDB.cursor()
        # Select new Schema
        myQuery = "USE Semi2DB"
        myCursor.execute(myQuery)
        myQuery = '''SELECT artist.name, COUNT(SongsPlays.name) AS Plays
                    FROM (
                    SELECT song.artistID AS artistID, song.name AS name FROM record
                    INNER JOIN song ON record.songID = song.songID
                    ) AS SongsPlays
                    INNER JOIN artist ON SongsPlays.artistID = artist.artistID
                    GROUP BY artist.name
                    ORDER BY Plays DESC
                    LIMIT 10'''
        myCursor.execute(myQuery)    
        myFile = open("consulta1.txt", "w")
        print("------------ CONSULTA 1 ------------", file=myFile)
        print(file=myFile)
        for row in myCursor:
            print(row, file=myFile)
        myCursor.close() 
        myFile.close()
        print("\x1b[1;33m"+"2) Reporte de consulta 1 generado exitosamente :D")
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al generar reporte :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")    
    
def Query2():
    try:
        # Top 10 most played songs
        myCursor = myDB.cursor()
        # Select new Schema
        myQuery = "USE Semi2DB"
        myCursor.execute(myQuery)
        myQuery = '''SELECT artist.name, SongsPlays.name, SongsPlays.Plays
                    FROM (
                    SELECT song.artistID AS artistID, song.name AS name, COUNT(record.songID) AS Plays FROM record
                    INNER JOIN song ON record.songID = song.songID
                    GROUP BY record.songID
                    ) AS SongsPlays
                    INNER JOIN artist ON SongsPlays.artistID = artist.artistID
                    ORDER BY Plays DESC
                    LIMIT 10'''
        myCursor.execute(myQuery)     
        myFile = open("consulta2.txt", "w")
        print("------------ CONSULTA 2 ------------", file=myFile)
        print(file=myFile)
        for row in myCursor:
            print(row, file=myFile)
        myCursor.close()
        myFile.close()
        print("\x1b[1;33m"+"2) Reporte de consulta 2 generado exitosamente :D")
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al generar reporte :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

def Query3():
    try:
        # Top 5 most played genres
        myCursor = myDB.cursor()
        # Select new Schema
        myQuery = "USE Semi2DB"
        myCursor.execute(myQuery)
        myQuery = '''SELECT `name`, COUNT(`name`) AS Plays FROM (
                        SELECT record.recordID, record.songID, SongGenreName.`name` AS `name` FROM (SELECT SongGenre.songID, Genre.genreID, Genre.name FROM SongGenre
                        INNER JOIN Genre ON SongGenre.genreID = Genre.genreID) AS SongGenreName
                        INNER JOIN record ON record.songID = SongGenreName.songID
                    ) AS RecordGenre
                    GROUP BY `name` 
                    ORDER BY Plays DESC 
                    LIMIT 5'''
        myCursor.execute(myQuery)
        myFile = open("consulta3.txt", "w")
        print("------------ CONSULTA 3 ------------", file=myFile)
        print(file=myFile)
        for row in myCursor:
            print(row, file=myFile)
        myCursor.close()     
        myFile.close()
        print("\x1b[1;33m"+"2) Reporte de consulta 3 generado exitosamente :D")
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al generar reporte :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

myApp = CLI()
import mysql.connector
from dotenv import load_dotenv
import traceback
import pandas
import time
import os

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
            # if keyInput == "3":
            #     Consultas()
            if keyInput == "4":
                dropModel()
            if keyInput == "5" or keyInput.lower() == "exit":
                print("\x1b[1;31m"+"\nHASTA LA PROXIMA :D")
                exit()
            
                

def menu():
    print("\x1b[1;34m"+"\n+++++++++++++++++++++++++++ ELIGE UNA OPCION +++++++++++++++++++++++++++")
    print("\x1b[1;32m"+"1) CREAR MODELO (DDL)")
    print("\x1b[1;33m"+"2) CARGAR INFORMACION (DML)")
    print("\x1b[1;35m"+"3) REALIZAR CONSULTAS (DML)")
    print("\x1b[1;31m"+"4) ELIMINAR MODELO (DDL)")
    print("\x1b[1;36m"+"5) SALIR\n")
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
        myDB.commit()
        print("\x1b[1;33m"+'Modelo en MySQL eliminado :(')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    except Exception as e: 
        print(e)
        print('Error al eliminar modelo :o')
        input("\x1b[1;31m"+"Presiona ENTER para continuar...")

def loadData():
    print("\x1b[1;34m"+"\n++++++++++++++++++++++++ CARGANDO INFORMACION ++++++++++++++++++++++++")
    csvData = pandas.read_csv(r'data/songs_normalize.csv')
    df = pandas.DataFrame(csvData)
    df = df.fillna(value=0)
    myCursor = myDB.cursor()
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
                ) AS SongsGenres'''  
    myCursor.execute(myQuery)
    myDB.commit()
    print("\x1b[1;33m"+"SE HAN CARGADO LOS DATOS EXITOSAMENTE :D")
    input("\x1b[1;31m"+"Presiona ENTER para continuar...")
    
# def Consultas():
#     cursor = conexion.cursor()
#     #CONSULTA 1
#     cursor.execute("select count(*) from alerta;")
#     consulta1 = cursor.fetchone()
#     #CONSULTA 2
#     cursor.execute('''select * from (
#                         select f.anio, u.pais, row_number() over(partition by f.anio ORDER BY u.pais) AS d from alerta a
#                         inner join ubicacion u on u.id_ubicacion=a.id_ubicacion
#                         inner join fecha f on f.id_fecha = a.id_fecha
#                         where u.pais <> '0' and a.magnitud > 0
#                         group by u.pais, f.anio 
#                     ) S
#                     pivot(
#                         max(pais)
#                         for [d] in ([1],[2],[3],[4],[5])
#                     ) P''')
#     consulta2 = cursor.fetchall()
#     #CONSULTA 3
#     cursor.execute('''select * from (
#                         select u.pais, f.anio, row_number() over(partition by u.pais ORDER BY f.anio) AS d from alerta a
#                         inner join ubicacion u on u.id_ubicacion=a.id_ubicacion
#                         inner join fecha f on f.id_fecha = a.id_fecha
#                         where u.pais <> '0' and a.magnitud > 0
#                         group by f.anio, u.pais 
#                     ) S
#                     pivot(
#                         max(anio)
#                         for [d] in ([1],[2],[3],[4],[5])
#                     ) P''')
#     consulta3 = cursor.fetchall()
#     #CONSULTA 4
#     cursor.execute('''select u.pais, avg(a.danios) prom from alerta a
#                         inner join ubicacion u on u.id_ubicacion = a.id_ubicacion
#                         where a.danios > 0 
#                         group by u.pais
#                         order by prom desc
#                         ;''')
#     consulta4 = cursor.fetchall()
#     #CONSULTA 5
#     cursor.execute('''select top 5 u.pais, sum(a.muertes) suma from alerta a
#                         inner join ubicacion u on a.id_ubicacion = u.id_ubicacion
#                         group by u.pais order by suma desc;''')
#     consulta5 = cursor.fetchall()
#     #CONSULTA 6
#     cursor.execute('''select top 5 f.anio, sum(a.muertes) as suma from alerta a
#                         inner join fecha f on f.id_fecha = a.id_fecha
#                         group by f.anio order by suma desc;''')
#     consulta6 = cursor.fetchall()
#     #CONSULTA 7
#     cursor.execute('''select top 5 f.anio, count(*) as total from alerta a 
#                         inner join fecha f on f.id_fecha = a.id_fecha 
#                         where a.magnitud > 0 
#                         group by f.anio
#                         order by total desc;''')
#     consulta7 = cursor.fetchall()
#     #CONSULTA 8
#     cursor.execute('''select top 5 u.pais, sum(a.casasdestruidas) as suma from alerta a
#                         inner join ubicacion u on u.id_ubicacion = a.id_ubicacion
#                         group by u.pais order by suma desc;''')
#     consulta8 = cursor.fetchall()
#     #CONSULTA 9
#     cursor.execute('''select top 5 u.pais, sum(a.casasdañadas) as suma from alerta a
#                         inner join ubicacion u on u.id_ubicacion = a.id_ubicacion
#                         group by u.pais order by suma desc;''')
#     consulta9 = cursor.fetchall()
#     #CONSULTA 10
#     cursor.execute('''select u.pais, avg(a.altura) prom from alerta as a
#                         inner join ubicacion u on u.id_ubicacion = a.id_ubicacion
#                         where a.altura > 0
#                         group by u.pais
#                         order by prom desc;''')
#     consulta10 = cursor.fetchall()
#     #-------------------------GENERANDO TXT--------------------------
#     file = open("consultas.txt", "w")
#     file.write("****************CONSULTA 1*********************"+os.linesep)
#     file.write(str(consulta1[0])+os.linesep)
#     file.write("****************CONSULTA 2*********************"+os.linesep)
#     file.write("---ANIO---          -------------PAISES-----------\n")
#     for dato in consulta2:
#         file.write("-----------------------------------------------------------------\n")
#         for pais in dato:
#             if str(pais)!="None":
#                 file.write(str(pais) + "\t")
#             else:
#                 file.write("    ")
#         file.write("\n")
#     file.write(os.linesep)
#     file.write("****************CONSULTA 3*********************"+os.linesep)
#     file.write("---PAIS---          ----------ANIOS-----------\n")
#     for dato in consulta3:
#         file.write("-----------------------------------------------------------------\n")
#         for año in dato:
#             if str(año)!="None":
#                 file.write(str(año) + "\t")
#             else:
#                 file.write("    ")
#         file.write("\n")
#     file.write(os.linesep)
#     file.write("****************CONSULTA 4*********************"+os.linesep)
#     file.write("---PAIS---          --PROMEDIO--\n")
#     for dato in consulta4:
#         file.write("--------------------------------------------\n")
#         file.write(str(dato[0])+"            "+str(dato[1])+"\n")
#     file.write(os.linesep)
#     file.write("****************CONSULTA 5*********************"+os.linesep)
#     file.write("---PAIS---          --# MUERTES--\n")
#     for dato in consulta5:
#         file.write("--------------------------------------------\n")
#         file.write(str(dato[0])+"            "+str(dato[1])+"\n")
#     file.write(os.linesep)
#     file.write("****************CONSULTA 6*********************"+os.linesep)
#     file.write("---ANIOS---          --# MUERTES--\n")
#     for dato in consulta6:
#         file.write("--------------------------------------------\n")
#         file.write(str(dato[0])+"            "+str(dato[1])+"\n")
#     file.write(os.linesep)
#     file.write("****************CONSULTA 7*********************"+os.linesep)
#     file.write("---ANIOS---          --# TSUNAMIS--\n")
#     for dato in consulta7:
#         file.write("--------------------------------------------\n")
#         file.write(str(dato[0])+"            "+str(dato[1])+"\n")
#     file.write(os.linesep)
#     file.write("****************CONSULTA 8*********************"+os.linesep)
#     file.write("---PAIS---          --# CASAS DESTRUIDAS--\n")
#     for dato in consulta8:
#         file.write("--------------------------------------------\n")
#         file.write(str(dato[0])+"            "+str(dato[1])+"\n")
#     file.write(os.linesep)
#     file.write("****************CONSULTA 9*********************"+os.linesep)
#     file.write("---PAIS---          --# CASAS DANIADAS--\n")
#     for dato in consulta9:
#         file.write("--------------------------------------------\n")
#         file.write(str(dato[0])+"            "+str(dato[1])+"\n")
#     file.write(os.linesep)
#     file.write("****************CONSULTA 10********************"+os.linesep)
#     file.write("---PAIS---          --PROM. ALTURA MAXIMA--\n")
#     for dato in consulta10:
#         file.write("--------------------------------------------\n")
#         file.write(str(dato[0])+"            "+str(dato[1])+"\n")
#     file.write(os.linesep)
#     file.close()
#     cursor.close()


# def EjSelect():
#     cursor = conexion.cursor()#estamos creando un cursor para enviar el query
#     cursor.execute("select * from fecha;")
#     respuesta = cursor.fetchone()#Seleccione uno por uno (fetchall si quiero todo se recorre con un for)
#     while respuesta:
#         print(respuesta)
#         respuesta = cursor.fetchone()
    
#     #poner que siempre se cierre el cursor para ahorrar recursos en python 
#     cursor.close()

myApp = CLI()
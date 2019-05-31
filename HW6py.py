# -*- coding: utf-8 -*-
"""
Created on Fri May 24 09:16:53 2019

@author: Jesse
"""
import sqlite3

def connect_to_db(): 
 """Connect to db. RETURN connection to db and cursor object."""
    
 db = input("Enter name of Database you want to connect to: ")
 print('')
 con = sqlite3.connect(db)
 cur = con.cursor()
 return con, cur

def drop_tables(cur): 
 """drop all tables IF EXIST."""
    
 cur.execute("DROP TABLE IF EXISTS MOVIES")
 cur.execute("DROP TABLE IF EXISTS RATINGS")
 cur.execute("DROP TABLE IF EXISTS USER")
 cur.execute("DROP TABLE IF EXISTS CATEGORY")
 cur.execute("DROP TABLE IF EXISTS AGE")
 cur.execute("DROP TABLE IF EXISTS OCCUPATION")
 
 
def create_tables(con, cur): # 
 """Creates all necessary the db tables."""
    # Include column constraints in your table definitions that correspond to the README file.
    
 cur.execute("""CREATE TABLE MOVIES  
            (
             MovieID TEXT PRIMARY KEY NOT NULL, 
             MovieTitle TEXT,
             Year TEXT
            );
            """)
                    
 cur.execute("""CREATE TABLE RATINGS
            ( 
            UserID TEXT NOT NULL,
            MovieID TEXT NOT NULL,
            Rating TEXT,
            Timestamp TEXT,
            
            PRIMARY KEY (UserID , MovieID)
            );
            """)

 cur.execute("""CREATE TABLE USER
            ( 
            UserID TEXT PRIMARY KEY NOT NULL,
            Gender TEXT,
            AgeCode TEXT,
            OccupationCode TEXT,
            Zipcode TEXT
            );
            """)

 cur.execute("""CREATE TABLE CATEGORY 
            ( 
            C_MOVIEID TEXT PRIMARY KEY NOT NULL,
            CATEGORY1 TEXT,
            CATEGORY2 TEXT,
            CATEGORY3 TEXT,
            CATEGORY4 TEXT,
            CATEGORY5 TEXT
            );
            """)
            
 cur.execute("""CREATE TABLE AGE
            ( 
            AgeCode TEXT PRIMARY KEY NOT NULL,
            AgeRange TEXT
            );
            """)
 
 cur.execute("""CREATE TABLE OCCUPATION
            ( 
            OccupationCode TEXT PRIMARY KEY NOT NULL,
            Occupation TEXT
            );
            """)
 con.commit()


def insert_into_AGE(cur): 
 """Insert Age data from flat file."""

 cur.execute("INSERT INTO AGE VALUES('1', 'Under 18');")
 cur.execute("INSERT INTO AGE VALUES('18', '18-24');")
 cur.execute("INSERT INTO AGE VALUES('25', '25-34');")
 cur.execute("INSERT INTO AGE VALUES('35', '35-44');")
 cur.execute("INSERT INTO AGE VALUES('45', '45-49');")
 cur.execute("INSERT INTO AGE VALUES('50', '50-55');")
 cur.execute("INSERT INTO AGE VALUES('56', '56+');")
 con.commit()
 
 print(" ")
 print('Number of rows inserted into the Agetable: 7')
 print(" ")
 
def insert_into_OCCUPATION(cur):
 """Insert Occupation data from flat file."""

 cur.execute("INSERT INTO OCCUPATION VALUES('0','other or not specified');")
 cur.execute("INSERT INTO OCCUPATION VALUES('1', 'academic/educator');")
 cur.execute("INSERT INTO OCCUPATION VALUES('2', 'artist');")
 cur.execute("INSERT INTO OCCUPATION VALUES('3', 'clerical/admin');")
 cur.execute("INSERT INTO OCCUPATION VALUES('4', 'college/grad student');")
 cur.execute("INSERT INTO OCCUPATION VALUES('5', 'customer service');")
 cur.execute("INSERT INTO OCCUPATION VALUES('6', 'doctor/health care');")
 cur.execute("INSERT INTO OCCUPATION VALUES('7', 'executive/managerial');")
 cur.execute("INSERT INTO OCCUPATION VALUES('8', 'farmer');")
 cur.execute("INSERT INTO OCCUPATION VALUES('9', 'homemaker');")
 cur.execute("INSERT INTO OCCUPATION VALUES('10', 'K-12 student');")
 cur.execute("INSERT INTO OCCUPATION VALUES('11', 'lawyer');")
 cur.execute("INSERT INTO OCCUPATION VALUES('12', 'programmer');")
 cur.execute("INSERT INTO OCCUPATION VALUES('14', 'sales/marketing');")
 cur.execute("INSERT INTO OCCUPATION VALUES('15', 'scientist');")
 cur.execute("INSERT INTO OCCUPATION VALUES('16', 'self-employed');")
 cur.execute("INSERT INTO OCCUPATION VALUES('17', 'technician/engineer');")
 cur.execute("INSERT INTO OCCUPATION VALUES('18', 'tradesman/craftsman');")
 cur.execute("INSERT INTO OCCUPATION VALUES('19',  'unemployed');")
 cur.execute("INSERT INTO OCCUPATION VALUES('20', 'writer');")
 con.commit()
 
 print(" ")
 print('Number of rows inserted into the Occupation table: 21')
 print(" ")

def read_file_movies():
    """Reads in the file movies.dat."""
    file = open('movies.dat', 'r')   
    inFile = file.readlines()
    file.close
    
    return inFile

def read_file_rating():
    """Reads in the file ratings.dat"""
    file = open('ratings.dat', 'r')   
    inFile = file.readlines()
    file.close
    
    return inFile

def read_file_users():
    """Reads in the file users.dat."""
    file = open('users.dat', 'r')   
    inFile = file.readlines()
    file.close
    
    return inFile


def process_movies(mfile):
    """Insert movie records from 'read_file_movies' into Movies table 
    and parse the category col into own table Category table."""
    row = ''
    MovieID = '' 
    MovieTitle = ''
    MCategory = ''
    tempMovieTitle = ''
    rowCount = 0
    
    for record in mfile:
        rowCount = rowCount + 1
        row = record.split('::')
        
        tempMovieTitle = row[1].split('(')
        
        MovieID = row[0]
        MovieTitle = tempMovieTitle[0]
        Year = tempMovieTitle[1].rstrip(')')
        MCategory = row[2].rstrip().split('|')
                
        cur.execute("INSERT INTO MOVIES VALUES(?, ?, ?)", (MovieID, MovieTitle, Year)) 
        
        if len(MCategory) == 1:
            cur.execute("INSERT INTO CATEGORY VALUES(?, ?, ?, ?, ?, ?)", (MovieID, MCategory[0], None, None, None, None))
        elif len(MCategory) == 2:
            cur.execute("INSERT INTO CATEGORY VALUES(?, ?, ?, ?, ?, ?)", (MovieID, MCategory[0], MCategory[1], None, None, None))
        elif len(MCategory) == 3:
            cur.execute("INSERT INTO CATEGORY VALUES(?, ?, ?, ?, ?, ?)", (MovieID, MCategory[0], MCategory[1], MCategory[2], None, None)) 
        elif len(MCategory) == 4:
            cur.execute("INSERT INTO CATEGORY VALUES(?, ?, ?, ?, ?, ?)", (MovieID, MCategory[0], MCategory[1], MCategory[2], MCategory[3], None))
        elif len(MCategory) == 5:
            cur.execute("INSERT INTO CATEGORY VALUES(?, ?, ?, ?, ?, ?)", (MovieID, MCategory[0], MCategory[1], MCategory[2], MCategory[3], MCategory[4]))
        else:
            cur.execute("INSERT INTO CATEGORY VALUES(?, ?, ?, ?, ?, ?)", (MovieID, None, None, None, None, None))
            
            
            
    print('Number of rows inserted into the Movies table: ' + str(rowCount))
    print(" ")
    print('Number of rows inserted into the Category table: ' + str(rowCount))
    
def process_rating(rfile):      
    """Insert ratings records from 'read_file_rating()' into Ratings table."""
    rowCount = 0
    
    for record in rfile:
        rowCount = rowCount + 1
        row = record.split('::') # delimiter
        
        UserID = row[0]
        MovieID  = row[1]
        Rating = row[2]
        Timestamp = row[3].strip()
                
        cur.execute("INSERT INTO RATINGS VALUES(?, ?, ?, ?)", (UserID, MovieID, Rating,Timestamp))
        
    print(" ")
    print('Number of rows inserted into the Ratings table: ' + str(rowCount))
    
def process_users(ufile):    
    """Insert user records from 'read_file_users()' into Users table."""
    rowCount = 0
    
    for record in ufile:
        
        rowCount = rowCount + 1
        
        row = record.split('::') # hw6db.db
        
        UserID = row[0]
        Gender  = row[1]
        AgeCode = row[2]
        OccupationCode = row[3]
        Zipcode = row[4].strip()
                
        cur.execute("INSERT INTO USER VALUES(?, ?, ?, ?, ?)", (UserID, Gender, AgeCode, OccupationCode, Zipcode))
    
    print(" ")
    print('Number of rows inserted into the Users table: ' + str(rowCount))

        
def QUESTION_1(cur):# 1) AVG rating of the movie "House II: The Second Story".

 cur.execute(
"""SELECT ROUND(AVG(RATINGS.Rating),2)
   FROM RATINGS
   INNER JOIN MOVIES
   ON RATINGS.MovieID = MOVIES.MovieID
   WHERE MOVIES.MovieID = "2149";"""
   )

 print('') 
 for record in cur:
     print('Query 1: The average rating of the movie "House II: The Second Story" is ' + str(record[0]) + ' stars.')    
 print('') 

def QUESTION_2(cur):# 2) WHAT MOVIES DID FEMALES, UNDER 18, WIth NO JOB, RATE 5 OUT 5, AT LEAST TWICE.

 cur.execute(
"""SELECT MOVIES.MovieTitle, MOVIES.year, COUNT(MOVIES.MovieTitle)
   FROM USER
   INNER JOIN AGE
   ON USER.AgeCode = AGE.AgeCode
   INNER JOIN OCCUPATION
   ON USER.OccupationCode = OCCUPATION.OccupationCode
   INNER JOIN RATINGS
   ON USER.UserID = RATINGS.UserID
   INNER JOIN MOVIES
   ON RATINGS.MovieID = MOVIES.MovieID
   WHERE USER.AgeCode = '1' AND OCCUPATION.OccupationCode = '0' AND USER.Gender = 'F' AND RATINGS.Rating = '5'
   GROUP BY MOVIES.MovieTitle
   HAVING COUNT(MOVIES.MovieTitle) > 2
   ;"""
   )
 names = [description[0] for description in cur.description] # col names.
 
 print('') 
 print('Query 2:') 
 print('') 
 print(names)
 print('') 
 for record in cur:
    print(record)      
 print('') 
 
 
###############################################################################
############################### MAIN BLOCK ####################################
###############################################################################
con, cur = connect_to_db() # connect to db.
drop_tables(cur) # drop all tables IF EXIST.
create_tables(con,cur) # create all db tables.
###############################################################################
mfile = read_file_movies() 
rfile = read_file_rating()
ufile = read_file_users()
############################################################################### 
###################### INSERT BLOCK ###########################################
insert_into_AGE(cur)
insert_into_OCCUPATION(cur)
##################### END INSERT BLOCK ########################################
############################################################################### 
process_movies(mfile)
process_rating(rfile) 
process_users(ufile) 

con.commit() # COMMIT ALL TO DB. add output for more tables

QUESTION_1(cur) # Query 1
QUESTION_2(cur) # Query 2

con.commit() 
con.close() # close db connection. 
############################################################################### 
############################# END MAIN BLOCK ##################################
############################################################################### 



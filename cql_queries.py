# CQL ANALYTICS QUERIES

query1 = """
        SELECT artist, length, song
        FROM music_library_by_sessions
        WHERE sessionid = 338 AND iteminsession = 4
    """

query2 = """
        SELECT artist, song, firstname, lastname
        FROM music_library_by_users_and_sessions
        WHERE userid = 10 AND sessionid = 182
    """

query3 = """
        SELECT firstname, lastname
        FROM music_library_by_songs
        WHERE song = 'All Hands Against His Own'
    """

# CREATE TABLES

create_table1 = """CREATE TABLE IF NOT EXISTS music_library_by_sessions (
        artist text, 
        iteminsession int, 
        length double, 
        sessionid int, 
        song text,
        
        PRIMARY KEY (sessionid, iteminsession)
        )"""

create_table2 = """CREATE TABLE IF NOT EXISTS music_library_by_users_and_sessions (
        artist text,
        iteminsession int,
        song text,
        firstname text,
        lastname text,
        sessionid int,
        userid int, 
        
        PRIMARY KEY ((userid, sessionid), iteminsession) 
        )"""

create_table3 = """CREATE TABLE IF NOT EXISTS music_library_by_songs (
        firstname text, 
        lastname text,  
        song text, 
        userid int,
        
        PRIMARY KEY (song, userid)
        )"""



# DROP TABLES 

drop_table1 = "DROP TABLE IF EXISTS music_library_by_sessions"

drop_table2 = "DROP TABLE IF EXISTS music_library_by_users_and_sessions"

drop_table3 = "DROP TABLE IF EXISTS music_library_by_songs"


# CREATE KEYSPACE

CREATE_KEYSPACE_QUERY = """
        CREATE KEYSPACE IF NOT EXISTS sparkify 
        WITH REPLICATION = {
            'class':'SimpleStrategy', 
            'replication_factor': 1
            }
        """


# QUERIES LISTS

CREATE_TABLES_QUERIES = [create_table1, create_table2, create_table3]

DROP_TABLES_QUERIES = [drop_table1, drop_table2, drop_table3]

SELECT_QUERIES = [query1, query2, query3]
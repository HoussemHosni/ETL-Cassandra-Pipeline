# ETL Pipeline For a Music Streaming App - Python & Cassandra
## Context
The analyticts Team of a music streaming Startup wants to analyze the data they have about songs and user activity on their music streaming app. The team is particularly interested in understanding what songs users are listening to. However, there is no straightforward way to do so with the songs Metadata and user activity logs residing in CSV files.

## :dart: Objective
The goal is to create a Cassandra database schema and Tables for a particular analytic focus and query in mind and build an ETL pipeline to automate extracting, transforming and loading the CSV data residing in directories into the database.

## :checkered_flag: Analytics Goals
>Analytics Team expects to write queries against the database to gain insights out of the behavior of its users and trends on songs in order to better serve more suitable and precise songs/products recommendations. Some of the insights that the analytics team may be interested in :
>
>> - Getting songs details such as name, artist and length and User's details during a specific session
>> - Returning all the users (name and last name) who have listened to a specific song

## :heavy_check_mark: Data Modeling
Each Table in The Cassandra database is designed based on a specific Query To optimize performance and maximize speed. 

### Query : SELECT artist, length, song FROM music_library WHERE sessionid = 338 AND iteminsession = 4
#### Corresponding Table : music_library_by_sessions
- `artist, iteminsession, length, sessionid, song`  
- `(sessionid, iteminsession) as a Composite Primary Key`  

### Query : SELECT artist, song, firstname, lastname FROM music_library WHERE userid = 10 AND sessionid = 182
#### Corresponding Table : music_library_by_users_and_sessions
- `artist, iteminsession, song, firstname, lastname, sessionid, userid`  
- `(userid, sessionid) as a Composite Partition Key and iteminsession as a Clustering Column`  

### Query : SELECT firstname, lastname FROM music_library WHERE song = 'All Hands Against His Own'
#### Corresponding Table : music_library_by_songs
- `firstname, lastname, song, userid`  
- `(song, userid) Composite Partition Key to make Primary Keys Unique`  

## ETL Pipeline

The ETL pipeline works in 3 steps:

1. Extract - The file structure is walked, and relevant CSV files are parsed and read 
2. Transform - Data is extracted from the CSV files and formatted as required by the Table Schema
3. Load - Data is inserted into the Corresponding tables

To do so, we manage to:  

- Process Event Data Directory
    - Parsing all CSV files and pulling data in one single CSV File  
- Loading data into each Table with corresponding columns data from each row.

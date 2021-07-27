import csv
import glob
import os
from cassandra.cluster import Cluster
from cql_queries import SELECT_QUERIES
from create_tables import get_session


def get_all_files(filepath):
    all_files = []
    for root, files, dirs in os.walk(filepath):

        files = glob.glob(os.path.join(root, '*events.csv'))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    return all_files

def preprocess_csv_files(files):

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        
        for csv_file in files:
            with open(csv_file, 'r', encoding = 'utf8', newline='') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    if (row[0] == ''):
                        continue
                    writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

def insert_into_table_1(session, file):

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = """
            INSERT INTO music_library_by_sessions (
                    artist, 
                    iteminsession, 
                    length, 
                    sessionid, 
                    song
                    ) 
            """
            query = query + "VALUES (%s,%s,%s,%s,%s)"
            session.execute(query, (line[0], int(line[3]), float(line[5]), int(line[8]), line[9] ) ) 

def insert_into_table_2(session, file):

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = """
            INSERT INTO music_library_by_users_and_sessions (
                    artist,
                    iteminsession,
                    song,
                    firstname, 
                    lastname,  
                    sessionid,  
                    userId) 
            """
            query = query + "VALUES (%s,%s,%s,%s,%s,%s,%s)"
            session.execute(query, (line[0], int(line[3]), line[9], line[1], line[4], int(line[8]), int(line[10])))

def insert_into_table_3(session, file):

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = """
            INSERT INTO music_library_by_songs ( 
                    firstname, 
                    lastname, 
                    song, 
                    userId) 
            """
            query = query + "VALUES (%s,%s,%s,%s)"
            session.execute(query, (line[1], line[4], line[9], int(line[10])))

def run_select_query(session, query):
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    return rows

def main():

    filepath = os.getcwd() + '/event_data'
    all_files = get_all_files(filepath)
    preprocess_csv_files(all_files)

    try:
        cluster = Cluster(['127.0.0.1'])
    except Exception as e:
        print(e)

    session = get_session(cluster)
    session.set_keyspace('sparkify')

    file = 'event_datafile_new.csv'
    insert_into_table_1(session, file)
    insert_into_table_2(session, file)
    insert_into_table_3(session, file)

    # Run SELECT Queries
    for query in SELECT_QUERIES:
        rows = run_select_query(session, query)
        for row in rows:
            print(row)
        print("############################################################")

    session.shutdown()
    cluster.shutdown()


if __name__ == '__main__':
    main()






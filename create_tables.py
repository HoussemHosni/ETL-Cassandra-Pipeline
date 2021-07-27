import cassandra
from cql_queries import DROP_TABLES_QUERIES, CREATE_TABLES_QUERIES, CREATE_KEYSPACE_QUERY
from cassandra.cluster import Cluster

def get_session(cluster):

    try:
        session = cluster.connect()
    except Exception as e:
        print(e)

    return session

def drop_tables(session):
    
    for query in DROP_TABLES_QUERIES:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):

    for query in CREATE_TABLES_QUERIES:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

def main():
    
    try:
        cluster = Cluster(['127.0.0.1'])
    except Exception as e:
        print(e)

    session = get_session(cluster)

    try:
        session.execute(CREATE_KEYSPACE_QUERY)
    except Exception as e:
        print(e)
    
    try:
        session.set_keyspace('sparkify')
    except Exception as e:
        print(e)

    drop_tables(session)

    create_tables(session)
    
    session.shutdown()
    cluster.shutdown()



if __name__ == '__main__':
    main()



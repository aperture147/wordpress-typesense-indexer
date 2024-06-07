import pymysql
import typesense

def get_mysql_connection(
    host: str, port: int, user: str, password: str, db_name: str, autocommit=False
):
    db_conn = pymysql.connect(
        host=host, port=port,
        user=user, passwd=password, 
        db=db_name,
        connect_timeout=3600,
        autocommit=autocommit
    )
    db_conn.ping()
    print(f'db {host} connected')
    return db_conn

def get_typesense_client(
    host: str, port: str, protocol: str, api_key: str
):
    typesense_client = typesense.Client({
        'nodes': [{
            'host': host,  # For Typesense Cloud use xxx.a1.typesense.net
            'port': port,       # For Typesense Cloud use 443
            'protocol': protocol   # For Typesense Cloud use https
        }],
        'api_key': api_key,
        'connection_timeout_seconds': 3600
    })
    print(f'typesense client {host} created')
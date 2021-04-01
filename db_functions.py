from sqlalchemy import create_engine

def connect_to_oracle_db(username, password, host, port, service):

    return create_engine(f'oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service}')


import sqlalchemy

from sqlalchemy.engine import Engine
from utils import YAMLReader

class AWSDBConnector:
    '''An object which connects to a remote AWS database.'''
    def __init__(self):
        pass
    
    def create_db_connector(self, credentials:str='db_creds')->Engine:
        '''
        Creates connection engine with given database credentials.  
        By default, it looks for a `.yaml` file named `db_creds`.
        '''
        if '.yaml' in credentials:
            credentials = credentials.split('.')[0]

        creds = YAMLReader(credentials)
        HOST = creds['HOST']
        USER = creds['USER']
        PASSWORD = creds['PASSWORD']
        DATABASE = creds['DATABASE']
        PORT = 3306

        engine = sqlalchemy.create_engine(
            f'mysql+pymysql'
            f'://{USER}'
            f':{PASSWORD}'
            f'@{HOST}'
            f':{PORT}'
            f'/{DATABASE}'
            '?charset=utf8mb4'
        )
        return engine
import pandas as pd
import datetime
from pymongo import MongoClient

def _connect_mongo(host, 
                    port, 
                    username, 
                    password, 
                    db):

    """ Criando a conexão no mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)
    return conn[db]

def read_mongo(collection, db, host, port, username=None, password=None, query={}, no_id=True):
    """ Ler do mongo e salvar no dataframe  """
    
    # conectar ao banco ---- OK
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Fazer a query especifica para o banco ----- OK
    cursor = db[collection]

    # Abrir os dados para o dataframe do pandas ---- OK
    df = pd.DataFrame(cursor.find(query))

    # Para ver os documentos
    """ for x in cursor.find(query):
        print(x) 
    """

    # Deletar coluna _id ---- ok
    if no_id:
        del df['_id']

    return df


# datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None, *, fold=0)¶

queryBuild = {"orderDate": {"$gte": datetime.datetime(2022, 1, 3, 0, 0, 0),
                        "$lte": datetime.datetime(2022, 1, 3, 23, 59, 59)}}

mongoDf = read_mongo(collection='transactionTest',
                     db='testeDB',
                     host="localhost",
                     port=27017,
                     query=queryBuild,
                     no_id=True)

mongoDf

""" 
    Proximas Etapas

    *inserir o dataframe em um banco como um insert

    Para automação

    --recolher o maior valor de data e hora do banco
    --converter em data
    --verificar a diferença de dias entre hoje e esta data
    --loopar o request atraves dessa data pegando dia a dia os dados para não sobrecarregar o banco 

"""

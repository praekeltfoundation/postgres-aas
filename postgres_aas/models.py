from twistar.dbobject import DBObject

class Host(DBObject):
    pass


class PostgresSQLHost(DBObject):
    HASMANY = [{'name': 'postgres_sql_server',
                'class_name': 'PostgresSQLCluster',
                'foreign_key': ['ip']}]


class PostgresSQLCluster(DBObject):
    HASMANY = [{'name': 'postgres_sql_instance',
                'class_name': 'PostgresSQLInstance',
                'foreign_key': ['ip', 'port']}]
    BELONGSTO = ['PostgresSQLHost']


class PostgresSQLInstance(DBObject):
    HASMANY = [{'name': 'postgres_sql_binding',
                'class_name': 'PostgresSQLBinding',
                'foreign_key': ['instance_id']}]
    BELONGSTO = ['PostgresSQLCluster']


class PostgresSQLBinding(DBObject):
    BELONGSTO = ['PostgresSQLInstance']
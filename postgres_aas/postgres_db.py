# this is the internal DAL for the internal postgres database
from twisted.enterprise import adbapi
from twistar.registry import Registry
from postgres_service.models import PostgresSQLInstance, PostgresSQLHost, PostgresSQLBinding, PostgresSQLCluster
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
import psycopg2
from psycopg2.extras import DictCursor


def _set_transaction_level(conn):
    conn.autocommit = True
    
def _done_query(result):
    print('Query completed.')
    return result

def _done_operation(result):
    print('Successfully executed command with result ' + str(result))


def _operation_failed(result):
    raise RuntimeError('Error occurred during operation: ' + str(result))


def _return_object(objects):
    return objects[0]

@inlineCallbacks
def _delete_object(object):
    yield object.delete().addCallbacks(_done_query, _operation_failed)
    
def ignore_pg_error(d, pgcode):
    '''
    Ignore a particular postgres error.
    '''

    def trap_err(f):
        f.trap(psycopg2.ProgrammingError)
        if f.value.pgcode != pgcode:
            return f

    return d.addErrback(trap_err)


class PSQLServiceDB(object):
    def __init__(self,
                 db_name,
                 db_host,
                 db_port,
                 db_user,
                 db_pass):
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass

    
    def add_psql_host(self,
                      hostname,
                      ip):
        host = PostgresSQLHost()
        host.host = hostname
        host.ip = ip
        return host.save().addCallbacks(_done_query, _operation_failed)
    
    def add_psql_cluster(self,
                        ip,
                        port):
        cluster = PostgresSQLCluster()
        cluster.ip = ip
        cluster.port = port
        return cluster.save().addCallbacks(_done_query, _operation_failed)
    
    def add_psql_instance(self,
                          ip,
                          port,
                          db_name,
                          instance_id):
        instance = PostgresSQLInstance()
        instance.db_name = db_name
        instance.ip = ip
        instance.port = port
        instance.instance_id = instance_id
        return instance.save().addCallbacks(_done_query, _operation_failed)
    
    def add_binding(self,
                    instance_id,
                    binding_id):
        binding = PostgresSQLBinding()
        binding.instance_id = instance_id
        binding.binding_id = binding_id
        return binding.save().addCallbacks(_done_query, _operation_failed)

    def remove_psql_host(self, ip):
        return PostgresSQLHost.find(
            where=['ip = ?', ip],
            limit=1).addCallbacks(_delete_object, _operation_failed)

    def remove_psql_cluster(self, ip, port):
        return PostgresSQLCluster.find(
            where=['ip = ? and port = ?', ip, port],
            limit=1).addCallbacks(_delete_object, _operation_failed)

    def remove_psql_instance(self, instance_id):
        return PostgresSQLInstance.find(
            where=['instance_id = ?', instance_id],
            limit=1).addCallbacks(_delete_object, _operation_failed)

    def remove_binding(self, binding_id):
        return PostgresSQLBinding.find(
            where=['binding_id = ?', binding_id],
            limit=1).addCallbacks(_delete_object, _operation_failed)

    def get_hosts(self):
        return PostgresSQLHost.all()

    def get_clusters(self):
        return PostgresSQLCluster.all()

    def get_instances(self):
        return PostgresSQLInstance.all()

    def get_bindings(self):
        return PostgresSQLBinding.all()

    def get_host(self, ip):
        return PostgresSQLHost.find(
                where=['ip = ?', ip],
                limit=1).addCallbacks(_done_query, _operation_failed)
    
    def get_cluster(self, ip, port):
        return PostgresSQLCluster.find(
                where=['ip = ? AND port = ?', ip, port],
                limit=1).addCallback(_done_query)
        
    def get_instance(self, instance_id):
        return PostgresSQLInstance.find(
                where=['instance_id = ?', instance_id],
                limit=1).addCallbacks(_done_query, _operation_failed)

    def get_binding(self, binding_id):
        binding = PostgresSQLBinding.find(
                where=['binding_id = ?', binding_id],
                limit=1).addCallbacks(_done_query, _operation_failed)
        return binding

    @inlineCallbacks
    def host_exists(self, ip):
        instance = yield self.get_host(ip)
        returnValue(instance)
        
    @inlineCallbacks
    def cluster_exists(self, ip, port):
        cluster = yield self.get_cluster(ip, port)
        returnValue(cluster)
        
    @inlineCallbacks
    def instance_exists(self, instance_id):
        instance = yield self.get_instance(instance_id)
        returnValue(instance)

    @inlineCallbacks
    def binding_exists(self, binding_id):
        binding = yield self.get_binding(binding_id)
        returnValue(binding)

    def _get_connection(self):
        Registry.DBPOOL = adbapi.ConnectionPool(
            'psycopg2',
            database=self.db_name,
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pass,
            cp_min=1,
            cp_max=2,
            cursor_factory=DictCursor,
            cp_openfun=_set_transaction_level)

    # Database needs to already be populated with lists of hosts and databases
    @inlineCallbacks
    def setup_db(self):
        self._get_connection()
        hosts = ('CREATE TABLE postgres_sql_hosts ('
                 'id SERIAL, '
                 'ip INET PRIMARY KEY, '
                 'host VARCHAR(256));')
        pg_clusters = ('CREATE TABLE postgres_sql_clusters ('
                      'id SERIAL, '
                      'ip INET REFERENCES postgres_sql_hosts(ip) '
                      'ON DELETE CASCADE ON UPDATE CASCADE, '
                      'port INT, '
                      'PRIMARY KEY (ip, port)'
                      ');')
        pg_dbs = ('CREATE TABLE postgres_sql_instances ('
                  'id SERIAL, '
                  'ip INET, '
                  'port INT, '
                  'db_name VARCHAR(256), '
                  'instance_id VARCHAR(256) UNIQUE, '
                  'FOREIGN KEY (ip, port) REFERENCES postgres_sql_clusters(ip, port) '
                  'ON DELETE CASCADE ON UPDATE CASCADE '
                  ');')
        pg_bindings = ('CREATE TABLE postgres_sql_bindings ('
                       'id SERIAL, '
                       'binding_id VARCHAR(256) UNIQUE, '
                       'instance_id VARCHAR(256) REFERENCES postgres_sql_instances(instance_id) '
                       'ON DELETE CASCADE ON UPDATE CASCADE);')

        yield Registry.DBPOOL.runOperation(hosts).addCallbacks(_done_operation, _operation_failed)
        yield Registry.DBPOOL.runOperation(pg_clusters).addCallbacks(_done_operation, _operation_failed)
        yield Registry.DBPOOL.runOperation(pg_dbs).addCallbacks(_done_operation, _operation_failed)
        yield Registry.DBPOOL.runOperation(pg_bindings).addCallbacks(_done_operation, _operation_failed)

        Registry.register(PostgresSQLHost, PostgresSQLCluster)
        Registry.register(PostgresSQLCluster, PostgresSQLInstance)
        Registry.register(PostgresSQLInstance, PostgresSQLBinding)
    
    def close_connection(self):
        if Registry.DBPOOL:
            Registry.DBPOOL.close()




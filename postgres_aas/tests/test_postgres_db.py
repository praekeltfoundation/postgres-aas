import psycopg2
import testing.postgresql
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from postgres_aas.postgres_db import PSQLServiceDB


class TestPostgresDB(unittest.TestCase):
    def setUp(self):
        global db, db_con, db_conf
        db = testing.postgresql.Postgresql()
        db_conf = db.dsn()
        db_con = psycopg2.connect(**db_conf)
        self.database = PSQLServiceDB(db_conf['database'],
                                      db_conf['host'],
                                      db_conf['port'],
                                      db_conf['user'],
                                      '')
        if self._testMethodName != 'test_setup':
            self.database._get_connection()
            self.setup_database()

    def setup_database(self):
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
        
        db_con.autocommit = True
        with db_con.cursor() as cur:
            cur.execute(hosts)
            cur.execute(pg_clusters)
            cur.execute(pg_dbs)
            cur.execute(pg_bindings)
    
    def make_host(self, ip, host):
        with db_con as conn:
            cur = conn.cursor()
            cur.execute('INSERT INTO postgres_sql_hosts '
                        'VALUES (DEFAULT, %s, %s)', (ip, host))
            
    def make_cluster(self, ip, host, port):
        self.make_host(ip, host)
        with db_con.cursor() as cur:
            cur.execute('INSERT INTO postgres_sql_clusters '
                        'VALUES (DEFAULT, %s, %s)', (ip, port))
            
    def add_cluster(self, ip, port):
        with db_con.cursor() as cur:
            cur.execute('INSERT INTO postgres_sql_clusters '
                        'VALUES (DEFAULT, %s, %s)', (ip, port))

    def make_instance(self, ip, host, port, db_name, instance_id):
        self.make_cluster(ip, host, port)
        with db_con.cursor() as cur:
            cur.execute('INSERT INTO postgres_sql_instances '
                        'VALUES (DEFAULT, %s, %s, %s, %s)', (ip, port, db_name, instance_id))
            
    def add_instance(self, ip, port, db_name, instance_id):
        with db_con.cursor() as cur:
            cur.execute('INSERT INTO postgres_sql_instances '
                        'VALUES (DEFAULT, %s, %s, %s, %s)', (ip, port, db_name, instance_id))

    def make_binding(self, ip, host, port, db_name, instance_id, binding_id):
        self.make_instance(ip, host, port, db_name, instance_id)
        with db_con.cursor() as cur:
            cur.execute('INSERT INTO postgres_sql_bindings '
                        'VALUES (DEFAULT, %s, %s)', (binding_id, instance_id))
            
    def add_binding(self, instance_id, binding_id):
        with db_con.cursor() as cur:
            cur.execute('INSERT INTO postgres_sql_bindings '
                        'VALUES (DEFAULT, %s, %s)', (binding_id, instance_id))

    @inlineCallbacks
    def test_setup(self):
        yield self.database.setup_db()
        with db_con.cursor() as cur:
            cur.execute('select * from information_schema.tables where table_name=%s', ('postgres_sql_instances',))
            rows = cur.fetchall()
            self.assertEquals(len(rows), 1)

    @inlineCallbacks
    def test_add_psql_cluster(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        self.make_host(ip, host)
        yield self.database.add_psql_cluster(ip, port)
        with db_con.cursor() as cur:
            cur.execute('select * from postgres_sql_clusters '
                        'where ip=%s and port=%s', (ip, port))
            rows = cur.fetchall()
            self.assertEquals(len(rows), 1)

    @inlineCallbacks
    def test_add_psql_instance(self):
        ip = '127.0.0.1'
        port = '5432'
        host = 'hostname'
        db_name = 'testdb'
        instance_id = 'instance1'
        self.make_cluster(ip, host, port)
        yield self.database.add_psql_instance(ip, port, db_name, instance_id)
        with db_con.cursor() as cur:
            cur.execute('select * from postgres_sql_instances '
                        'where instance_id=%s', (instance_id,))
            rows = cur.fetchall()
            self.assertEquals(len(rows), 1)

    @inlineCallbacks
    def test_add_psql_binding(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_instance(ip, host, port, db_name, instance_id)
        yield self.database.add_binding(instance_id, binding_id)
        with db_con.cursor() as cur:
            cur.execute('select * from postgres_sql_bindings '
                        'where binding_id=%s', (binding_id,))
            rows = cur.fetchall()
            self.assertEquals(len(rows), 1)

    @inlineCallbacks
    def test_remove_psql_host(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_binding(ip, host, port, db_name, instance_id, binding_id)
        ip = '127.0.0.1'
        yield self.database.remove_psql_host(ip)
        with db_con.cursor() as cur:
            cur.execute('select * from postgres_sql_hosts '
                        'where ip=%s', (ip,))
            rows = cur.fetchall()
            self.assertEquals(len(rows), 0)
            
    @inlineCallbacks
    def test_remove_psql_cluster(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_binding(ip, host, port, db_name, instance_id, binding_id)
        ip = '127.0.0.1'
        port = 5432
        yield self.database.remove_psql_cluster(ip, port)
        with db_con.cursor() as cur:
            cur.execute('select * from postgres_sql_clusters '
                        'where ip=%s and port=%s', (ip, port))
            rows = cur.fetchall()
            self.assertEquals(len(rows), 0)

    @inlineCallbacks
    def test_remove_psql_instance(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_binding(ip, host, port, db_name, instance_id, binding_id)
        yield self.database.remove_psql_instance(instance_id)
        with db_con.cursor() as cur:
            cur.execute('select * from postgres_sql_instances '
                        'where instance_id=%s', (instance_id,))
            rows = cur.fetchall()
            self.assertEquals(len(rows), 0)
            
    @inlineCallbacks
    def test_remove_binding(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_binding(ip, host, port, db_name, instance_id, binding_id)
        binding_id = 'purple-rain2342285'
        yield self.database.remove_binding(binding_id)
        with db_con.cursor() as cur:
            cur.execute('select * from postgres_sql_bindings '
                        'where binding_id=%s', (binding_id,))
            rows = cur.fetchall()
            self.assertEquals(len(rows), 0)

    def test_host_exists(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_binding(ip, host, port, db_name, instance_id, binding_id)
        ip = '127.0.0.1'
        result = self.database.host_exists(ip)
        self.assertTrue(result)

    def test_cluster_exists(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_binding(ip, host, port, db_name, instance_id, binding_id)
        result = self.database.cluster_exists(ip, port)
        self.assertTrue(result)
        
    def test_instance_exists(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_binding(ip, host, port, db_name, instance_id, binding_id)
        result = self.database.instance_exists(instance_id)
        self.assertTrue(result)
        
    def test_binding_exists(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id = 'purple-rain2342285'
        self.make_binding(ip, host, port, db_name, instance_id, binding_id)
        binding_id = 'purple-rain2342285'
        result = self.database.binding_exists(binding_id)
        self.assertTrue(result)
        
        
    @inlineCallbacks
    def test_get_hosts(self):
        host1 = 'hostname1'
        host2 = 'hostname2'
        ip1 = '127.0.0.1'
        ip2 = '127.0.0.2'
        self.make_host(ip1, host1)
        self.make_host(ip2, host2)
        result = yield self.database.get_hosts()
        self.assertEquals(len(result), 2)
        
    @inlineCallbacks
    def test_get_clusters(self):
        host = 'hostname1'
        ip = '127.0.0.1'
        port1 = 5432
        port2 = 5433
        self.make_host(ip, host)
        self.add_cluster(ip, port1)
        self.add_cluster(ip, port2)
        result = yield self.database.get_clusters()
        self.assertEquals(len(result), 2)
        
    @inlineCallbacks
    def test_get_instances(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name1 = 'testdb'
        instance_id1 = 'instance'
        db_name2 = 'testdb1'
        instance_id2 = 'instance1'
        self.make_cluster(ip, host, port)
        self.add_instance(ip, port, db_name1, instance_id1)
        self.add_instance(ip, port, db_name2, instance_id2)
        result = yield self.database.get_instances()
        self.assertEquals(len(result), 2)
        
    @inlineCallbacks
    def test_get_bindings(self):
        ip = '127.0.0.1'
        host = 'hostname'
        port = 5432
        db_name = 'testdb'
        instance_id = 'instance1'
        binding_id1 = 'binding1'
        binding_id2 = 'binding2'
        self.make_instance(ip, host, port, db_name, instance_id)
        self.add_binding(instance_id, binding_id1)
        self.add_binding(instance_id, binding_id2)
        result = yield self.database.get_bindings()
        self.assertEquals(len(result), 2)
        
    def tearDown(self):
        self.database.close_connection()
        db_con.close()
        db.stop()

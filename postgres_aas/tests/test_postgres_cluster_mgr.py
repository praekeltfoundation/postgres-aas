import psycopg2
from psycopg2 import OperationalError
import testing.postgresql
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from postgres_aas.postgres_cluster_mgr import PostgreSQLClusterMgr
from contextlib import closing


class TestPostgresClusterMgr(unittest.TestCase):
    def setUp(self):
        global db, db_con, db_conf
        db = testing.postgresql.Postgresql()
        db_conf = db.dsn()
        db_con = psycopg2.connect(**db_conf)
        self.database = PostgreSQLClusterMgr(db_conf['database'],
                                             db_conf['host'],
                                             db_conf['port'],
                                             db_conf['user'],
                                             '')

    def get_connection(self,
                       dbname,
                       host,
                       port,
                       user,
                       password):
        try:
            conn = psycopg2.connect(dbname=dbname,
                                    host=host,
                                    port=port,
                                    user=user,
                                    password=password)
            return conn
        except OperationalError as e:
            raise e

    def can_connect(self,
                    dbname,
                    host,
                    port,
                    user,
                    password):
        conn = None
        try:
            conn = self.get_connection(dbname=dbname,
                                       host=host,
                                       port=port,
                                       user=user,
                                       password=password)
            return True
        except OperationalError as e:
            return False
        finally:
            if conn:
                conn.close()


    def check_has_role(self, conn, user, role):
        check_role = ('WITH RECURSIVE cte AS ('
                      'SELECT oid FROM pg_roles WHERE rolname = \'{0}\' '
                      'UNION ALL SELECT m.roleid '
                      'FROM   cte '
                      'JOIN   pg_auth_members m ON m.member = cte.oid) '
                      'SELECT pg_roles.rolname '
                      'FROM cte, pg_roles '
                      'WHERE cte.oid = pg_roles.oid '
                      'AND rolname = \'{1}\';').format(user, role)
        try:
            with conn.cursor() as cur:
                cur.execute(check_role)
                rows = cur.fetchall()
                return rows
        except OperationalError as e:
            return False
        finally:
            if conn:
                conn.close()

    @inlineCallbacks
    def test_create_db(self):
        dbname = 'test1'
        user = 'postgres'
        yield self.database._create_database(dbname)
        result = self.can_connect(dbname=dbname,
                                  host=self.database.db_host,
                                  port=self.database.db_port,
                                  user=user,
                                  password=None)
        self.assertTrue(result)

    @inlineCallbacks
    def test_add_owner_role(self):
        db_name = self.database.db_name
        db_owner = 'owner'
        db_pass = 'password'
        conn = self.database._connect_to_base_db()
        yield self.database._add_owner_role(conn, db_name, db_owner, db_pass)
        result = self.can_connect(dbname=db_name,
                                  host=self.database.db_host,
                                  port=self.database.db_port,
                                  user=db_owner,
                                  password=db_pass)
        self.assertTrue(result)

    @inlineCallbacks
    def test_add_vault_user(self):
        db_name = self.database.db_name
        vault_user = 'vaultuser'
        vault_pass = 'testpassword'
        conn = self.database._connect_to_base_db()
        yield self.database._add_vault_user(conn, db_name, vault_user, vault_pass)
        result = self.can_connect(dbname=db_name,
                                  host=self.database.db_host,
                                  port=self.database.db_port,
                                  user=vault_user,
                                  password=vault_pass)
        self.assertTrue(result)

    @inlineCallbacks
    def test_grant_owner_to_vuser(self):
        dbname = self.database.db_name
        vault_user = 'vaultuser'
        vault_pass = 'testpassword'
        db_owner = 'owner'
        db_pass = 'password'
        with closing(self.database._connect_to_base_db()) as conn:
            yield self.database._add_vault_user(conn, dbname, vault_user, vault_pass)
            yield self.database._add_owner_role(conn, dbname, db_owner, db_pass)
            yield self.database._grant_owner_to_vuser(conn, db_owner, vault_user)
        conn = self.get_connection(dbname,
                                   self.database.db_host,
                                   self.database.db_port,
                                   self.database.db_user,
                                   self.database.db_pass
                                   )
        result = self.check_has_role(conn, vault_user, db_owner)
        self.assertTrue(result)

    @inlineCallbacks
    def test_provision(self):
        dbname = 'test2'
        vault_user = 'vaultuser'
        vault_pass = 'testpassword'
        db_owner = 'owner'
        db_pass = 'password'
        postgres_pass = 'postgrespassword'
        yield self.database.provision(dbname, vault_user, vault_pass, db_owner, db_pass, postgres_pass)
        result = self.can_connect(dbname=dbname,
                         host=self.database.db_host,
                         port=self.database.db_port,
                         user=db_owner,
                         password=db_pass)
        self.assertTrue(result)
        result = self.can_connect(dbname=dbname,
                         host=self.database.db_host,
                         port=self.database.db_port,
                         user=vault_user,
                         password=vault_pass)
        self.assertTrue(result)

    @inlineCallbacks
    def test_deprovision(self):
        dbname = 'test2'
        vault_user = 'vaultuser'
        vault_pass = 'testpassword'
        db_owner = 'owner'
        db_pass = 'password'
        postgres_pass = 'postgrespassword'
        yield self.database.provision(dbname, vault_user, vault_pass, db_owner, db_pass, postgres_pass)
        result = self.can_connect(dbname=dbname,
                         host=self.database.db_host,
                         port=self.database.db_port,
                         user=db_owner,
                         password=db_pass)
        self.assertTrue(result)
        yield self.database.deprovision(dbname, db_owner, vault_user)
        result = self.can_connect(dbname,
                         host=self.database.db_host,
                         port=self.database.db_port,
                         user='postgres',
                         password=postgres_pass)
        self.assertFalse(result)
        result = self.can_connect(dbname=self.database.db_name,
                         host=self.database.db_host,
                         port=self.database.db_port,
                         user=vault_user,
                         password=vault_pass)
        self.assertFalse(result)
        
    def tearDown(self):
        db_con.close()
        db.stop()

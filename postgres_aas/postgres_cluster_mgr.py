# Manager of Postgres resources. Connects to a cluster and
# provisions/deprovisions databases
from psycopg2 import OperationalError
from twisted.enterprise import adbapi
from psycopg2.extras import DictCursor
from contextlib import closing
from twisted.internet.defer import inlineCallbacks


def _set_transaction_level(conn):
    conn.autocommit = True


def _done_query(result):
    print('Query completed.')
    return result


def _done_operation(result):
    print('Successfully executed command with result ' + str(result))


def _operation_failed(result):
    raise OperationalError('Error occurred during operation: ' + str(result))


class PostgreSQLClusterMgr(object):
    def __init__(self,
                 db_name,
                 db_host,
                 db_port,
                 db_user,
                 db_pass
                 ):
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass

    @inlineCallbacks
    def provision(self,
                  db_name,
                  vault_user,
                  vault_pass,
                  db_owner,
                  db_pass,
                  postgres_pass):
        yield self._create_database(db_name)
        with closing(self._connect_to_new_db(db_name)) as conn:
            yield self._revoke_connect_on_database(conn, db_name)
            yield self._change_postgres_password(conn, postgres_pass)
            yield self._add_owner_role(conn, db_name, db_owner, db_pass)
            yield self._add_vault_user(conn, db_name, vault_user, vault_pass)
            yield self._grant_owner_to_vuser(conn, db_owner, vault_user)

    @inlineCallbacks
    def deprovision(self, db_name, owner, vault_user):
        yield self._drop_role(owner)
        yield self._drop_role(vault_user)
        yield self._drop_database(db_name)

    @inlineCallbacks
    def _create_database(self, db_name):
        create_db = 'CREATE DATABASE {0} WITH OWNER DEFAULT;'.format(db_name)
        with closing(self._connect_to_base_db()) as conn:
            yield conn.runOperation(create_db).addCallbacks(_done_operation, _operation_failed)

    @inlineCallbacks
    def _drop_database(self, db_name):
        drop_db = ('SELECT pg_terminate_backend(pg_stat_activity.pid)'
                   'FROM pg_stat_activity WHERE pg_stat_activity.datname = \'{0}\''
                   'AND pid <> pg_backend_pid();').format(db_name)
        delete_db = 'DROP DATABASE {0};'.format(db_name)
        with closing(self._connect_to_base_db()) as conn:
            yield conn.runOperation(drop_db).addCallbacks(_done_operation, _operation_failed)
            yield conn.runOperation(delete_db).addCallbacks(_done_operation, _operation_failed)

    def _revoke_connect_on_database(self, conn, db_name):
        revoke_cod = 'REVOKE connect ON DATABASE {0} FROM PUBLIC;'.format(db_name)
        return conn.runOperation(revoke_cod).addCallbacks(_done_operation, _operation_failed)

    def _change_postgres_password(self, conn, postgres_pass):
        change_postgres_password = 'ALTER USER postgres WITH ENCRYPTED PASSWORD \'{0}\''.format(postgres_pass)
        return conn.runOperation(change_postgres_password).addCallbacks(_done_operation, _operation_failed)

    def _add_vault_user(self,
                        conn,
                        db_name,
                        vault_user,
                        vault_password):
        add_vuser = ('CREATE ROLE {0} WITH CREATEROLE CREATEDB INHERIT LOGIN ENCRYPTED PASSWORD \'{1}\';'
                     'GRANT ALL PRIVILEGES ON DATABASE {2} TO {0} WITH GRANT OPTION;'.format(vault_user,
                                                                                             vault_password,
                                                                                             db_name))
        return conn.runOperation(add_vuser).addCallbacks(_done_operation, _operation_failed)

    def _add_owner_role(self,
                        conn,
                        db_name,
                        db_owner,
                        db_pass):
        add_owner = ('CREATE ROLE {1} WITH LOGIN ENCRYPTED PASSWORD \'{2}\';'
                     'ALTER DATABASE {0} OWNER TO {1};'
                     'ALTER SCHEMA public OWNER TO {1};'.format(db_name, db_owner, db_pass))
        return conn.runOperation(add_owner).addCallbacks(_done_operation, _operation_failed)

    def _grant_owner_to_vuser(self,
                              conn,
                              db_owner,
                              vault_user):
        grant_owner = ('GRANT {0} TO {1};'.format(db_owner, vault_user))
        return conn.runOperation(grant_owner).addCallbacks(_done_operation, _operation_failed)

    def _drop_role(self, role):
        drop_role = ('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public from {0};'
                     'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public from {0};'
                     'REVOKE ALL PRIVILEGES ON SCHEMA public from {0};').format(role)
        with closing(self._connect_to_base_db()) as conn:
            yield conn.runOperation(drop_role).addCallbacks(_done_operation, _operation_failed)

    def _remove_owner_role(self,
                           conn,
                           db_name,
                           db_owner,
                           db_pass):
        add_owner = ('CREATE ROLE {1} WITH LOGIN ENCRYPTED PASSWORD \'{2}\';'
                     'ALTER DATABASE {0} OWNER TO {1};'
                     'ALTER SCHEMA public OWNER TO {1};'.format(db_name, db_owner, db_pass))
        return conn.runOperation(add_owner).addCallbacks(_done_operation, _operation_failed)

    def _connect_to_base_db(self):
        return self._get_connection_pool(host=self.db_host,
                                         port=self.db_port,
                                         user=self.db_user,
                                         password=self.db_pass,
                                         db=self.db_name)

    def _connect_to_new_db(self, db_name, username=None, password=None):
        user = username if username else self.db_user
        passwd = password if password else self.db_pass
        return self._get_connection_pool(host=self.db_host,
                                         port=self.db_port,
                                         user=user,
                                         password=passwd,
                                         db=db_name)

    def _get_connection_pool(self, host=None, port=None, user=None, password=None, db=None):
        return adbapi.ConnectionPool('psycopg2',
                                     database=db,
                                     host=host,
                                     port=port,
                                     user=user,
                                     password=password,
                                     cp_min=1,
                                     cp_max=2,
                                     cursor_factory=DictCursor,
                                     cp_openfun=_set_transaction_level)

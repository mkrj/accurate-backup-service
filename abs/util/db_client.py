import pymysql
import atexit
import logging
import os


class Client(object):

    connection = None

    def __init__(self, host, user, password, port=3306, db=None):
        self.logger = logging.getLogger('abs')
        self.connection = self._get_connection(host, user, password, db)
        atexit.register(self._close)

    def get_table_names(self, db, table_prefix) -> list:
        with self.connection.cursor() as cursor:
            self.connection.select_db(db)
            sql = f"SHOW TABLES LIKE '{pymysql.escape_string(table_prefix)}%'"
            cursor.execute(sql)

            return [row[0] for row in cursor.fetchall()]

    def delete_tables(self, db: str, tables: list):
        tables = [f'{db}.{table}' for table in tables]
        tables_str = ','.join(tables)

        with self.connection.cursor() as cursor:
            sql = f"DELETE TABLE {tables_str}"
            cursor.execute(sql)

        self.connection.commit()

    def copy_table_scheme(self, source_db: str, target_db: str, source_table: str, target_table: str,
                          without_indexes=True):
        with self.connection.cursor() as cursor:
            self.connection.select_db(target_db)

            source_table_with_db = pymysql.escape_string(f'{source_db}.{source_table}')
            target_table = pymysql.escape_string(target_table)

            if without_indexes:
                sql = f'CREATE TABLE {target_table} AS SELECT * FROM {source_table_with_db} WHERE 1=2'
            else:
                sql = f'CREATE TABLE {target_table} LIKE {source_table_with_db}'

            cursor.execute(sql)

        self.connection.commit()

    def insert_from(self, source_db, target_db, source_table, target_table, joins: list, where: list):
        select = self._build_select_sql(source_db, source_table, joins, where)

        with self.connection.cursor() as cursor:
            sql = f'INSERT INTO {target_db}.{target_table} ({select})'
            self.logger.debug(f'RUN... ({sql})')
            cursor.execute(sql)

        self.connection.commit()

    def _get_connection(self, host, user, password, db):
        connection = pymysql.connect(host=host,
                                     user=user,
                                     password=password,
                                     db=db)

        self.logger.info(f'数据库连接初始化 pid: {os.getpid()} ppid: {os.getppid()}')

        return connection

    def _close(self):
        self.logger.info('数据库连接关闭')
        self.connection.close()

    @classmethod
    def _build_select_sql(cls, source_db, source_table, joins, where):
        select = ['SELECT', f'DISTINCT {source_db}.{source_table}.*', 'FROM', f'{source_db}.{source_table}']

        for join in joins:
            select.append(f"INNER JOIN {source_db}.{join[0]} ON {source_db}.{join[1]} {join[2]} {source_db}.{join[3]}")

        if where:
            select.append(f"WHERE {source_db}.{where[0]} {where[1]} {where[2]}")

        select_str = ' '.join(select)

        return select_str

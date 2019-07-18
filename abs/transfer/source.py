import logging
import copy
from multiprocessing import Pool
from pymysql.err import DatabaseError
from abs.util.db_client import Client


class Source(object):

    db_client = None

    target_db = ''
    source_tables = ''

    transfer_table_names = None

    def __init__(self, source_db: str, source_name_prefix: str = '', target_name_prefix: str = '',
                 data_filters: dict = None, included_tables: list = None, excluded_tables: list = None):

        self.logger = logging.getLogger('abs')

        self.source_db = source_db
        self.source_table_prefix = source_name_prefix
        self.target_table_prefix = target_name_prefix
        self.data_filters = data_filters

        self.included_tables = included_tables
        self.excluded_tables = excluded_tables

    def migrate(self, pool: Pool, db_client: Client, target_db: str, migrate_data: bool):
        self.target_db = target_db
        self.db_client = db_client

        self._transfer_scheme()  # scheme 复制不能用多进程处理，因为数据复制依赖于此。另外这里速度很快也不需要

        if migrate_data:
            self._transfer_data(pool)

    @staticmethod
    def _transfer_data_internal(source_db, target_db, source_table, target_table, joins, where):
        """
        子进程执行的业务
        """
        try:
            Source._transfer_data_internal._db_client.insert_from(source_db, target_db, source_table, target_table, joins,
                                                           where)
        except DatabaseError as e:
            Source._transfer_data_internal._logger.error(f'数据表 {source_table} 复制错误 ({str(e)})')

    def _transfer_data(self, pool: Pool):
        """
        进程池异步并发处理
        """
        for source_table, target_table in self.transfer_table_names:
            joins = []
            common_where = copy.deepcopy(self.data_filters['_default'])
            common_where[0] = f"{source_table}.{common_where[0]}"  # 补表名
            where = common_where

            if source_table in self.data_filters and 'join' in self.data_filters[source_table]:
                joins = self.data_filters[source_table]['join']

            if source_table in self.data_filters and 'where' in self.data_filters[source_table]:
                where = self.data_filters[source_table]['where']

            pool.apply_async(self._transfer_data_internal,
                             (self.source_db, self.target_db, source_table, target_table, joins, where))

    def _transfer_scheme(self):
        self.transfer_table_names = self._get_transfer_table_names()

        for source_table, target_table in self.transfer_table_names:
            self.db_client.copy_table_scheme(self.source_db, self.target_db, source_table, target_table)
            self.logger.debug(f'copy table scheme {source_table} to {target_table}')

    def _get_transfer_table_names(self) -> list:
        """
        场景1
        source_table_prefix = db_chalkng_school_3_
        target_table_prefix = ''
        source_table = db_chalkng_school_3_nas_score
        target_table = nas_score
        场景2
        source_table_prefix = ''
        target_table_prefix = db_chalkng_school_3_
        source_table = nas_score
        target_table = db_chalkng_school_3_nas_score

        :return: array [[source,target]...]
        """
        source_tables = self.db_client.get_table_names(self.source_db, self.source_table_prefix)
        transfer_tables = []

        for source_table in source_tables:

            if source_table.startswith(self.source_table_prefix):
                origin_table = source_table[len(self.source_table_prefix):]
            else:
                origin_table = source_table

            if (self.included_tables and origin_table not in self.included_tables) or (self.excluded_tables and origin_table in self.excluded_tables):
                continue

            target_table = f'{self.target_table_prefix}{origin_table}'

            transfer_tables.append([source_table, target_table])

        return transfer_tables

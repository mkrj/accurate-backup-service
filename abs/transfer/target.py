import logging
import oss2
import subprocess
import datetime
import multiprocessing
import zstandard as zstd
from oss2 import Bucket
from abs.transfer.source import Source
from abs.util.db_client import Client
from abs.config.db import db_host, db_user, db_password, db_port, data_transfer_process_amount, \
    db_uploader_process_amount, backup_config, group_id
from abs.config.oss import endpoint, access_key_id, access_key_secret, target_bucket


class Target(object):

    sources = None

    def __init__(self, target_db: str):
        self.db_client = Client(db_host, db_user, db_password, port=db_port)
        self.logger = logging.getLogger('abs')

        self.target_db = target_db

    def add_source(self, source: Source):
        if self.sources is None:
            self.sources = []

        self.sources.append(source)

    def migrate(self, migrate_data=True, upload=False):
        if not self.sources:
            return None

        # 数据复制到目标库
        transfer_pool = multiprocessing.Pool(processes=data_transfer_process_amount, initializer=self._init_db_client,
                                             initargs=(Source._transfer_data_internal,))

        for source in self.sources:
            source.migrate(transfer_pool, self.db_client, self.target_db, migrate_data)

        # 调用join之前，先调用close函数，否则会出错，执行完close后不会有新的进程加入到pool
        transfer_pool.close()
        # join函数等待所有子进程结束
        transfer_pool.join()

        # 目标库导出压缩并上传
        if not upload:
            return None

        uploader_pool = multiprocessing.Pool(processes=db_uploader_process_amount, initializer=self._init_uploader,
                                             initargs=(self._upload_dumped_db_internal,))

        self._upload_dumped_db(uploader_pool)

        uploader_pool.close()
        uploader_pool.join()

    @staticmethod
    def _init_db_client(function):
        """
        每个子进度初始化独立的 mysql 连接
        :param function: 用来向子进程传递参数，避免使用全局变量
        :return:
        """
        function._db_client = Client(db_host, db_user, db_password, port=db_port)
        function._logger = logging.getLogger('abs')

    @staticmethod
    def _init_uploader(function):
        function._logger = logging.getLogger('abs')

    def _upload_dumped_db(self, pool):
        """
        mysqldump 导出并上传 target_db 中的数据表
        """
        table_names = []
        for source_config in backup_config['source']:
            table_names.extend(self.db_client.get_table_names(self.target_db, source_config['target_table_prefix']))

        self.logger.info(f'待导出表: {",".join(table_names)}')

        for table in table_names:
            pool.apply_async(self._upload_dumped_db_internal, (self.target_db, table))

    @staticmethod
    def _upload_dumped_db_internal(target_db: str, table: str):
        """
        流式上传数据至阿里云 oss
        """
        auth = oss2.Auth(access_key_id, access_key_secret)
        bucket: Bucket = oss2.Bucket(auth, endpoint, target_bucket)
        logger: logging.Logger = Target._upload_dumped_db_internal._logger

        composer = zstd.ZstdCompressor(level=5)

        today = datetime.datetime.now().strftime('%Y%m%d')
        cmd = f'mysqldump --host={db_host} --user={db_user} --password={db_password} {target_db} {table}'

        with subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE) as dump_stream:
            with composer.stream_reader(dump_stream.stdout) as composer_stream:
                object_key = f'group_backup/{group_id}-{today}/{table}'
                result = bucket.put_object(object_key, composer_stream)

                logger.info(f'已上传 {object_key} http status: {result.status}')

        # 如需保存到本地
        # cmd = f'mysqldump --user={db_user} --password={db_port} {target_db} {table}'
        # with open('/path/to/file', 'wb') as fh:
        #     with subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE) as p:
        #         with composer.stream_reader(p.stdout) as r:
        #             while True:
        #                 file = r.read(1024 * 1024 * 10)
        #                 fh.write(file)
        #                 if not file:
        #                     break

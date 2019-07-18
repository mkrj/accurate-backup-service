from pymysql.converters import escape_int
from os import getenv

db_host = getenv('DB_HOST', '127.0.0.1')
db_user = getenv('DB_USER', 'root')
db_password = getenv('DB_PASSWORD')
db_port = int(getenv('DB_PORT', 3306))

# source 到 target 数据库迁移的进程数量
data_transfer_process_amount = int(getenv('DATA_TRANSFER_PROCESS_AMOUNT'))
db_uploader_process_amount = int(getenv('DB_UPLOADER_PROCESS_AMOUNT'))

# 组织 ID
group_id = escape_int(getenv('GROUP_ID'))

backup_config = {
    'target': [
        {
            'target_db': 'test'
        },
    ],
    'source': [
        {
            'target': 'test',  # 和 target.[].target_db 对应
            'source_db': 'prod_order',
            'source_table_prefix': '',
            'target_table_prefix': f'group_{group_id}_db_order_',
            'source_data_filters': {
                # 某些表中可能没有 org_id 这个字段可供过滤，因此需要通过 join 有 org_id 的表，然后过滤
                # 类 laravel query buildv语法
                '_default': ['org_id', '=', group_id],
                'drop_log': {
                    'join': [['order', 'order.id', '=', 'drop_log.order_id']],
                    'where': ['order.org_id', '=', group_id]
                },
                'choice': {
                    'join': [['order', 'order.id', '=', 'choice.order_id']],
                    'where': ['order.org_id', '=', group_id]
                },
                'question_option': {
                    'join': [
                        ['question', 'question.id', '=',
                         'question_option.question_id'],
                        ['order', 'ordern.id', '=', 'question.order_id'],
                    ],
                    'where': ['order.org_id', '=', group_id]
                },
                'assignment_status': {
                    'where': ['1', '=', '1']
                }
            },
            'excluded_tables': [
                # 静态数据
                'org_place', 'rank', 'requirement', 'college_requirement_major',
                # 系统数据
                'launcher_feature', 'major', 'migration', 'rdr_checksum', 'rdr_error', 'rdr_status',
                # 其他不需要的数据
                'approval_log'

            ],
        },
        {
            'source_db': 'prod_platform',
            'target': 'test',
            'source_table_prefix': '',
            'target_table_prefix': f'group_{group_id}_db_platform_',
            'source_data_filters': {
                '_default': ['org_id', '=', group_id],
                'articles': {
                    'join': [
                        ['users', 'users.uid', '=', 'articles.created_by'],
                        ['user_reflections', 'user_reflections.uid', '=', 'users.uid']
                    ],
                    'where': ['user_reflections.org_id', '=', group_id]
                },
                'files': {
                    'join': [
                        ['users', 'users.uid', '=', 'files.user_id'],
                        ['user_reflections', 'user_reflections.uid', '=', 'users.uid']
                    ],
                    'where': ['user_reflections.org_id', '=', group_id]
                },
                'users': {
                    'join': [['user_reflections', 'user_reflections.uid', '=', 'users.uid']],
                    'where': ['user_reflections.org_id', '=', group_id]
                }
            },
            'excluded_tables': [
                'apps', 'migrations', 'tokens', 'user_profile_developers',
                'schedules', 'short_urls',
            ],
        }
    ]
}

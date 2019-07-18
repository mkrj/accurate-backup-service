import click
import logging.config
from abs.transfer.source import Source
from abs.transfer.target import Target

from abs.config.log import log_config
from abs.config.db import backup_config


@click.group()
def cli():
    pass


@cli.command()
def backup():
    target = {}

    for target_config in backup_config['target']:
        target[target_config['target_db']] = Target(target_config['target_db'])

    for source_config in backup_config['source']:
        source = Source(
            source_config['source_db'],
            source_config['source_table_prefix'],
            source_config['target_table_prefix'],
            data_filters=source_config['source_data_filters'],
            excluded_tables=source_config['excluded_tables']
        )

        target[source_config['target']].add_source(source)

    for target_config in backup_config['target']:
        target[target_config['target_db']].migrate(migrate_data=True, upload=True)


@cli.command()
def restore():
    pass


if __name__ == '__main__':
    logging.config.dictConfig(log_config)
    cli()

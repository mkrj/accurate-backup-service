from os import getenv
log_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'default': {
            'format': '[%(asctime)s][%(process)s][%(levelname)s][%(module)s] %(message)s',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'formatter': 'default',
        },
        'application_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': getenv('LOG_FILE', 'logs/abs.log'),
            'maxBytes': 1024 * 1024 * 20,
            'backupCount': 20,
            'formatter': 'default',
        },
    },
    'loggers': {
        'abs': {
            'handlers': ['console', 'application_file'],
            'level': getenv('LOG_LEVEL', 'INFO'),
            'propagate': True
        },
    },
}
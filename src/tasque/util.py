import logging

LOGGER = None

def set_logger(logger):
    global LOGGER
    LOGGER = logger

def set_default_logger():
    global LOGGER
    LOGGER = logging.getLogger('pipeline_executor')

def _LOG(msg, level, buf=None):
    msg = str(msg)
    if LOGGER:
        if level == 'info':
            LOGGER.info(msg)
        elif level == 'debug':
            LOGGER.debug(msg)
        elif level == 'warn':
            LOGGER.warning(msg)
        elif level == 'error':
            LOGGER.error(msg)
        else:
            LOGGER.info(msg)
    if buf is not None:
        buf.write(f'[{level}] {msg}\n')

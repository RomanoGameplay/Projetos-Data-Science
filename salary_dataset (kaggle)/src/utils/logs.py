import logging
from pathlib import Path


def configura_logs():
    # cria uma pasta com a saída dos "logs"
    logs = Path(__file__).parent.parent / 'logs'
    logs.mkdir(parents=True, exist_ok=True)

    #
    raw_log = logging.getLogger()
    raw_log.handlers
    raw_log.setLevel(logging.INFO)

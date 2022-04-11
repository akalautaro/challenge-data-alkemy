import logging as log
import os

try:
    os.makedirs('../logs/')
except FileExistsError:
    pass

log.basicConfig(level=log.CRITICAL,
                format='%(asctime)s: %(levelname)s [%(filename)s:%(lineno)s] %(message)s',
                datefmt='%I:%M:%S %p',
                handlers=[
                    log.FileHandler('../logs/ETL_Process.log'),
                    log.StreamHandler()
                ])

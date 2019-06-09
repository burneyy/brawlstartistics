import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)-8s: %(name)-12s: %(message)s',
                    datefmt='%d.%m.%y %H:%M')
from .brawlstats import Client

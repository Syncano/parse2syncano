# -*- coding: utf-8 -*-
# create console handler and set level to debug
import logging

__version__ = '0.0.1'
VERSION = __version__

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger('moses')
log.addHandler(handler)

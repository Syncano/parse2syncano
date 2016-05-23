# -*- coding: utf-8 -*-
# create console handler and set level to debug
import logging

__version__ = '0.0.4'
VERSION = __version__

__author__ = "Sebastian Opalczynski"
__credits__ = ["Sebastian Opalczynski"]
__copyright__ = 'Copyright 2016 Syncano'
__license__ = 'MIT'

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger('moses')
log.addHandler(handler)
log.setLevel(logging.INFO)

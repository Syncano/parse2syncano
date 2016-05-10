#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
sys.path.append('/home/sopalczy/projects/syncano/parse2syncano')
sys.path.append('/home/sopalczy/projects/syncano/syncano-python')

from migrations.transfer import SyncanoTransfer


def transfer():
    transfer = SyncanoTransfer()
    transfer.through_the_red_sea()


if __name__ == '__main__':
    transfer()

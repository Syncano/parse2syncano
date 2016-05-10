#!/usr/bin/python
# -*- coding: utf-8 -*-

from migrations.transfer import SyncanoTransfer


def transfer():
    transfer = SyncanoTransfer()
    transfer.through_the_red_sea()

if __name__ == '__main__':
    transfer()

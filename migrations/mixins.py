# -*- coding: utf-8 -*-
import syncano
from parse.connection import ParseConnection
from settings import (PARSE_APPLICATION_ID, PARSE_MASTER_KEY,
                      PARSE_PAGINATION_LIMIT, SYNCANO_ADMIN_API_KEY,
                      SYNCANO_APIROOT, SYNCANO_INSTANCE_NAME)


class ParseConnectionMixin(object):

    _parse = None

    @property
    def parse(self):
        if self._parse:
            return self._parse

        parse_connection = ParseConnection(
            application_id=PARSE_APPLICATION_ID,
            master_key=PARSE_MASTER_KEY,
        )

        self._parse = parse_connection
        return self._parse


class SyncanoConnectionMixin(object):

    _syncano = None

    @property
    def syncano(self):
        if self._syncano:
            return self._syncano

        syncano_connection = syncano.connect(
            api_key=SYNCANO_ADMIN_API_KEY,
            host=SYNCANO_APIROOT,
            instance_name=SYNCANO_INSTANCE_NAME,
        )

        self._syncano = syncano_connection
        return self._syncano


class PaginationMixin(object):

    @classmethod
    def get_limit_and_skip(cls):
        return PARSE_PAGINATION_LIMIT, 0

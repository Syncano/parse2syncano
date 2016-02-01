# -*- coding: utf-8 -*-
import syncano

from helpers.classes import create_schema
from parse.connection import ParseConnection
from settings import PARSE_APPLICATION_ID, SYNCANO_INSTANCE_NAME
from settings import PARSE_MASTER_KEY


class SyncanoTransfer(object):

    def __init__(self):
        self.parse = ParseConnection(
            application_id=PARSE_APPLICATION_ID,
            master_key=PARSE_MASTER_KEY,
        )
        self.syncano = syncano.connect(
            api_key='6b78a465212cc2eb30e741d2da3838a3f7f5024e',
        )

    def get_syncano_instance(self):
        try:
            instance = self.syncano.Instance.please.get(name=SYNCANO_INSTANCE_NAME)
        except:
            instance = self.syncano.Instance.please.create(name=SYNCANO_INSTANCE_NAME)

        return instance

    def transfer_classes(self, instance):
        schemas = self.parse.get_schemas()

        for schema in schemas:
            class_name, syncano_schema = create_schema(schema)
            instance.classes.create(name=class_name, schema=syncano_schema)

    def through_the_red_sea(self):
        instance = self.get_syncano_instance()
        self.transfer_classes(instance)

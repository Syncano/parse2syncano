# -*- coding: utf-8 -*-
import logging

import syncano

from mappers.class_map import ClassAttributeMapper
from parse.connection import ParseConnection
from redsea.aggregation import DataAggregated
from settings import PARSE_APPLICATION_ID, SYNCANO_INSTANCE_NAME
from settings import PARSE_MASTER_KEY

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log = logging.getLogger(__file__)
log.addHandler(ch)


class SyncanoTransfer(object):

    def __init__(self):
        self.parse = ParseConnection(
            application_id=PARSE_APPLICATION_ID,
            master_key=PARSE_MASTER_KEY,
        )
        self.syncano = syncano.connect(
            api_key='6b78a465212cc2eb30e741d2da3838a3f7f5024e',
        )

        self.data = DataAggregated()

    def get_syncano_instance(self):
        try:
            instance = self.syncano.Instance.please.get(name=SYNCANO_INSTANCE_NAME)
        except:
            instance = self.syncano.Instance.please.create(name=SYNCANO_INSTANCE_NAME)

        return instance

    def transfer_classes(self, instance):
        schemas = self.parse.get_schemas()

        for schema in schemas:
            class_name, syncano_schema = ClassAttributeMapper.create_schema(schema)
            self.data.add_class(syncano_class=class_name, parse_class=schema['className'])
            try:
                instance.classes.create(name=class_name, schema=syncano_schema)
            except:
                log.error('Class already defined in this instance: {}/{}'.format(class_name, instance.name))

    def transfer_objects(self, instance):
        for syncano_class, parse_class in self.data.classes:
            objects = self.parse.get_class_objects(parse_class)
            for object in objects['results']:
                s_class = instance.classes.get(name=syncano_class)
                syncano_fields = ClassAttributeMapper.get_fields(object.keys())
                syncano_object = {key: value for key, value in object.iteritems() if key in syncano_fields}
                syncano_object = ClassAttributeMapper.process_object(syncano_object)
                s_class.objects.create(**syncano_object)

    def through_the_red_sea(self):
        instance = self.get_syncano_instance()
        self.transfer_classes(instance)
        self.transfer_objects(instance)

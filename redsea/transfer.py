# -*- coding: utf-8 -*-
import logging
import time

import syncano
from mappers.class_map import ClassAttributeMapper
from parse.connection import ParseConnection
from redsea.aggregation import DataAggregated
from settings import (PARSE_APPLICATION_ID, PARSE_MASTER_KEY,
                      PARSE_PAGINATION_LIMIT, SYNCANO_ADMIN_API_KEY,
                      SYNCANO_APIROOT, SYNCANO_INSTANCE_NAME)

# create console handler and set level to debug
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger('moses')
log.addHandler(handler)


class SyncanoTransfer(object):

    def __init__(self):
        self.parse = ParseConnection(
            application_id=PARSE_APPLICATION_ID,
            master_key=PARSE_MASTER_KEY,
        )
        self.syncano = syncano.connect(
            api_key=SYNCANO_ADMIN_API_KEY,
            host=SYNCANO_APIROOT,
            instance_name=SYNCANO_INSTANCE_NAME,
        )

        self.data = DataAggregated()
        self.syncano_classes = {}

    def get_syncano_instance(self):
        try:
            instance = self.syncano.Instance.please.get(name=SYNCANO_INSTANCE_NAME)
        except:
            instance = self.syncano.Instance.please.create(name=SYNCANO_INSTANCE_NAME)

        return instance

    def transfer_classes(self, instance):
        schemas = self.parse.get_schemas()

        for parse_schema in schemas:
            class_name, syncano_schema = ClassAttributeMapper.create_schema(parse_schema)
            self.data.add_class(syncano_name=class_name, syncano_schema=syncano_schema,
                                parse_name=parse_schema['className'], parse_schema=parse_schema)

        for class_to_process in self.data.sort_classes():
            try:
                instance.classes.create(name=class_to_process.syncano_name, schema=class_to_process.syncano_schema)
            except Exception as e:
                log.error('Class already defined in this instance: {}/{}'.format(class_name, instance.name))
                log.error(e.message)

    def transfer_objects(self, instance):
        for class_to_process in self.data.sort_classes():
            limit = PARSE_PAGINATION_LIMIT
            skip = 0
            processed = 0
            while True:
                objects = self.parse.get_class_objects(class_to_process.parse_name, limit=limit, skip=skip)
                if not len(objects['results']):
                    break
                limit += PARSE_PAGINATION_LIMIT
                skip += PARSE_PAGINATION_LIMIT
                objects_to_add = []
                parse_ids = []
                for object in objects['results']:
                    s_class = self.get_class(instance=instance, class_name=class_to_process.syncano_name)
                    syncano_object = ClassAttributeMapper.process_object(object, self.data.reference_map)

                    if len(objects_to_add) == 10:
                        processed += 10
                        created_objects = s_class.objects.batch(
                            *objects_to_add
                        )
                        for parse_id, syncano_id in zip(parse_ids, [o.id for o in created_objects]):
                            self.data.reference_map[parse_id] = syncano_id

                        objects_to_add = []
                        parse_ids = []
                        time.sleep(1)  # avoid throttling;
                        log.warning('Processed {} objects of class {}'.format(processed, class_to_process.syncano_name))

                    batched_syncano_object = s_class.objects.as_batch().create(**syncano_object)
                    objects_to_add.append(batched_syncano_object)
                    parse_ids.append(object['objectId'])

                # if objects to add is less than < 10 elements
                if objects_to_add:
                    created_objects = s_class.objects.batch(
                        *objects_to_add
                    )
                    for parse_id, syncano_id in zip(parse_ids, [o.id for o in created_objects]):
                        self.data.reference_map[parse_id] = syncano_id
                    log.warning('Processed {} elements of {} in class: {}'.format(len(objects_to_add),
                                                                                  len(objects_to_add),
                                                                                  class_to_process.syncano_name))

    def get_class(self, instance, class_name):
        s_class = self.syncano_classes.get(class_name)
        if not s_class:
            s_class = instance.classes.get(name=class_name)
            self.syncano_classes[class_name] = s_class
        return s_class

    def through_the_red_sea(self):
        instance = self.get_syncano_instance()
        self.transfer_classes(instance)
        self.transfer_objects(instance)

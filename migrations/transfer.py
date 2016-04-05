# -*- coding: utf-8 -*-
import time

import syncano

from log_handler import log
from migrations.aggregation import DataAggregated
from parse.connection import ParseConnection
from processors.klass import ClassProcessor
from settings import (PARSE_APPLICATION_ID, PARSE_MASTER_KEY,
                      PARSE_PAGINATION_LIMIT, SYNCANO_ADMIN_API_KEY,
                      SYNCANO_APIROOT, SYNCANO_INSTANCE_NAME)


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
        self.relations = None

    def set_relations(self, relations):
        self.relations = relations

    def process_relations(self, instance):
        if self.relations:
            for class_relation in self.relations:
                for class_name, relations in class_relation.iteritems():
                    for relation in relations:
                        for field_name, relation_meta in relation.iteritems():
                            relation_class_name = "{}_{}".format(class_name, relation_meta['targetClass'])
                            schema = [
                                {
                                    'name': class_name,
                                    'type': 'reference',
                                    'target': class_name
                                },
                                {
                                    'name': field_name,
                                    'type': 'reference',
                                    'target': ClassProcessor.normalize_class_name(relation_meta['targetClass'])
                                },
                            ]

                            try:
                                instance.classes.create(
                                        name=relation_class_name,
                                        schema=schema
                                )
                            except Exception as e:
                                log.error('Class already defined in this instance: {}/{}'.format(relation_class_name,
                                                                                                 instance.name))
                                log.error(e.message)

                            # get the parse classes now;
                            for parse_class_name, objects_id_map in self.data.reference_map.iteritems():
                                if class_name == ClassProcessor.normalize_class_name(parse_class_name):
                                    for parse_id, syncano_id in objects_id_map.iteritems():
                                        query = {
                                            "$relatedTo": {
                                                "object": {
                                                    "__type": "Pointer",
                                                    "className": parse_class_name,
                                                    "objectId": parse_id},
                                                "key": field_name}
                                        }
                                        limit, skip = self.get_limit_and_skip()

                                        objects = self.parse.get_class_objects(relation_meta['targetClass'],
                                                                               limit=limit, skip=skip, query=query)


    def get_syncano_instance(self):
        try:
            instance = self.syncano.Instance.please.get(name=SYNCANO_INSTANCE_NAME)
        except:
            instance = self.syncano.Instance.please.create(name=SYNCANO_INSTANCE_NAME)

        return instance

    def transfer_classes(self, instance):
        schemas = self.parse.get_schemas()

        relations = []

        for parse_schema in schemas:
            syncano_schema = ClassProcessor.create_schema(parse_schema)
            self.data.add_class(syncano_name=syncano_schema.class_name, syncano_schema=syncano_schema.schema,
                                parse_name=parse_schema['className'], parse_schema=parse_schema)

            if syncano_schema.has_relations:
                relations.append(
                    {
                        syncano_schema.class_name: syncano_schema.relations
                    }
                )

        for class_to_process in self.data.sort_classes():
            try:
                instance.classes.create(name=class_to_process.syncano_name, schema=class_to_process.syncano_schema)
            except Exception as e:
                log.error('Class already defined in this instance: {}/{}'.format(syncano_schema.class_name,
                                                                                 instance.name))
                log.error(e.message)

        self.set_relations(relations)

    def transfer_objects(self, instance):
        for class_to_process in self.data.sort_classes():
            limit, skip = self.get_limit_and_skip()

            processed = 0
            while True:
                objects = self.parse.get_class_objects(class_to_process.parse_name, limit=limit, skip=skip)
                if not len(objects['results']):
                    break
                limit += PARSE_PAGINATION_LIMIT
                skip += PARSE_PAGINATION_LIMIT
                objects_to_add = []
                parse_ids = []
                for data_object in objects['results']:
                    s_class = self.get_class(instance=instance, class_name=class_to_process.syncano_name)
                    syncano_object = ClassProcessor.process_object(data_object, self.data.reference_map)

                    if len(objects_to_add) == 10:
                        processed += 10
                        created_objects = s_class.objects.batch(
                            *objects_to_add
                        )
                        for parse_id, syncano_id in zip(parse_ids, [o.id for o in created_objects]):
                            self.data.reference_map[class_to_process.parse_name][parse_id] = syncano_id

                        objects_to_add = []
                        parse_ids = []
                        time.sleep(1)  # avoid throttling;
                        log.warning('Processed {} objects of class {}'.format(processed, class_to_process.syncano_name))

                    batched_syncano_object = s_class.objects.as_batch().create(**syncano_object)
                    objects_to_add.append(batched_syncano_object)
                    parse_ids.append(data_object['objectId'])

                # if objects to add is less than < 10 elements
                if objects_to_add:
                    created_objects = s_class.objects.batch(
                        *objects_to_add
                    )
                    for parse_id, syncano_id in zip(parse_ids, [o.id for o in created_objects]):
                        self.data.reference_map[class_to_process.parse_name][parse_id] = syncano_id
                    log.warning('Processed {} elements of {} in class: {}'.format(len(objects_to_add),
                                                                                  len(objects_to_add),
                                                                                  class_to_process.syncano_name))

    def get_class(self, instance, class_name):
        s_class = self.syncano_classes.get(class_name)
        if not s_class:
            s_class = instance.classes.get(name=class_name)
            self.syncano_classes[class_name] = s_class
        return s_class

    @classmethod
    def get_limit_and_skip(cls):
        return PARSE_PAGINATION_LIMIT, 0

    def through_the_red_sea(self):
        instance = self.get_syncano_instance()
        self.transfer_classes(instance)
        self.transfer_objects(instance)
        self.process_relations(instance)

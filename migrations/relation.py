# -*- coding: utf-8 -*-
# a relation helper
import time

from log_handler import log
from migrations.aggregation import data_aggregate
from migrations.mixins import PaginationMixin, ParseConnectionMixin
from processors.klass import ClassProcessor
from settings import PARSE_PAGINATION_LIMIT
from syncano.models import Object


class RelationProcessor(ParseConnectionMixin, PaginationMixin):

    def __init__(self, relations, *args, **kwargs):
        super(RelationProcessor, self).__init__(*args, **kwargs)
        self.relations = relations
        self.reference_map = data_aggregate.reference_map

    def process(self, instance):
        for class_relation in self.relations:
                for class_name, relations in class_relation.iteritems():
                    for relation in relations:
                        for field_name, relation_meta in relation.iteritems():
                            target_name = relation_meta['targetClass']
                            relation_class_name = "{}_{}".format(class_name, target_name)
                            schema = self.create_join_class_schema(class_name, field_name, target_name)
                            self.create_join_class(instance, relation_class_name, schema)

                            # get the parse classes now;
                            for parse_class_name, objects_id_map in self.reference_map.iteritems():
                                if class_name == ClassProcessor.normalize_class_name(parse_class_name):
                                    for parse_id, syncano_id in objects_id_map.iteritems():
                                        limit, skip = self.get_limit_and_skip()

                                        while True:

                                            objects = self.get_parse_objects(parse_class_name, parse_id, field_name,
                                                                             target_name, limit, skip)
                                            if not objects['results']:
                                                break

                                            limit += PARSE_PAGINATION_LIMIT
                                            skip += PARSE_PAGINATION_LIMIT

                                            objects_to_add = []

                                            for data_object in objects['results']:

                                                syncano_object = {
                                                    class_name: syncano_id,
                                                    field_name: self.reference_map[
                                                        ClassProcessor.normalize_class_name(target_name)
                                                    ][data_object['objectId']],
                                                    'class_name': relation_class_name,
                                                }

                                                if len(objects_to_add) == 10:
                                                    Object.please.batch(
                                                        *objects_to_add
                                                    )

                                                    objects_to_add = []
                                                    time.sleep(1)  # avoid throttling;

                                                batched_syncano_object = Object.please.as_batch().create(
                                                    **syncano_object
                                                )
                                                objects_to_add.append(batched_syncano_object)

                                            if objects_to_add:
                                                Object.please.batch(
                                                    *objects_to_add
                                                )

    @classmethod
    def create_join_class_schema(cls, class_name, field_name, target_name):
        return [
            {
                'name': class_name,
                'type': 'reference',
                'target': class_name
            },
            {
                'name': field_name,
                'type': 'reference',
                'target': ClassProcessor.normalize_class_name(target_name)
            },
        ]

    @classmethod
    def create_join_class(cls, instance, relation_class_name, schema):
        try:
            instance.classes.create(
                name=relation_class_name,
                schema=schema
            )
        except Exception as e:
            log.error('Class already defined in this instance: {}/{}'.format(relation_class_name,
                                                                             instance.name))
            log.error(e.message)

    def get_parse_objects(self, parse_class_name, parse_id, field_name, target_name, limit, skip):
        query = {
            "$relatedTo": {
                "object": {
                    "__type": "Pointer",
                    "className": parse_class_name,
                    "objectId": parse_id},
                "key": field_name}
        }

        return self.parse.get_class_objects(target_name, limit=limit, skip=skip, query=query)

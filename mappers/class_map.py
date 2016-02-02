# -*- coding: utf-8 -*-

import json
from datetime import datetime


class ClassAttributeMapper(object):
    map = {
        'Number': 'integer',
        'Date': 'datetime',
        'Boolean': 'boolean',
        'String': 'string',
        'Array': 'text',
        'Object': 'text',
    }

    @classmethod
    def handle_value(cls, value):
        return value

    @classmethod
    def handle_json_value(cls, value):
        return json.dumps(value)

    @classmethod
    def get_fields(cls, parse_fields):
        FIELDS_TO_SKIP = ['updatedAt', 'createdAt', 'ACL', 'self']  # TODO: handle ACL later on

        fields = []

        for field in parse_fields:
            if field in FIELDS_TO_SKIP:
                continue
            fields.append(field)
        return fields

    @classmethod
    def process_object(cls, syncano_object):
        for key, value in syncano_object.iteritems():
            if isinstance(value, dict) and '__type' in value:
                if value['__type'] == 'Date':
                    syncano_object[key] = None #value['iso']
        return syncano_object

    @classmethod
    def create_schema(cls, parse_schema):
        """
        Return syncano schema for a class;
        :param parse_schema: the schema from parse;
        :return: tha class name and the schema used in Syncano;
        """

        FIELDS_TO_SKIP = ['updatedAt', 'createdAt', 'ACL']  # TODO: handle ACL later on

        schema = []

        for field, field_meta in parse_schema['fields'].iteritems():
            if field not in FIELDS_TO_SKIP:
                type = field_meta['type']
                if type in ['Relation', 'Pointer']:  # TODO: skip for now
                    continue

                new_type = ClassAttributeMapper.map[type]

                if field == 'objectId':
                    schema.append({'name': field, 'type': new_type, 'filter_index': True})
                else:
                    schema.append({'name': field, 'type': new_type})

        name = parse_schema['className']
        if name.startswith('_'):
            name = 'internal_' + name[1:]
        return name, schema
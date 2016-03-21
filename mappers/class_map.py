# -*- coding: utf-8 -*-

import json

from parse.constants import ParseFieldTypeE


class ClassAttributeMapper(object):
    map = {
        'Number': 'integer',
        'Date': 'datetime',
        'Boolean': 'boolean',
        'String': 'string',
        'Array': 'array',
        'Object': 'object',
        'Pointer': 'reference',
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
            fields.append(field.lower())
        return fields

    @classmethod
    def process_object(cls, parse_object, reference_map):
        syncano_fields = ClassAttributeMapper.get_fields(parse_object.keys())
        processed_object = {}
        for key, value in parse_object.iteritems():
            if isinstance(value, dict):
                if '__type' in value:
                    if value['__type'] == ParseFieldTypeE.DATE:
                        processed_object[key.lower()] = value['iso']
                    if value['__type'] == ParseFieldTypeE.POINTER:
                        processed_object[key.lower()] = reference_map.get(value['objectId'])
                else:  # and 'Object' case
                    processed_object[key.lower()] = json.dumps(value)
            elif isinstance(value, list):
                processed_object[key.lower()] = json.dumps(value)
            else:
                if key.lower() in syncano_fields:
                    processed_object[key.lower()] = value
        return processed_object

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
                if type in ['Relation']:  # TODO: skip for now
                    continue

                new_type = ClassAttributeMapper.map[type]

                if field == 'objectId':
                    schema.append({'name': field.lower(), 'type': new_type, 'filter_index': True})
                elif new_type == 'reference':
                    schema.append({'name': field.lower(), 'type': new_type,
                                   'target': cls.normalize_class_name(field_meta['targetClass'])})
                else:
                    schema.append({'name': field.lower(), 'type': new_type})
        name = cls.normalize_class_name(parse_schema['className'])
        return name, schema

    @classmethod
    def normalize_class_name(cls, class_name):
        name = class_name
        if name.startswith('_'):
            name = 'internal_' + name[1:].lower()
        return name

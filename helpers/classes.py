from mappers.class_map import class_map


def create_schema(parse_schema):
    """
    Return syncano schema for a class;
    :param parse_schema: the schema from parse;
    :return: tha class name and the schema used in Syncano;
    """

    FIELDS_TO_SKIP = ['objectId', 'updatedAt', 'createdAt', 'ACL']  # TODO: handle ACL later on

    schema = []

    for field, field_meta in parse_schema['fields'].iteritems():
        if field not in FIELDS_TO_SKIP:
            type = field_meta['type']
            if type in ['Relation', 'Pointer']:  # TODO: skip for now
                continue
            if type == 'Object':  # TODO: also skip this now; identify - what is it?
                continue
            new_type = class_map[type]
            schema.append({'name': field, 'type': new_type})
    name = parse_schema['className']
    if name.startswith('_'):
        name = 'internal_' + name[1:]
    return name, schema

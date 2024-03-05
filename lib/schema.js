'use strict';
const ObjectID = require('bson-objectid');

const {
    kExtendedTypeDate,
    kExtendedTypeBuffer,

    ajv, marshallingAjv, unMarshallingAjv,

    kSchemaCompiled,
    kSchemaMarshall,
    kSchemaUnMarshall,
    kSchemaIndices,
    kSchemaNewId,


    DocIdField,
    TypeField,
    VersionField,
    CreatedAtField,
    UpdatedAtField,

} = require('./shared.js');

const validIndexName = /^[a-zA-Z0-9_.-]{3,255}$/;

const indexSpecSchema = ajv.compile({
    type: 'object',
    properties: {
        'hashKey': {type:'string'},
        'sortKey': {type:'string'}
    },
    required: ['hashKey'],
    additionalProperties: false
});

const attributeTypeFromSchema = (indexName, schema, propertyName) => {
    if (typeof (schema?.properties?.[propertyName]) === 'undefined') {
        throw new Error(`The schema must define the type of property .${propertyName} used by index "${indexName}".`);
    } else if (schema?.properties?.[propertyName].type === 'string') {
        return 'S';
    } else if (schema?.properties?.[propertyName]?.extendedType === kExtendedTypeBuffer) {
        return 'B';
    } else if (schema?.properties?.[propertyName]?.extendedType === kExtendedTypeDate || schema?.properties?.[propertyName].type === 'number') {
        return 'N';
    }
    throw new Error(`The schema type of property .${propertyName}, "${JSON.stringify(schema?.properties?.[propertyName])}" used by index "${indexName}" is not indexable.`);
};

const parseIndexSpecification = (index, schemaSource) => {
    if (!index) return [];
    const indices = [];
    for (const [indexName, indexSpec] of Object.entries(index)) {
        if (!validIndexName.exec(indexName)) {
            throw new Error(`Invalid index name "${indexName}": Must be between 3 and 255 characters long, and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'.`);
        }
        if (indexName === 'type') {
            throw new Error('Invalid index name "type": this name is reserved for the built-in type index.');
        }
        const index = {
            IndexName: indexName,
            Projection: { ProjectionType: 'KEYS_ONLY' }
        };
        const requiredAttributes = [];
        let hashKey, sortKey;

        if (indexSpec === 1 || indexSpec === true) {
            index.KeySchema = [
                { AttributeName: indexName, KeyType: 'HASH' }
            ];
            requiredAttributes.push({
                AttributeName: indexName,
                AttributeType: attributeTypeFromSchema(indexName, schemaSource, indexName)
            });
            hashKey = indexName;
        } else if (indexSpecSchema(indexSpec)){
            index.KeySchema = [
                { AttributeName: indexSpec.hashKey, KeyType: 'HASH'}
            ];
            requiredAttributes.push({
                AttributeName: indexSpec.hashKey,
                AttributeType: attributeTypeFromSchema(indexName, schemaSource, indexSpec.hashKey)
            });
            hashKey = indexSpec.hashKey;
            if (indexSpec.sortKey) {
                index.KeySchema.push({
                    AttributeName: indexSpec.sortKey,
                    KeyType: 'RANGE'
                });
                requiredAttributes.push({
                    AttributeName: indexSpec.sortKey,
                    AttributeType: attributeTypeFromSchema(indexName, schemaSource, indexSpec.sortKey)
                });
                sortKey = indexSpec.sortKey;
            }
        } else {
            throw new Error('Invalid index specification ${indexSpec}: must be 1,true, or {hashKey:"", sortKey?:""}.');
        }
        indices.push({
            index,
            requiredAttributes,
            hashKey,
            sortKey
        });
    }
    return indices;
};

const findPropertyValue = (object, value, duplicateErrMsg) => {
    let r = null;
    for (const [k, v] of Object.entries(object)) {
        if (v === value) {
            if (r === null) {
                r = k;
            } else {
                throw new Error(duplicateErrMsg);
            }
        }
    }
    return r;
};

class Schema {
    // Public fields:
    name = '';
    idFieldName = '';
    typeFieldName = '';
    versionFieldName = '';
    createdAtFieldName = '';
    updatedAtFieldName = '';
    source = null;
    methods = Object.create(null);
    statics = Object.create(null);
    virtuals = Object.create(null);
    converters = new Array();

    // Protected fields:
    [kSchemaCompiled] = null;
    [kSchemaIndices] = [];
    [kSchemaNewId] = null;
    [kSchemaMarshall] = null;
    [kSchemaUnMarshall] = null;

    constructor(name, schemaSource, options) {
        const { index, generateId, versioning } = options ?? {};
        if (['object', 'undefined'].includes(typeof schemaSource) === false) {
            throw new Error('Invalid schema: must be an object or undefined.');
        }

        schemaSource = schemaSource ?? {};
        const schemaSourceProps = schemaSource.properties ?? {};

        if (!(name && (typeof name === 'string'))) {
            throw new Error('Invalid name: must be a string.');
        }
        this.name = name;
        // TODO: since the id and type field names must be the same across the
        // whole table, it would make sense to allow them to be specified in
        // options (taking lower precedence than the values in the schema)
        // extract the field names for fields with special meanings
        this.idFieldName   = findPropertyValue(schemaSourceProps, DocIdField, 'Duplicate id field.')  || 'id';
        this.typeFieldName = findPropertyValue(schemaSourceProps, TypeField, 'Duplicate type field.') || 'type';
        this.createdAtFieldName = findPropertyValue(schemaSourceProps, CreatedAtField, 'Duplicate createdAt field.');
        this.updatedAtFieldName = findPropertyValue(schemaSourceProps, UpdatedAtField, 'Duplicate updatedAt field.');

        const versionFieldName = findPropertyValue(schemaSourceProps, VersionField, 'Duplicate version field.');
        if (versioning === false) {
            if (versionFieldName) {
                options.logger.warn(`options.versioning is false, so the ${this.name} Schema properties VersionField .${versionFieldName} is ignored`);
            }
        } else {
            this.versionFieldName = versionFieldName || 'v';
        }

        if (schemaSource?.type && schemaSource?.type !== 'object') {
            throw new Error('Schema type must be object (or can be omitted).');
        }

        const schemaProperties = Object.assign(
            Object.create(null),
            schemaSourceProps,
            // ensure optional required fields are present in schema:
            {
                [this.idFieldName]: DocIdField,
                [this.typeFieldName]: TypeField,
            },
            {
                ...(this.versionFieldName?  {[this.versionFieldName]: VersionField} : {})
            }

        );

        const schemaRequired = [...new Set([this.idFieldName, this.typeFieldName, ...(schemaSource?.required ?? [])])];

        schemaSource = {
            type: 'object',
            properties: schemaProperties,
            required: schemaRequired,
            additionalProperties: schemaSource?.additionalProperties
        };

        this.source = schemaSource;
        this[kSchemaCompiled] = ajv.compile(schemaSource);
        this[kSchemaMarshall] = marshallingAjv.compile(schemaSource);
        this[kSchemaUnMarshall] = unMarshallingAjv.compile(schemaSource);
        this[kSchemaIndices] = parseIndexSpecification(index, schemaSource);
        this[kSchemaNewId] = generateId ?? this.#generateDefaultId.bind(this);
    }

    // Private methods:
    #generateDefaultId() {
        // TODO, generic model loading requires ids always to start with
        // this.name + separator, a more generic compound-id api needs to
        // require this? Or dynamic model loading should be scrapped?
        return this.name + '.' + ObjectID();
    }
}

module.exports = {
    Schema
};

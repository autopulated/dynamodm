'use strict';

const Ajv = require('ajv');

// internal types for AJV
const kExtendedTypeDate = Symbol('extendedType:date');
const kExtendedTypeBuffer = Symbol('extendedType:buffer');

// this AJV instance is used for general validation, compiled schemas do not modify data
const ajv = new Ajv({
    useDefaults: true
});

// this AJV instance is used for marshalling our built-in types before saving to Dynamo (e.g. Date -> number)
const marshallingAjv = new Ajv({
    useDefaults: true
});

// this AJV instance is used for unmarshalling out built-in types after loading from Dynamo (e.g. number -> Date)
// in this case defaults are also used
const unMarshallingAjv = new Ajv({
    useDefaults: true
});

// this AJV instance is used for checking query values against parts of the model schema, so it needs to ignore defaults
const defaultIgnoringAjv = new Ajv({
    useDefaults: false
});

// add our internal types:
const extendedTypeKeyword = {
    keyword: 'extendedType',
    validate: function extendedTypeValidate(schema, data){
        if (schema === kExtendedTypeDate) {
            if (data instanceof Date) {
                return true;
            } else {
                extendedTypeValidate.errors = [{keyword:'extendedType', message:'must be a Date', params:{dataWas:data}}];
                return false;
            }
        } else if (schema === kExtendedTypeBuffer) {
            if (Buffer.isBuffer(data)) {
                return true;
            } else {
                extendedTypeValidate.errors = [{keyword:'extendedType', message:'must be a Buffer', params:{dataWas:data}}];
                return false;
            }
        } else {
            extendedTypeValidate.errors = [{keyword:'extendedType', message:'is an unknown extended type', params:{schemaWas:schema}}];
            return false;
        }
    }
};
ajv.addKeyword(extendedTypeKeyword);
defaultIgnoringAjv.addKeyword(extendedTypeKeyword);

// Marshalling of built-in types to dynamodb types:
marshallingAjv.addKeyword({
    keyword: 'extendedType',
    modifying: true,
    validate: function validateMarshall(schema, data, parentSchema, ctx) {
        if (schema === kExtendedTypeDate) {
            /* c8 ignore next 3 */
            if (!ctx.parentData) {
                throw new Error('Cannot marshall types that are the root data: there is no parent to assign on.');
            }
            if (data instanceof Date) {
                ctx.parentData[ctx.parentDataProperty] = data.getTime();
                return true;
            } else {
                validateMarshall.errors = [{keyword:'extendedType', message:'must be a Date', params:{dataWas:data}}];
                return false;
            }
        } else if (schema === kExtendedTypeBuffer) {
            if (Buffer.isBuffer(data)) {
                // buffers do not need marshalling (only unmarshalling from Uint8Array)
                return true;
            } else {
                validateMarshall.errors = [{keyword:'extendedType', message:'must be a Buffer', params:{dataWas:data}}];
                return false;
            }
        } else {
            validateMarshall.errors = [{keyword:'extendedType', message:'is an unknown extended type', params:{schemaWas:schema}}];
            return false;
        }
    }
});

// UnMarshalling of dynamodb types to built-in types:
unMarshallingAjv.addKeyword({
    keyword: 'extendedType',
    modifying: true,
    validate: function validateUnMarshall(schema, data, parentSchema, ctx) {
        /* c8 ignore next 3 */
        if (!ctx.parentData) {
            throw new Error('Cannot unmarshall types that are the root data: there is no parent to assign on.');
        }
        if (schema === kExtendedTypeDate) {
            if (typeof data === 'number') {
                ctx.parentData[ctx.parentDataProperty] = new Date(data);
                return true;
            } else {
                validateUnMarshall.errors = [{keyword:'extendedType', message:`Expected marshalled type of Date property ${ctx.parentDataProperty} to be a number (got ${typeof data})`, params:{dataWas:data}}];
                return false;
            }
        } else if (schema === kExtendedTypeBuffer) {
            if (data?.constructor === Uint8Array) {
                ctx.parentData[ctx.parentDataProperty] = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
                return true;
            } else {
                validateUnMarshall.errors = [{keyword:'extendedType', message:`Expected marshalled type of Buffer property ${ctx.parentDataProperty} to be a Uint8Array (got ${typeof data})`, params:{dataWas:data}}];
                return false;
            }
        } else {
            validateUnMarshall.errors = [{keyword:'extendedType', message:'is an unknown extended type', params:{schemaWas:schema}}];
            return false;
        }
    }
});

const kModelTable = Symbol();
const kModelSchema = Symbol();
const kModelLogger = Symbol();
const kTableIsReady = Symbol();
const kTableDDBClient = Symbol();
const kTableIndices = Symbol();
const kTableGetBackoffDelayMs = Symbol();
const kSchemaCompiled = Symbol('schema:compiled');
const kSchemaMarshall = Symbol('schema:marshall');
const kSchemaUnMarshall = Symbol('schema:unmarshall');
const kSchemaIndices = Symbol('schema:indices');
const kSchemaNewId = Symbol('schema:newId');
const kOptionSkipValidation = Symbol();

// Built-in schema types
const DocId = { type:'string', minLength:1, maxLength:1024 };
const Timestamp = { extendedType: kExtendedTypeDate };
const Binary = { extendedType: kExtendedTypeBuffer };
const TypeFieldType = { type:'string', minLength:1, maxLength:1024 };
const VersionFieldType = { type:'integer', minimum:0 };

// Built-in schema types that are compared by identity in order to identify special field names
const DocIdField = Object.assign({}, DocId);
const TypeField = Object.assign({}, TypeFieldType);
const VersionField = Object.assign({}, VersionFieldType);
const CreatedAtField = Object.assign({}, Timestamp);
const UpdatedAtField = Object.assign({}, Timestamp);

const delayMs = async (ms) => new Promise(resolve => setTimeout(resolve, ms));

module.exports = {
    kExtendedTypeDate,
    kExtendedTypeBuffer,

    ajv,
    marshallingAjv,
    unMarshallingAjv,
    defaultIgnoringAjv,

    kModelTable,
    kModelSchema,
    kModelLogger,
    kTableIsReady,
    kTableDDBClient,
    kTableIndices,
    kTableGetBackoffDelayMs,
    kSchemaCompiled,
    kSchemaMarshall,
    kSchemaUnMarshall,
    kSchemaIndices,
    kSchemaNewId,
    kOptionSkipValidation,

    DocId,
    Timestamp,
    Binary,

    DocIdField,
    TypeField,
    VersionField,
    CreatedAtField,
    UpdatedAtField,

    delayMs,
};

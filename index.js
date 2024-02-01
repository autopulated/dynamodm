'use strict';
// dead simple Object Document Mapper for dynamodb, supporting single-table design.
const { DynamoDBClient, CreateTableCommand, DescribeTableCommand, UpdateTableCommand, DeleteTableCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand, GetCommand, BatchGetCommand, DeleteCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const Ajv = require('ajv');
const ObjectID = require('bson-objectid');

const {inspect} = require('util');

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

// Marshalling of query values:
const marshallValue = (propSchema, value) => {
    // anything which is a date in the schema needs marshalling to a number:
    if (propSchema?.extendedType === kExtendedTypeDate) {
        return value.getTime();
    } else {
        return value;
    }
};

const indexSpecSchema = ajv.compile({
    type: 'object',
    properties: {
        'hashKey': {type:'string'},
        'sortKey': {type:'string'}
    },
    required: ['hashKey'],
    additionalProperties: false
});

const validTableName = /^[a-zA-Z0-9_.-]{3,255}$/;
const validIndexName = /^[a-zA-Z0-9_.-]{3,255}$/;

const supportedQueryConditions = new Map([['$gt','>'], ['$gte','>='], ['$lt','<'], ['$lte','<=']]);

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

// Built-in schema types that are compared by identity in order to identify special field names
const DocIdField = Object.assign({}, DocId);
const TypeField = Object.assign({}, TypeFieldType);
const CreatedAtField = Object.assign({}, Timestamp);
const UpdatedAtField = Object.assign({}, Timestamp);

const delayMs = async (ms) => new Promise(resolve => setTimeout(resolve, ms));

const deepCloneObjectsAndArrays = (v) => {
    if (Array.isArray(v)) {
        return v.map(deepCloneObjectsAndArrays);
    } else if ((v?.constructor === Object) || (v instanceof BaseModel)) {
        return Object.fromEntries(Object.entries(v).map(([k,v]) => [k, deepCloneObjectsAndArrays(v)]));
    } else {
        return v;
    }
};

const indexDescriptionsEqual = (a, b) => {
    // TODO: not comparing NonKeyAttributes here, but should be
    return a && b &&
           a.IndexName === b.IndexName &&
           a.KeySchema.length === b.KeySchema.length &&
           a.Projection.ProjectionType === b.Projection.ProjectionType &&
           a.KeySchema.every((el, i) => {
               return el.AttributeName === b.KeySchema[i].AttributeName &&
                      el.KeyType === b.KeySchema[i].KeyType;
           });
};

class Table {
    // public fields
    name = '';
    client = null;
    docClient = null;

    // private fields:
    #models = new Map();
    #idFieldName = '';
    #typeFieldName = '';
    #clientShouldBeDestroyed = false;
    #logger = null;

    // protected fields that other classes need direct access to
    [kTableIsReady] = false;
    [kTableDDBClient] = null;
    [kTableIndices] = [];

    constructor(options) {
        // TODO: The marshall options should be fixed?
        let { name, client, clientOptions } = options;

        if (!validTableName.exec(name)) {
            throw new Error(`Invalid table name "${name}": Must be between 3 and 255 characters long, and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'.`);
        }
        this.#logger = options.logger.child({table: name});
        if (!client) {
            client = new DynamoDBClient(clientOptions);
        }
        this.name = name;
        this.client = client;
        this.docClient = DynamoDBDocumentClient.from(client);
        this[kTableDDBClient] = this.docClient;
        this.#clientShouldBeDestroyed = !options.client;
    }

    // public methods:
    // wait for this table to be ready
    async ready({allowAliasedSchemas, waitForIndexes}={}) {
        if (this[kTableIsReady] && !waitForIndexes) {
            return;
        } else if (!this.client) {
            throw new Error('Connection has been destroyed.');
        }
        if (!this.#models.size) {
            throw new Error('At least one schema is required in a table.');
        }
        // check for and create indexes:
        const {idField, typeField} = this.#basicReadyChecks({allowAliasedSchemas});
        this.#idFieldName = idField;
        this.#typeFieldName = typeField;

        const requiredIndexes = this.#requiredIndexes();
        this[kTableIndices] = requiredIndexes;
        const {uniqueRequiredAttributes} = this.#checkIndexCompatibility(requiredIndexes);

        try {
            await this[kTableDDBClient].send(new CreateTableCommand({
                TableName: this.name,
                // the id field (table key), as well as any attributes referred to
                // by indexes need to be defined at table creation
                AttributeDefinitions: uniqueRequiredAttributes,
                KeySchema: [
                    { AttributeName: this.#idFieldName, KeyType: 'HASH' }
                ],
                BillingMode: 'PAY_PER_REQUEST',
                GlobalSecondaryIndexes: requiredIndexes.map(x => x.index),
            }));
        } catch (err) {
            // ResourceInUseException is only thrown if the table already exists
            if (err.name !== 'ResourceInUseException') {
                /* c8 ignore next 2 */
                throw err;
            }
        }

        let tableHasRequiredIndexes;
        let missingIndexes = [];
        let differentIndexes = [];
        let response;
        let created = false;
        do {
            tableHasRequiredIndexes = true;
            response = await this[kTableDDBClient].send(new DescribeTableCommand({TableName: this.name}));
            if (response.Table.TableStatus === 'CREATING') {
                /* c8 ignore next */
                await new Promise(resolve => setTimeout(resolve, 500));
            } else if (response.Table.TableStatus === 'ACTIVE' || response.Table.TableStatus === 'UPDATING') {
                this.#logger.info('Table %s now %s', this.name, response.Table.TableStatus);
                created = true;
            } else {
                /* c8 ignore next 2 */
                throw new Error(`Table ${this.name} status is ${response.Table.TableStatus}.`);
            }
            // check if we have all the required indexes
            for (const {index, requiredAttributes} of requiredIndexes) {
                const match = response.Table.GlobalSecondaryIndexes?.find(i => i.IndexName === index.IndexName);
                if (!match) {
                    missingIndexes.push({index, requiredAttributes});
                    tableHasRequiredIndexes = false;
                } else if (!indexDescriptionsEqual(match, index)){
                    differentIndexes.push({index, requiredAttributes});
                    tableHasRequiredIndexes = false;
                }
                // TODO: while we don't need to check for missing
                // requiredAttributes, we should check for incompatible
                // requiredAttributes returned from DescribeTableCommand I think,
                // as those could prevent index creation?
            }
        } while (!created);

        if (!tableHasRequiredIndexes) {
            await this.#updateIndexes({differentIndexes, missingIndexes, existingIndexes:response.Table.GlobalSecondaryIndexes});
        }

        if (waitForIndexes) {
            await this.#waitForIndexes();
        }

        this[kTableIsReady] = true;
    }

    // assume that the table is ready (e.g. if connecting to the DB from a
    // short-lived lambda function, and you know the table has been created
    // correctly already).
    assumeReady() {
        // still do a quick check that the registered models are compatible:
        this.#basicReadyChecks();
        // the list of indexes needs initialising for the query api
        this[kTableIndices] = this.#requiredIndexes();
        // TODO: not sure if index compatibility should be checked, this could be relatively expensive:
        this.#checkIndexCompatibility(this[kTableIndices]);
        this[kTableIsReady] = true;
    }

    async destroyConnection() {
        this[kTableIsReady] = false;
        if (this.#clientShouldBeDestroyed) {
            this.client.destroy();
            this.#clientShouldBeDestroyed = false;
        }
        this.client = this.docClient = null;
        this.#models.clear();
    }

    model(schema){
        if (!(schema instanceof Schema)) {
            throw new Error('The model schema must be a valid DynamoDM.Schema().');
        }
        // add the schema to the table's schema list and return a corresponding
        // model that can be used to create and retrieve documents,or return the
        // existing model if this schema has already been added.
        // Validating most compatibility is done in Table.ready()
        if (this.#models.has(schema)) {
            return this.#models.get(schema);
        }
        if (this[kTableIsReady]) {
            throw new Error(`Table ${this.name} ready() has been called, so more schemas cannot be added now.`);
        }

        const model = createModel({table:this, schema, logger:this.#logger});
        this.#models.set(schema, model);

        return model;
    }

    async getById(id) {
        // load a model by id only (without knowing its type in advance). The
        // type is inferred from the id, and requires the id to be of the form
        // {schemaName}.{anything}, via the Schema->Models map this.#models:
        const matchingModels = [...this.#models.entries()].filter(([s,ignored_m]) => id.startsWith(`${s.name}.`)).map(([ignored_s,m]) => m);
        if (matchingModels.length > 1) {
            throw new Error(`Table has multiple ambiguous model types for id "${id}", so it cannot be loaded generically.`);
        } else if (matchingModels.length === 0) {
            throw new Error(`Table has no matching model type for id "${id}", so it cannot be loaded.`);
        }
        return matchingModels[0].getById(id);
    }

    async deleteTable() {
        this.#logger.info({}, 'Deleting tqble %s', this.name);
        await this[kTableDDBClient].send(new DeleteTableCommand({ TableName: this.name }));
    }

    // protected methods
    [kTableGetBackoffDelayMs] = (retryNumber) => {
        // TODO, should be configurable by table options
        const exponent = 2;
        const delayRandomness = 0.75; // 0 = no jitter, 1 = full jitter
        const maxRetries = 5;
        if (retryNumber >= maxRetries) {
            throw new Error('Request failed: maximum retries exceeded.');
        }
        return (exponent ** retryNumber) * ((1-delayRandomness) + delayRandomness * Math.random());
    };

    // private methods:
    #basicReadyChecks({allowAliasedSchemas} = {}) {
        const idProps = new Set();
        const typeProps = new Set();
        const typeNames = new Map();
        for (const schema of this.#models.keys()) {
            idProps.add(schema.idFieldName);
            typeProps.add(schema.typeFieldName);
            if (!typeNames.has(schema.name)) typeNames.set(schema.name, []);
            typeNames.get(schema.name).push(schema);
        }
        if (!allowAliasedSchemas) {
            for (const [name, schemas] of typeNames) {
                if (schemas.length > 1) {
                    throw new Error(`Schemas in the same table must have unique names (${name} referrs to multiple unique schemas).`);
                }
            }
        }
        if (idProps.size > 1) {
            throw new Error(`Schemas in the same table must have the same idFieldName (encountered:${[...idProps].join(',')}).`);
        }
        if (typeProps.size > 1) {
            throw new Error(`Schemas in the same table must have the same typeFieldName (encountered:${[...typeProps].join(',')}).`);
        }
        return {
            idField: idProps.values().next().value,
            typeField: typeProps.values().next().value
        };
    }

    #requiredIndexes() {
        const requiredIndexes = [
            {
                index: {
                    // The built-in type index,
                    IndexName: 'type',
                    KeySchema: [
                      { AttributeName: this.#typeFieldName, KeyType: 'HASH' },
                      { AttributeName: this.#idFieldName,   KeyType: 'RANGE'}
                    ],
                    Projection: { ProjectionType: 'KEYS_ONLY' },
                },
                requiredAttributes: [
                    { AttributeName: this.#idFieldName,   AttributeType: 'S' },
                    { AttributeName: this.#typeFieldName, AttributeType: 'S' }
                ],
                hashKey: this.#typeFieldName,
                sortKey: this.#idFieldName
            }
        ];
        for (const schema of this.#models.keys()) {
            requiredIndexes.push(...schema[kSchemaIndices]);
        }
        return requiredIndexes;
    }

    #checkIndexCompatibility(allRequiredIndexes) {
        const uniqueRequiredAttributes = [];
        const attributeTypes = new Map();
        const indexNames = new Map();
        for (const {index, requiredAttributes} of allRequiredIndexes) {
            for (const {AttributeName, AttributeType} of requiredAttributes) {
                // store all the types we encounter for each attribute name for comparison:
                if(!attributeTypes.has(AttributeName)) {
                    attributeTypes.set(AttributeName, AttributeType);
                    uniqueRequiredAttributes.push({AttributeName, AttributeType});
                } else if (attributeTypes.get(AttributeName) !== AttributeType) {
                    let offendingSchemas = [];
                    let offendingIndexes = [];
                    let offendingDefinitions = [];
                    for (const schema of this.#models.keys()) {
                        for (const schemaIndex of schema[kSchemaIndices]) {
                            for (const attr of schemaIndex.requiredAttributes) {
                                if (attr.AttributeName === AttributeName) {
                                    offendingSchemas.push(schema.name);
                                    offendingIndexes.push(schemaIndex.index.IndexName);
                                    offendingDefinitions.push(attr.AttributeType);
                                    break;
                                }
                            }
                        }
                    }
                    throw new Error(`Schema(s) "${offendingSchemas.join(', ')}" define incompatible types (${offendingDefinitions.join(',')}) for ".${AttributeName}" in index(es) "${offendingIndexes.join(', ')}".`);
                }
            }
            if(!indexNames.has(index.IndexName)) {
                indexNames.set(index.IndexName, index);
            } else if(!indexDescriptionsEqual(index, indexNames.get(index.IndexName))) {
                let offendingSchemas = [];
                let offendingDefinitions = [];
                for (const schema of this.#models.keys()) {
                    for (const schemaIndex of schema[kSchemaIndices]) {
                        if (schemaIndex.index.IndexName === index.IndexName) {
                            offendingSchemas.push(schema.name);
                            offendingDefinitions.push(schemaIndex.index);
                            break;
                        }
                    }
                }
                throw new Error(`Schema(s) "${offendingSchemas.join(', ')}" define incompatible versions of index "${index.IndexName}".`);
            }
        }
        return {uniqueRequiredAttributes};
    }

    async #updateIndexes({differentIndexes, missingIndexes, existingIndexes}) {
        if (differentIndexes.length) {
            this.#logger.warn({existingIndexes, differentIndexes}, `WARNING: indexes "${differentIndexes.map(i => i.index.IndexName).join(',')}" differ from the current specifications, but these will not be automatically updated.`);
        }
        // FIXME: we can only add one missing index at a time, so just try to
        // add the first one. Need a createIndexes option, as this could be a
        // long wait if we need to create many? (Subscriber limit exceeded:
        // Only 1 online index can be created or deleted simultaneously per
        // table -> each update table command can only create one index....
        // "You can create or delete only one global secondary index per
        // UpdateTable operation.")
        if (missingIndexes.length) {
            const updates = {
                TableName: this.name,
                GlobalSecondaryIndexUpdates: [],
                AttributeDefinitions: []
            };
            // we only need to include the attribute definitions required by
            // the indexes being created, existing attribute definitions used
            // by other indexes do not need to be repeated:
            // FIXME see above, only adding the first missing one:
            //for (const missingIndex of missingIndexes) {
            //    updates.GlobalSecondaryIndexUpdates.push({Create: missingIndex.index});
            //    updates.AttributeDefinitions = updates.AttributeDefinitions.concat(missingIndex.requiredAttributes);
            //}
            const missingIndex = missingIndexes.shift();
            updates.GlobalSecondaryIndexUpdates.push({Create: missingIndex.index});
            updates.AttributeDefinitions = updates.AttributeDefinitions.concat(missingIndex.requiredAttributes);

            this.#logger.info({updates}, 'Updating table %s.', this.name);
            await this[kTableDDBClient].send(new UpdateTableCommand(updates));
        }
    }

    async #waitForIndexes() {
        let response;
        while (true) {
            response = await this[kTableDDBClient].send(new DescribeTableCommand({TableName: this.name}));
            // TODO: should be able to cover this actually
            /* c8 ignore else */
            if (response.Table.TableStatus === 'ACTIVE') {
                break;
            } else if (response.Table.TableStatus === 'UPDATING'){
                await new Promise(resolve => setTimeout(resolve, 500));
            } else {
                throw new Error(`Table ${this.name} status is ${response.Table.TableStatus}.`);
            }
        }
    }
}

class BaseModel {
    // public fields:
    // (none - the intention is that any name can be used for a model property name)

    // private fields:
    #modelIsNew = false;
    #logger = null;

    constructor({schema, params, options, logger}) {
        params = params ?? {};
        if (!params[schema.idFieldName]){
            params[schema.idFieldName] = schema[kSchemaNewId](params, options);
        }
        if (!params[schema.typeFieldName]){
            params[schema.typeFieldName] = schema.name;
        }
        // when creating via unmarshalling, the unmarshall process will have
        // already validated the data against the schema, so this step can be
        // skipped:
        if (!options?.[kOptionSkipValidation]) {
            const validate = schema[kSchemaCompiled];
            const valid = validate(params);
            if (!valid) {
                const e = new Error(`Document does not match schema for ${schema.name}: ${validate.errors[0]?.instancePath ?? ''} ${validate.errors[0]?.message}.`);
                e.validationErrors = validate.errors;
                throw e;
            }
        }
        this.#modelIsNew = true;
        this.#logger = logger;

        Object.assign(this, params);
        return this;
    }

    // public methods:
    async save() { return this.#save(); }
    async remove() { return this.#remove(); }
    async toObject({virtuals=true, ...otherOptions}={}) { return this.#toObject({virtuals, ...otherOptions}); }

    // public static methods:
    // options: {ConsistentRead: true, abortSignal: ...} ... dynamoDB consistent read option (defaults to false), and dynamoDB abortSignal options
    static #getById_options_validate = ajv.compile({
        type: 'object',
        properties: {
            abortSignal: {type:'object'},
            ConsistentRead: {type:'boolean'},
        },
        additionalProperties: false
    });
    static async getById(id, options={}) {
        if (!BaseModel.#getById_options_validate(options)) {
            throw new Error(`Invalid options: ${inspect(BaseModel.#getById_options_validate.errors, {breakLength:Infinity})}.`);
        }
        // get one model by its id
        // forward the derived class we were called on to the private
        // implementation (since the private implementation must be called on
        // the base class)
        return BaseModel.#getById(this, id, options);
    }

    // TODO: I don't think this should be public, but it's nice to be able to test it directly
    // options: {ConsistentRead: true, abortSignal: ...} ... dynamoDB consistent read option (defaults to false), and dynamoDB abortSignal options
    static #getByIds_options_validate = ajv.compile({
        type: 'object',
        properties: {
            abortSignal: {type:'object'},
            ConsistentRead: {type:'boolean'},
        },
        additionalProperties: false
    });
    static async getByIds(ids, options={}) {
        if(!BaseModel.#getByIds_options_validate(options)){
            throw new Error(`Invalid options: ${inspect(BaseModel.#getByIds_options_validate.errors, {breakLength:Infinity})}.`);
        }
        // get an array of models (of the same type) by id
        return BaseModel.#getByIds(this, ids, options);
    }

    // Query API
    //  * using options.abortSignal (from an AbortController) for cancellation, and passing this through to the underlying AWS command send() calls.
    //  * queryMany supports options.limit
    //  * queryMany is the preferred option, queryIterator might be added...
    //
    // implemented = x
    //
    // x async .queryOne(query, options) -> doc
    // x async .queryMany(query, options) -> [doc, ...]
    //   async .queryIterator(query, ?options) -> async iterator (doc)
    // x async .queryOneId(query, options) -> id
    // x async .queryManyIds(query, options) -> [id, ...]
    //   async .queryIteratorIds(query, ?options) -> async iterator (id)
    //   async Raw queries only support ids:
    // x async .rawQueryOneId(options) -> id
    // x async .rawQueryManyIds(options) -> [id, ...]
    // x async .rawQueryIteratorIds(options, cancelationPromise) -> async iterator (id)

    // Query API Methods:
    // For all non-ids methods a separate request is required to fetch the models, rather than just their IDs, which uses options from options.rawFetchOptions
    //
    static #rawQueryOptionsSchema = {
        type: 'object',
        properties: {
            'Limit': {type:'number'},
            'ScanIndexForward': {type:'boolean'},
            'FilterExpression': {type:'string'},
            'ExpressionAttributeNames': {type:'object'},
            'ExpressionAttributeValues': {type:'object'},
            'ExclusiveStartKey': {type:'object'}
        },
        additionalProperties: false
    };
    static #rawFetchOptionsSchema = {
        type: 'object',
        properties: {
            'ScanIndexForward': {type:'boolean'},
            'FilterExpression': {type:'string'},
            'ExpressionAttributeNames': {type:'object'},
            'ExpressionAttributeValues': {type:'object'},
            'ExclusiveStartKey': {type:'object'}
        },
        additionalProperties: false
    };
    static #queryOne_options_validate = ajv.compile({
        type: 'object',
        properties: {
            limit: {type:'number', const: 1},
            abortSignal: {type:'object'},
            startAfter: {type: 'object'},
            rawQueryOptions: this.#rawQueryOptionsSchema,
            rawFetchOptions: this.#rawFetchOptionsSchema
        },
        additionalProperties: false
    });
    // returns a model, or null
    static async queryOne(query, options={}) {
        if(!BaseModel.#queryOne_options_validate(options)){
            throw new Error(`Invalid options: ${inspect(BaseModel.#queryOne_options_validate.errors, {breakLength:Infinity})}.`);
        }
        // TODO: would be better to specialise this, but for now just use queryMany with limit:1, and rawQueryOptions.Limit: 1, note that rawQueryOptions.Limit:1 would be a bad choice if rawQueryOptions.FilterExpression is provided, so if you provide a FilterExpression also set Limit to something higher
        options = Object.assign({}, options, {limit:1, rawQueryOptions: Object.assign({Limit:1}, options?.rawQueryOptions)});
        return (await this.queryMany(query, options))[0] || null;
    }

    static #queryOneId_options_validate = ajv.compile({
        type: 'object',
        properties: {
            limit: {type:'number', const: 1},
            abortSignal: {type:'object'},
            startAfter: {type: 'object'},
            rawQueryOptions: this.#rawQueryOptionsSchema,
        },
        additionalProperties: false
    });
    // returns a model id, or null
    static async queryOneId(query, options={}) {
        if(!BaseModel.#queryOneId_options_validate(options)){
            throw new Error(`Invalid options: ${inspect(BaseModel.#queryOneId_options_validate.errors, {breakLength:Infinity})}.`);
        }
        options = Object.assign({}, options, {limit:1, rawQueryOptions: Object.assign({Limit:1}, options?.rawQueryOptions)});
        return (await this.queryManyIds(query, options))[0] || null;
    }

    static #queryMany_options_validate = ajv.compile({
        type: 'object',
        properties: {
            limit: {type:'number', default: 50},
            abortSignal: {type:'object'},
            startAfter: {type: 'object'},
            rawQueryOptions: this.#rawQueryOptionsSchema,
            rawFetchOptions: this.#rawFetchOptionsSchema
        },
        additionalProperties: false
    });
    // return an array of up to options.limit items, starting from options.startAfter
    // supported options:
    // {
    //   limit: N, limit on the total number of returned items. Distinct from
    //          the dynamoDB Limit option, which limits the number of evaluated
    //          items, the query will be paginated until either the entire
    //          result set has been evaluated or N matching items have been
    //          found. Defaults to 50;
    //
    //   abortSignal: an AbortSignal (or compatible) that can be used by the
    //                caller to interrupt in-progress operations.
    //                (https://developer.mozilla.org/en-US/docs/Web/API/AbortSignal#implementing_an_abortable_api),
    //                this will be passed as options.abortSignal to all
    //                underlying AWS SDK APIs which are used.
    //
    //   startAfter: <model-value> from which an ExclusiveStartKey is
    //               constructed. ExclusiveStartKey for the dynamodb document
    //               client is of the form {hasKeyName:hashKeyVal,
    //               sortKeyName:sortKeyVal} for the hashkey and optionally
    //               sortKey of the index being queried.
    //
    //   rawQueryOptions: {
    //      ... raw options passed to DynamoDB.QueryCommand for the query phase, e.g. ScanIndexForward: false to reverse the results
    //   }
    //   rawFetchOptions: {
    //      ... raw options passed to DynamoDB.GetItemsCommand for the fetch phase, e.g. ConsistentRead: true for strongly consistent reads
    //   }
    // }
    static async queryMany(query, options={}) {
        if(!BaseModel.#queryMany_options_validate(options)){
            throw new Error(`Invalid options: ${inspect(BaseModel.#queryMany_options_validate.errors, {breakLength:Infinity})}.`);
        }
        let {rawQueryOptions, rawFetchOptions, ...otherOptions} = options ?? {};

        // returns an array of models (possibly empty)
        const rawQuery = BaseModel.#convertQuery(this, query, Object.assign({startAfter: otherOptions.startAfter, limit: otherOptions.limmit}, rawQueryOptions));
        const pending = [];
        let errorOccurred = false;
        const setErrorOccurred = () => { errorOccurred = true; };
        // ... relying on #rawQueryIdsBatchIterator to return the right number in total, based on otherOptions.limit
        for await (const ids of BaseModel.#rawQueryIdsBatchIterator(this, rawQuery, otherOptions)) {
            if (errorOccurred) break;
            // start fetching the models from the IDs of this batch immediately, but don't await yet so requests can be parallelised
            const pendingGetByIds = BaseModel.#getByIds(this, ids, Object.assign({abortSignal: otherOptions.abortSignal}, rawFetchOptions));
            pending.push(pendingGetByIds);
            // however, we must .catch any errors, so we can fail fast (and prevent PromiseRejectionHandledWarning logs)
            pendingGetByIds.catch(setErrorOccurred);
        }
        return (await Promise.all(pending)).flat();
    }

    static #queryManyIds_options_validate = ajv.compile({
        type: 'object',
        properties: {
            limit: {type:'number', default:50},
            abortSignal: {type:'object'},
            startAfter: {type: 'object'},
            rawQueryOptions: this.#rawQueryOptionsSchema
        },
        additionalProperties: false
    });
    static async queryManyIds(query, options={}) {
        if(!BaseModel.#queryManyIds_options_validate(options)){
            throw new Error(`Invalid options: ${inspect(BaseModel.#queryManyIds_options_validate.errors, {breakLength:Infinity})}.`);
        }
        // options are as queryMany, except Ids are returned, so there are no rawFetchOptions
        let {rawQueryOptions, ...otherOptions} = options ?? {};
        otherOptions = Object.assign({limit: 50}, otherOptions);
        const rawQuery = BaseModel.#convertQuery(this, query, Object.assign({startAfter: otherOptions.startAfter, limit: otherOptions.limmit}, rawQueryOptions));
        const results = [];
        for await (const batch of BaseModel.#rawQueryIdsBatchIterator(this, rawQuery, otherOptions)) {
            results.push(batch);
        }
        return results.flat();
    }

    // TODO: I'm not sure queryIterator is a useful API, at least for the non-Ids case? The hidden internal batching isn't very useful?
    // TODO: if this api is supported, it will need to listen to the abortSignal inbetween yielding each result, in addition to passing the abortSignal the AWS sdk
    // yield all the results of this query up to options.limit (which may be undefined,==Infinity), handling continuation internally, until .return or .throw is called on the returned generator
    // static async* queryIterator(query, options) {
    // }
    // static async* queryIteratorIds(query, options) {
    // }

    static async rawQueryOneId(rawQuery, options={}) {
        return BaseModel.#rawQueryOneId(this, rawQuery, options);
    }
    static async rawQueryManyIds(rawQuery, options={}) {
        // TODO: schema for options which assigns default limit, checks types
        options = Object.assign({limit: 50}, options);
        const results = [];
        rawQuery = {
            TableName: this[kModelTable].name,
            ...rawQuery
        };
        for await (const batch of BaseModel.#rawQueryIdsBatchIterator(this, rawQuery, options)) {
            results.push(batch);
        }
        return results.flat();
    }
    static async* rawQueryIteratorIds(rawQuery, options={}) {
        options = Object.assign({limit: Infinity}, options);
        yield* BaseModel.#rawQueryIds(this, rawQuery, options);
    }

    // TODO: not sure if some sort of list-all-ids query shortcut should exist?
    // static async* listAllIds(options) { yield* BaseModel.#listAllIds(this, options); }

    // private methods:
    async #save() {
        const DerivedModel = this.constructor;
        const schema = DerivedModel[kModelSchema];
        const table  = DerivedModel[kModelTable];
        // TODO, should add a version field, and use a ConditionExpression on its equality to the current version https://stackoverflow.com/questions/46531331/how-to-prevent-a-dynamodb-item-being-overwritten-if-an-entry-already-exists
        if (!table[kTableIsReady]) {
            await table.ready();
        }
        // update timestamp fields
        const now = new Date();
        if (this.#modelIsNew && schema.createdAtFieldName) {
            this[schema.createdAtFieldName] = now;
        }
        if (schema.updatedAtFieldName) {
            this[schema.updatedAtFieldName] = now;
        }
        // check against schema, and marshal types for db:
        const properties = deepCloneObjectsAndArrays(this);
        const marshall = schema[kSchemaMarshall];
        const marshallValid = marshall(properties);
        if (!marshallValid) {
            const e = new Error(`Document does not match schema for ${schema.name}: ${marshall.errors[0]?.instancePath ?? ''} ${marshall.errors[0]?.message}.`);
            e.validationErrors = schema[kSchemaCompiled].errors;
            throw e;
        }
        // NOTE: this makes it possible to remove properties by setting .prop =
        // undefined; ajv allows .prop = undefined through the schema, but to
        // actually get the dynamodb client to delete the field we need to
        // delete .prop instead (passing undefined will cause an error like
        // "Cannot read properties of undefined (reading 'S')"):
        for (const [k, v] of Object.entries(properties)) {
            if (typeof v === 'undefined') {
                delete properties[k];
            }
        }
        // if the model is new, check that we are not saving a duplicate:
        const commandArgs = {
            TableName: table.name,
            Item: properties
        };
        if (this.#modelIsNew) {
            commandArgs.ConditionExpression = 'attribute_not_exists(#idFieldName)';
            commandArgs.ExpressionAttributeNames = { '#idFieldName': schema.idFieldName };
        }
        const command = new PutCommand(commandArgs);
        this.#logger.trace({command}, 'save %s', this.id);
        try {
            const response = await table[kTableDDBClient].send(command);
            this.#logger.trace({response}, 'save %s response', this.id);
        } catch (e) {
            if(e.name === 'ConditionalCheckFailedException') {
                throw new Error(`An item already exists with id field .${schema.idFieldName}="${this[schema.idFieldName]}"`);
            } else {
                /* c8 ignore next 2 */
                throw e;
            }
        }
        // after saving once, we're no longer new
        this.#modelIsNew = false;
        return this;
    }

    async #remove(){
        const table = this.constructor[kModelTable],
             schema = this.constructor[kModelSchema];
        // TODO, should add a version field, and use a ConditionExpression on its equaltiy to the current version https://stackoverflow.com/questions/46531331/how-to-prevent-a-dynamodb-item-being-overwritten-if-an-entry-already-exists
        if (!table[kTableIsReady]) {
            /* c8 ignore next 2 */
            await table.ready();
        }
        const command = new DeleteCommand({
            TableName: table.name,
            Key: { [schema.idFieldName]: this[schema.idFieldName] }
        });
        this.#logger.trace({command}, 'remove %s', this.id);
        const data = await table[kTableDDBClient].send(command);
        this.#logger.trace({response: data}, 'remove %s response', this.id);
        return this;
    }

    async #toObject(options) {
        const schema = this.constructor[kModelSchema];
        let r = Object.assign(Object.create(null), this);
        if (options?.virtuals !== false) {
            // include virtuals by enumerating the definition keys (even
            // enumerable virtuals are not included without doing this, as they
            // are defined on the prototype, and Object.assign only assigns
            // *own* enumerable properties)
            for (const k of Object.keys(schema.virtuals)) {
                r[k] = this[k];
            }
        }
        // run the series of schema.converters over the value. These can be
        // used (for example) to remove fields from a public api, add
        // asynchronously computed fields, or populate ids to object values:
        for (const convertor of schema.converters) {
            r = await convertor.call(this, r, options);
        }
        return r;
    }

    // private static methods:
    static #createFromMarshalled(DerivedModel, params) {
        const schema = DerivedModel[kModelSchema];
        const unmarshall = schema[kSchemaUnMarshall];
        const valid = unmarshall(params);
        if (!valid) {
            // if we've loaded a model of a different type, and there is nothing
            let e;
            if (params.type !== DerivedModel[kModelSchema].name) {
                e = new Error(`Document does not match schema for ${schema.name}. The loaded document has a different type "${params.type}", and the schema is incompatible: ${unmarshall.errors[0]?.instancePath ?? ''} ${unmarshall.errors[0]?.message}.`);
            } else {
                e = new Error(`Document does not match schema for ${schema.name}: ${unmarshall.errors[0]?.instancePath ?? ''} ${unmarshall.errors[0]?.message}.`);
            }
            e.validationErrors = unmarshall.errors;
            throw e;
        }
        const instance = new DerivedModel(params, {[kOptionSkipValidation]: true});
        // tag this instance as returned from the db, so when it is saved we do not check against overwrites
        instance.#modelIsNew = false;
        return instance;
    }

    // get an instance of this schema by id
    static async #getById(DerivedModel, id, rawOptions) {
        // only the ConsistentRead option is supported
        const { ConsistentRead, abortSignal } = rawOptions ?? {};
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        if (!table[kTableIsReady]) {
            /* c8 ignore next 2 */
            await table.ready();
        }
        if ((typeof id !== 'string') || (!id.length)) {
            throw new Error('Invalid id: must be string of nonzero length.');
        }
        const sendOptions = {
            ...(abortSignal && {abortSignal})
        };
        const command = new GetCommand(Object.assign({
            TableName: table.name,
            Key: {
                [schema.idFieldName]: id
            }
        }, ConsistentRead? {ConsistentRead} : undefined));
        DerivedModel[kModelLogger].trace({command, sendOptions}, 'getById %s', id);
        const data = await table[kTableDDBClient].send(command, sendOptions);
        DerivedModel[kModelLogger].trace({response: data}, 'getById response %s', id);
        if (!data.Item) {
            return null;
        } else {
            return this.#createFromMarshalled(DerivedModel, data.Item);
        }
    }

    // get an array of instances of this schema by id
    // At most 100 items can be fetched at one time (the limit to the dynamodb BatchGetItem request size)
    static async #getByIds(DerivedModel, ids, rawOptions) {
        // only the ConsistentRead option is supported
        const { ConsistentRead, abortSignal } = rawOptions ?? {};
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        if (!table[kTableIsReady]) {
            /* c8 ignore next 2 */
            await table.ready();
        }
        if (!Array.isArray(ids)) {
            throw new Error('Invalid ids: must be array of strings of nonzero length.');
        }
        for(const id of ids) {
            if ((typeof id !== 'string') || (!id.length)) {
                throw new Error('Invalid ids: must be array of strings of nonzero length.');
            }
        }
        const sendOptions = {
            ...(abortSignal && {abortSignal})
        };
        let Keys = ids.map(id => ({ [schema.idFieldName]: id }));
        const results = new Map();
        let retryCount = 0;
        while(Keys.length) {
            const command = new BatchGetCommand({
                RequestItems: {
                    [table.name]: Object.assign({
                        Keys,
                    }, ConsistentRead? {ConsistentRead} : undefined)
                },
            });
            DerivedModel[kModelLogger].trace({command, sendOptions}, 'getByIds %s', ids);
            const response = await table[kTableDDBClient].send(command, sendOptions);
            DerivedModel[kModelLogger].trace({response}, 'getByIds response %s', ids);
            response.Responses[table.name].forEach(data => {
                results.set(data[schema.idFieldName], data);
            });
            Keys = response?.UnprocessedKeys?.[table.name]?.Keys ?? [];
            // exponential backoff as rescommended
            // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff
            // since unprocessed keys might be caused by read capacity throttling:
            retryCount += 1;
            await delayMs(table[kTableGetBackoffDelayMs](retryCount));
        }
        // return the results by mapping the original ids, so that the results are in the same order
        return ids.map(
            id => {
                const data = results.get(id);
                return data? this.#createFromMarshalled(DerivedModel, data) : null;
            }
        );
    }

    // query for an instance of this schema, an async generator returning the
    // ids of the stored records that match the query. Use getById to get the
    // object for each ID.
    static async* #rawQueryIds(DerivedModel, rawQuery, options) {
        const {limit, abortSignal} = options?? {};
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        if (!table[kTableIsReady]) {
            /* c8 ignore next 2 */
            await table.ready();
        }
        const sendOptions = {
            ...(abortSignal && {abortSignal})
        };
        const commandParams = {
            TableName: table.name,
            ...rawQuery,
        };
        let response;
        let returned = 0;
        do {
            const command = new QueryCommand(commandParams);
            DerivedModel[kModelLogger].trace({command}, 'rawQueryIds');
            response = await table[kTableDDBClient].send(command, sendOptions);
            DerivedModel[kModelLogger].trace({response: response}, 'rawQueryIds response');

            for (const item of response.Items) {
                returned += 1;
                yield item[schema.idFieldName];
                if (returned >= limit) {
                    break;
                }
            }
            commandParams.ExclusiveStartKey = response.LastEvaluatedKey;
        } while (response.LastEvaluatedKey && returned < limit);
    }

    // query for instances of this schema, an async generator returning arrays of ids matching the query, up to options.limit.
    // rawQueryIdsBatchIterator does NOT set the TableName, unlike other #rawQuery* APIs.
    static async* #rawQueryIdsBatchIterator(DerivedModel, rawQuery, options) {
        const {limit, abortSignal} = options?? {};
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        if (!table[kTableIsReady]) {
            /* c8 ignore next 2 */
            await table.ready();
        }
        const sendOptions = {
            ...(abortSignal && {abortSignal})
        };
        let response;
        let limitRemaining = limit ?? Infinity;
        do {
            const command = new QueryCommand(rawQuery);
            DerivedModel[kModelLogger].trace({command}, 'rawQueryIdsBatch');
            response = await table[kTableDDBClient].send(command, sendOptions);
            DerivedModel[kModelLogger].trace({response: response}, 'rawQueryIdsBatch response');

            if (response.Items.length > limitRemaining) {
                yield response.Items.slice(0, limitRemaining).map(item => item[schema.idFieldName]);
                break;
            } else {
                limitRemaining -= response.Items.length;
                yield response.Items.map(item => item[schema.idFieldName]);
            }
            rawQuery.ExclusiveStartKey = response.LastEvaluatedKey;
        } while (response.LastEvaluatedKey);
    }

    static async #rawQueryOneId(DerivedModel, rawQuery, options) {
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        if (!table[kTableIsReady]) {
            /* c8 ignore next 2 */
            await table.ready();
        }
        const commandParams = {
            ...rawQuery,
            TableName: table.name,
            Limit: 1
        };
        const response = await table[kTableDDBClient].send(new QueryCommand(commandParams), options);
        return response?.Items?.[0]?.[schema.idFieldName] ?? null;
    }

    // TODO: see comment on .listAllIds
    // static async* #listAllIds(DerivedModel, options) {
    //     const schema = DerivedModel[kModelSchema];
    //     for await (const val of this.#rawQueryIds(DerivedModel, {
    //         ...options,
    //         IndexName: 'type',
    //         KeyConditionExpression: '#typeFieldName = :type',
    //         ExpressionAttributeValues: {
    //             ':type': schema.name
    //         },
    //         ExpressionAttributeNames: { '#typeFieldName': schema.typeFieldName }
    //     }, { limit: Infinity })){
    //         yield val;
    //     }
    // }

    static #queryEntries(queryObject) {
        // return {key: keyFieldName, value: queryValue, condition: '=','<','>'} based on a mongodb-like query object:
        // { key1: value1} -> {key: key1, value: value1, condition: '='}
        // { key1: {$gt: value1}} -> {key: key1, value: value1, condition: '>'}
        // { key1: {$lt: value1}} -> {key: key1, value: value1, condition: '<'}
        return Object.entries(queryObject).map( ([k,v]) => {
            if (typeof v === 'object') {
                const conditions = Object.keys(v).filter(k => k.startsWith('$'));
                if (conditions.length > 1) {
                    throw new Error(`Only a single ${[...supportedQueryConditions.keys()].join('/')} condition is supported in the simple query api.`);
                } else if (conditions.length === 1) {
                    const conditionOp = supportedQueryConditions.get(conditions[0]);
                    if (conditionOp) {
                        return {key:k, value: v[conditions[0]], condition:conditionOp};
                    } else {
                        throw new Error(`Condition "${conditions[0]}" is not supported.`);
                    }
                } else {
                    return {key:k, value:v, condition:'='};
                }
            } else {
                return {key:k, value:v, condition:'='};
            }
        });
    }

    // convert the simple mongoose-style object query (+options) into a raw query for DynamoDB for the specified model type:
    static #convertQuery(DerivedModel, query, options) {
        // TODO: the original intent of supporting merging in additional
        // ExpressionAttributeNames and ExpressionAttributeValues here was that
        // a caller could use this simple query API but provide their own raw
        // FilterExpression in options (with corresponding
        // ExpressionAttributeNames and ExpressionAttributeValues), but in
        // practise this is not useful without additional projected attributes
        // on indexes, so I'm not sure this should be supported.
        const {ExpressionAttributeNames, ExpressionAttributeValues, startAfter, ...otherOptions} = options ?? {};
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        const queryEntries = this.#queryEntries(query);

        if (queryEntries.length !== 1 && queryEntries.length !== 2) {
            // TODO: possibly in the future indexes with additional projected attributes could be supported, and additional query entries could be converted into a FilterExpression
            throw new Error(`Unsupported query: "${inspect(query, {breakLength:Infinity})}" Queries must have at most two properties to match against index hash and range attributes.`);
        }
        // check all the indexes for ones that include all of the query entries:
        let matchingIndexes = table[kTableIndices].filter(index => {
            if (queryEntries.length === 1) {
                return index.hashKey === queryEntries[0].key;
            } else {
                if ((index.hashKey === queryEntries[0].key && queryEntries[0].condition === '=') && index.sortKey === queryEntries[1].key) {
                    return true;
                } else if ((index.hashKey === queryEntries[1].key && queryEntries[1].condition === '=') && index.sortKey === queryEntries[0].key) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        if (!matchingIndexes.length) {
            throw new Error(`Unsupported query: "${inspect(query, {breakLength:Infinity})}". No index found for query fields [${queryEntries.map(x => x.key).join(', ')}]`);
        } else if (matchingIndexes.length > 1) {
            DerivedModel[kModelLogger].warn({matchingIndexes, query}, 'multiple indexes match query');
            if (queryEntries.length === 1) {
                // if we only have a hash key to query by, prefer indexes that only have a hash key
                const bestIndexes = matchingIndexes.filter(index => !index.sortKey);
                if (bestIndexes.length) {
                    matchingIndexes = bestIndexes;
                }
            }
        }
        const index = matchingIndexes[0];

        for (const entry of queryEntries){
            // check query values against schema, and marshall:
            const keySchema = schema.source.properties[entry.key];
            if (keySchema) {
                const valid = defaultIgnoringAjv.validate(keySchema, entry.value);
                if (!valid) {
                    const e = new Error(`Value does not match schema for ${entry.key}: ${defaultIgnoringAjv.errors[0]?.instancePath ?? ''} ${defaultIgnoringAjv.errors[0]?.message}.`);
                    e.validationErrors = defaultIgnoringAjv.errors;
                    throw e;
                }
            }
            entry.value = marshallValue(keySchema, entry.value);
        }
        let ExclusiveStartKey;
        if (startAfter) {
            if (!(startAfter instanceof DerivedModel)) {
                throw new Error(`options.startAfter must be a ${DerivedModel.name} model instance. To specify ExclusiveStartKey directly use options.rawQueryOptions.ExclusiveStartKey instead.`);
            }
            // The ExclusiveStartKey is composed of the GSI hash key, the GSI range key (if it exists), the table hash key
            ExclusiveStartKey = {
                // this will be the table's hash key. Table range key is currently not supported:
                [schema.idFieldName]: options.startAfter[schema.idFieldName],
                [index.hashKey]: options.startAfter[index.hashKey],
                ...(index.sortKey && {[index.sortKey]: options.startAfter[index.sortKey]}),
            };
        }
        const KeyConditionExpression    = queryEntries.map((v,i) => `#fieldName${i} ${v.condition} :fieldValue${i}`).join(' AND ');
        const mergedExprAttributeValues = Object.fromEntries(queryEntries.map(({value},i) => [`:fieldValue${i}`, value]).concat(Object.entries(ExpressionAttributeValues ?? {})));
        const mergedExprAttributeNames  = Object.fromEntries(queryEntries.map(({key},i) => [`#fieldName${i}`, key]).concat(Object.entries(ExpressionAttributeNames ?? {})));

        return Object.assign(Object.create(null), {
            IndexName: index.index.IndexName,
            TableName: table.name,
            KeyConditionExpression,
            ExpressionAttributeValues: mergedExprAttributeValues,
            ExpressionAttributeNames: mergedExprAttributeNames,
            ...(ExclusiveStartKey && {ExclusiveStartKey}),
            // TODO: not sure if this will mess up pagination?
            // set the dynamodb Limit to the options limit, or the maximum supported, so that we don't evaluate more items than necessary
            // ...(options.limit && {Limit:options.limit})
        }, otherOptions);
    }
}

const createModel = function({table, schema, logger}) {
    // create a unique class for this type, all the functionality is implemented in the base class
    const childLogger = logger.child({model: schema.name});
    class Model extends BaseModel {
        // public static fields
        static table = table;
        static name  = schema.name;

        // protected (accessed by base class) static fields:
        static [kModelTable]  = table;
        static [kModelSchema] = schema;
        static [kModelLogger] = childLogger;

        constructor(params, options){
            super({schema, params, options, logger: childLogger});
        }
    }
    // override the name:
    Object.defineProperty(Model.prototype.constructor, 'name', {value: `Model_${schema.name}`});

    // TODO: more comprehensive reserved name list
    if (schema.methods.constructor) {
        throw new Error('The name \'constructor\' is reserved and cannot be used for a method.');
    }

    Object.assign(Model, schema.statics);
    Object.assign(Model.prototype, schema.methods);
    // generate getters and setters for virtuals:
    Object.defineProperties(Model.prototype, generateGettersAndSetters(schema));

    // check that converters are all valid:
    for (const [i, f] of schema.converters.entries()) {
        if (typeof f !== 'function') {
            throw new Error(`Converters must be functions or async functions: typeof converters[${i}] is ${typeof f}.`);
        }
    }

    return Model;
};

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
        const { index, generateId } = options ?? {};
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

        if (schemaSource?.type && schemaSource?.type !== 'object') {
            throw new Error('Schema type must be object (or can be omitted).');
        }

        const schemaProperties = Object.assign(
            Object.create(null),
            schemaSourceProps,
            // ensure optional required fields are present in schema:
            {
                [this.idFieldName]: DocIdField,
                [this.typeFieldName]: TypeField
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

// generate getter and setter functions for a Model's prototype from schema.virtuals:
function generateGettersAndSetters(schema) {
    const descriptors = { };
    for (const [k,v] of Object.entries(schema.virtuals)) {
        if (typeof v === 'string') {
            // a simple aliased property, check that the aliased name exists:
            if (!schema.source.properties[v]) {
                throw new Error(`Virtual property "${k}" is an alias for an unknown property "${v}".`);
            }
            descriptors[k] = {
                get() {
                    return this[v];
                },
                set(newValue) {
                    this[v] = newValue;
                }
            };
        } else if (typeof v === 'object') {
            // otherwise we should have a data descriptor or accessor descriptor that should be passed to defineProperties directly:
            for (const d of Object.keys(v)) {
                if (['configurable', 'enumerable', 'value', 'writable', 'get', 'set'].indexOf(d) === -1) {
                    throw new Error(`Virtual property "${k}" invalid descriptor key "${d}" is not one of 'configurable', 'enumerable', 'value', 'writable', 'get' or 'set'.`);
                }
            }
            descriptors[k] = Object.assign(Object.create(null), v);
            // if we have a getter but no setter, add a setter which throws
            if (descriptors[k].get && !descriptors[k].set) {
                descriptors[k].set = function(){
                    throw new Error(`Virtual property "${schema.name}.${k}" cannot be assigned.`);
                };
            }
        } else {
            throw new Error(`Virtual property "${k}" must be a string alias, or a data descriptor or accessor descriptor.`);
        }
    }
    return descriptors;
}


const createLogger = (loggingOptions) => {
    if (loggingOptions){
        loggingOptions = (typeof loggingOptions === 'object')? loggingOptions : {};
        return require('pino')(loggingOptions);
    } else {
        const logger = require('abstract-logging');
        logger.child = () => logger;
        return logger;
    }
};

function DynamoDM(options) {
    // global cross-table state:
    const logger = createLogger(options?.logger);
    const defaultOptions = Object.assign(Object.create(null), options, { logger });
    const PublicAPI = {
        // Public API
        // Create a schema, merging in default options:
        Schema: (name, schemaSource, schemaOptions={}) => {
            return new Schema(name, schemaSource, Object.assign(Object.create(null), defaultOptions, schemaOptions));
        },

        // Create a table, merging in default options:
        Table: (tableOptions={}) => {
            return new Table(Object.assign(Object.create(null), defaultOptions, tableOptions));
        },

        // Special Schema types
        DocId,
        Timestamp,
        Binary,

        // Special Schema fields
        DocIdField,
        TypeField,
        CreatedAtField,
        UpdatedAtField,
    };
    return PublicAPI;
}

module.exports = DynamoDM;
module.exports.DynamoDM = DynamoDM;
module.exports.default = DynamoDM;

// provide a helpful error message if someone forgets to call the API generator:
module.exports.Table = module.exports.Schema = function incorrectUsage(){
    throw new Error("DynamoDM must be called as a function to get an instance of the API, e.g. const DynamoDM = require('dynamodm')(options);");
};

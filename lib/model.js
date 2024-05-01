'use strict';
const { inspect } = require('util');
const { PutCommand, GetCommand, BatchGetCommand, DeleteCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');

const {
    kExtendedTypeDate,

    ajv, defaultIgnoringAjv,


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
    kSchemaNewId,
    kOptionSkipValidation,

    delayMs,
} = require('./shared.js');


const kConditionEqual = Symbol('=');
const kConditionLT  = Symbol('<');
const kConditionLTE = Symbol('<=');
const kConditionGT  = Symbol('>');
const kConditionGTE = Symbol('>=');
const kConditionBetween = Symbol('between');
const kConditionBegins = Symbol('begins');
const kBatchGetItemLimit = 100;

const supportedQueryConditions = new Map([
    // dynamodm query condition => [internal identifier, number of arguments required]
    ['$gt', [kConditionGT, 1]],
    ['$gte', [kConditionGTE, 1]],
    ['$lt', [kConditionLT, 1]],
    ['$lte', [kConditionLTE, 1]],
    ['$between', [kConditionBetween, 2]],
    ['$begins', [kConditionBegins, 1]]
]);

// Marshalling of query values:
const marshallValue = (propSchema, value) => {
    // anything which is a date in the schema needs marshalling to a number:
    if (propSchema?.extendedType === kExtendedTypeDate) {
        return value.getTime();
    } else {
        return value;
    }
};

const deepCloneObjectsAndArrays = (v) => {
    if (Array.isArray(v)) {
        return v.map(deepCloneObjectsAndArrays);
    } else if ((v?.constructor === Object) || (v instanceof BaseModel)) {
        return Object.fromEntries(Object.entries(v).map(([k,v]) => [k, deepCloneObjectsAndArrays(v)]));
    } else {
        return v;
    }
};

// generate getter and setter functions for a Model's prototype from schema.virtuals:
const generateGettersAndSetters = function(schema) {
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
};

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
        if (schema.versionFieldName && !params[schema.versionFieldName]) {
            params[schema.versionFieldName] = 0;
        }
        // when creating via unmarshalling, the unmarshall process will have
        // already validated the data against the schema, so this step can be
        // skipped:
        if (!options?.[kOptionSkipValidation]) {
            const validate = schema[kSchemaCompiled];
            const valid = validate(params);
            if (!valid) {
                const e = new Error(`Document does not match schema for ${schema.name}: ${validate.errors[0].instancePath} ${validate.errors[0].message}.`);
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
    static #abortSignalSchema = {
        type: 'object',
        apiArgument: {
            validate: (data) => (typeof data.aborted === 'boolean') && (typeof data.addEventListener === 'function'),
            error: 'Must be an AbortController Signal.'
        }
    };
    static #getById_options_validate = ajv.compile({
        type: 'object',
        properties: {
            abortSignal: this.#abortSignalSchema,
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
            abortSignal: this.#abortSignalSchema,
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
            abortSignal: this.#abortSignalSchema,
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
            abortSignal: this.#abortSignalSchema,
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
            abortSignal: this.#abortSignalSchema,
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
        let {rawQueryOptions, rawFetchOptions, ...otherOptions} = options;

        // returns an array of models (possibly empty)
        const rawQuery = BaseModel.#convertQuery(this, query, Object.assign({startAfter: otherOptions.startAfter, limit: otherOptions.limit}, rawQueryOptions));
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
            abortSignal: this.#abortSignalSchema,
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
        let {rawQueryOptions, ...otherOptions} = options;
        otherOptions = Object.assign({limit: 50}, otherOptions);
        const rawQuery = BaseModel.#convertQuery(this, query, Object.assign({startAfter: otherOptions.startAfter, limit: otherOptions.limit}, rawQueryOptions));
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
            const e = new Error(`Document does not match schema for ${schema.name}: ${marshall.errors[0].instancePath} ${marshall.errors[0].message}.`);
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
        const commandArgs = {
            TableName: table.name,
            Item: properties
        };
        if (this.#modelIsNew) {
            // if the model is new, check that we are not saving a duplicate:
            commandArgs.ConditionExpression = 'attribute_not_exists(#idFieldName)';
            commandArgs.ExpressionAttributeNames = { '#idFieldName': schema.idFieldName };
            if(schema.versionFieldName) {
                properties[schema.versionFieldName] = 1;
            }
        } else if (schema.versionFieldName){
            const previousVersion = properties[schema.versionFieldName];
            if (!properties[schema.versionFieldName]) {
                this.#logger.warn('Adding missing version field %s to document %s.', schema.versionFieldName, this.id);
                properties[schema.versionFieldName] = 1;
                // we can still make sure that another process is not adding the version field in parallel:
                commandArgs.ConditionExpression = 'attribute_not_exists(#v)';
                commandArgs.ExpressionAttributeNames = { '#v': schema.versionFieldName };
            } else {
                properties[schema.versionFieldName] += 1;
                // otherwise, check that the version field is the same as it was when we loaded this model. Each save increments the version.
                commandArgs.ConditionExpression = '#v = :v';
                commandArgs.ExpressionAttributeNames = { '#v': schema.versionFieldName };
                commandArgs.ExpressionAttributeValues = { ':v': previousVersion };
            }
        }
        const command = new PutCommand(commandArgs);
        this.#logger.trace({command}, 'save %s', this.id);
        try {
            const response = await table[kTableDDBClient].send(command);
            this.#logger.trace({response}, 'save %s response', this.id);
        } catch (e) {
            if(e.name === 'ConditionalCheckFailedException') {
                if (this.#modelIsNew) {
                    throw new Error(`An item already exists with id field .${schema.idFieldName}="${this[schema.idFieldName]}"`);
                } else {
                    throw new Error(`Version error: the model .${schema.idFieldName}="${this[schema.idFieldName]}" was updated by another process between loading and saving.`);
                }
                /* c8 ignore next 3 */
            } else {
                throw e;
            }
        }
        // after saving once, we're no longer new
        this.#modelIsNew = false;
        if (schema.versionFieldName) {
            // and increment the visible version
            this[schema.versionFieldName] = properties[schema.versionFieldName];
        }
        return this;
    }

    async #remove(){
        const table = this.constructor[kModelTable],
             schema = this.constructor[kModelSchema];
        /* c8 ignore next 3 */
        if (!table[kTableIsReady]) {
            await table.ready();
        }
        const commandArgs = {
            TableName: table.name,
            Key: { [schema.idFieldName]: this[schema.idFieldName] }
        };
        // check that the version field is the same as it was when we loaded this model.
        if (schema.versionFieldName) {
            if (!this[schema.versionFieldName]) {
                this.#logger.warn('Removing versioned document .%s="%s" missing version field.', schema.versionFieldName, this.id);
                commandArgs.ConditionExpression = 'attribute_not_exists(#v)';
                commandArgs.ExpressionAttributeNames = { '#v': schema.versionFieldName };
            } else {
                commandArgs.ConditionExpression = '#v = :v';
                commandArgs.ExpressionAttributeNames = { '#v': schema.versionFieldName };
                commandArgs.ExpressionAttributeValues = { ':v': this[schema.versionFieldName] };
            }
        }
        const command = new DeleteCommand(commandArgs);
        this.#logger.trace({command}, 'remove %s', this.id);
        let data;
        try {
            data = await table[kTableDDBClient].send(command);
        } catch (e) {
            if (e.name === 'ConditionalCheckFailedException') {
                throw new Error(`Version error: the model .${schema.idFieldName}="${this[schema.idFieldName]}" was updated by another process between loading and removing.`);
                /* c8 ignore next 3 */
            } else {
                throw e;
            }
        }
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
                e = new Error(`Document does not match schema for ${schema.name}. The loaded document has a different type "${params.type}", and the schema is incompatible: ${unmarshall.errors[0].instancePath} ${unmarshall.errors[0].message}.`);
            } else {
                e = new Error(`Document does not match schema for ${schema.name}: ${unmarshall.errors[0].instancePath} ${unmarshall.errors[0].message}.`);
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
        const { ConsistentRead, abortSignal } = rawOptions;
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        /* c8 ignore next 3 */
        if (!table[kTableIsReady]) {
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
    static async #getByIds(DerivedModel, ids, rawOptions) {
        const { ConsistentRead, abortSignal } = rawOptions;
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        /* c8 ignore next 3 */
        if (!table[kTableIsReady]) {
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
        let keysExceedingLimit;
        // At most kBatchGetItemLimit (100) items can be fetched at one time
        // (the limit to the dynamodb BatchGetItem request size), so if
        // rawOptions.limit is greater than this, batch the request.
        keysExceedingLimit = Keys.slice(kBatchGetItemLimit);
        Keys = Keys.slice(0, kBatchGetItemLimit);
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
            if (Keys.length) {
                // exponential backoff as recommended
                // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff
                // since unprocessed keys might be caused by read capacity throttling:
                retryCount += 1;
                await delayMs(table[kTableGetBackoffDelayMs](retryCount));
            }
            // if there's any room after the unprocessed keys from the
            // response, request some of the keys that haven't been requested
            // yet as well:
            const spaceAvailable = kBatchGetItemLimit - Keys.length;
            Keys = Keys.concat(keysExceedingLimit.slice(0, spaceAvailable));
            keysExceedingLimit = keysExceedingLimit.slice(spaceAvailable);
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
        const {limit, abortSignal} = options;
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        /* c8 ignore next 3 */
        if (!table[kTableIsReady]) {
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
        const {limit, abortSignal} = options;
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        /* c8 ignore next 3 */
        if (!table[kTableIsReady]) {
            await table.ready();
        }
        const sendOptions = {
            ...(abortSignal && {abortSignal})
        };
        let response;
        let limitRemaining = limit;
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
        /* c8 ignore next 3 */
        if (!table[kTableIsReady]) {
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
        // return {key: keyFieldName, values: [queryValue], condition: '$eq','$lt','$gt'} based on a mongodb-like query object:
        // { key1: value1} -> {key: key1, values: [value1], condition: '$eq'}
        // { key1: {$gt: value1}} -> {key: key1, values: [value1], condition: '$gt'}
        // { key1: {$lt: value1}} -> {key: key1, values: pvalue1], condition: '$lt'}
        // { key1: {$between: [v1, v2]}} -> {key: key1, values: [v1, v2], condition: '$between'}
        return Object.entries(queryObject).map( ([k,v]) => {
            if (typeof v === 'object') {
                const conditions = Object.keys(v).filter(k => k.startsWith('$'));
                if (conditions.length > 1) {
                    throw new Error(`Only a single ${[...supportedQueryConditions.keys()].join('/')} condition is supported in the simple query api.`);
                } else if (conditions.length === 1) {
                    const conditionOp = supportedQueryConditions.get(conditions[0]);
                    if (conditionOp) {
                        if (conditionOp[1] === 1) {
                            // single value condition
                            return {key:k, values: [v[conditions[0]]], condition: conditionOp[0]};
                        } else {
                            // multiple value condition (values should be an array)
                            if ((!Array.isArray(v[conditions[0]])) || v[conditions[0]].length !== conditionOp[1]) {
                                throw new Error(`Condition "${conditions[0]}" in query requires an array of ${conditionOp[1]} values.`);
                            }
                            return {key:k, values: v[conditions[0]], condition: conditionOp[0]};
                        }
                    } else {
                        throw new Error(`Condition "${conditions[0]}" is not supported. Supported conditions are: ${[...supportedQueryConditions.keys()].join(', ')}.`);
                    }
                } else {
                    return {key:k, values:[v], condition: kConditionEqual};
                }
            } else {
                return {key:k, values:[v], condition: kConditionEqual};
            }
        });
    }

    static #keyConditionExpressionForQueryEntry({condition}, i) {
        if (condition === kConditionEqual) {
            return `#n${i} = :v${i}x0`;
        } else if (condition === kConditionLT) {
            return `#n${i} < :v${i}x0`;
        } else if (condition === kConditionLTE) {
            return `#n${i} <= :v${i}x0`;
        } else if (condition === kConditionGT) {
            return `#n${i} > :v${i}x0`;
        } else if (condition === kConditionGTE) {
            return `#n${i} >= :v${i}x0`;
        } else if (condition === kConditionBetween) {
            return `#n${i} BETWEEN :v${i}x0 AND :v${i}x1`;
        } else if (condition === kConditionBegins) {
            return `begins_with(#n${i}, :v${i}x0)`;
            // (not reachable)
            /* c8 ignore next 3 */
        } else {
            throw new Error('Unsupported query condition.');
        }
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
        const {ExpressionAttributeNames, ExpressionAttributeValues, startAfter, ...otherOptions} = options;
        const table = DerivedModel[kModelTable];
        const schema = DerivedModel[kModelSchema];
        const queryEntries = this.#queryEntries(query);

        if (queryEntries.length !== 1 && queryEntries.length !== 2) {
            // TODO: possibly in the future indexes with additional projected attributes could be supported, and additional query entries could be converted into a FilterExpression
            throw new Error(`Unsupported query: "${inspect(query, {breakLength:Infinity})}" Queries must have at most two properties to match against index hash and range attributes.`);
        }
        if (!queryEntries.some(e => e.condition === kConditionEqual)) {
            throw new Error(`Unsupported query: "${inspect(query, {breakLength:Infinity})}" Queries must include an equality condition for the index hash key.`);
        }
        // check all the indexes for ones that include all of the query entries:
        let matchingIndexes = table[kTableIndices].filter(index => {
            if (queryEntries.length === 1) {
                return index.hashKey === queryEntries[0].key;
            } else {
                if ((index.hashKey === queryEntries[0].key && queryEntries[0].condition === kConditionEqual) && index.sortKey === queryEntries[1].key) {
                    return true;
                } else if ((index.hashKey === queryEntries[1].key && queryEntries[1].condition === kConditionEqual) && index.sortKey === queryEntries[0].key) {
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
                for (const v of entry.values) {
                    const valid = defaultIgnoringAjv.validate(keySchema, v);
                    if (!valid) {
                        const e = new Error(`Value does not match schema for ${entry.key}: ${defaultIgnoringAjv.errors[0].instancePath} ${defaultIgnoringAjv.errors[0].message}.`);
                        e.validationErrors = defaultIgnoringAjv.errors;
                        throw e;
                    }
                }
            }
            entry.values = entry.values.map(v => marshallValue(keySchema, v));
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

        const KeyConditionExpression = queryEntries.map(this.#keyConditionExpressionForQueryEntry).join(' AND ');
        const mergedExprAttributeValues = Object.fromEntries(
            queryEntries.map(
                ({values},i) => values.map( (v,j) => [`:v${i}x${j}`, v])
            ).flat()
            .concat(Object.entries(ExpressionAttributeValues ?? {}))
        );
        const mergedExprAttributeNames  = Object.fromEntries(queryEntries.map(({key},i) => [`#n${i}`, key]).concat(Object.entries(ExpressionAttributeNames ?? {})));

        return Object.assign(Object.create(null), {
            IndexName: index.index.IndexName,
            TableName: table.name,
            KeyConditionExpression,
            ExpressionAttributeValues: mergedExprAttributeValues,
            ExpressionAttributeNames: mergedExprAttributeNames,
            ...(ExclusiveStartKey && {ExclusiveStartKey}),
            // set the dynamodb Limit to the options limit, so that we don't
            // evaluate more items than necessary. If making a query with a
            // filter expression it may make sense to specify a larger limit,
            // in which case that can be specified in otherOptions.Limit
            ...(options.limit && {Limit: options.limit})
        }, otherOptions);
    }
}

const createModel = function({table, schema, logger}) {
    // create a unique class for this type, all the functionality is implemented in the base class
    const childLogger = logger.child({model: schema.name});
    class Model extends BaseModel {
        // public static fields
        static table = table;
        static type = schema.name;

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

module.exports = {
    createModel
};

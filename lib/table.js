'use strict';
const { DynamoDBClient, CreateTableCommand, DescribeTableCommand, UpdateTableCommand, DeleteTableCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');

const { createModel } = require('./model');
const {
    kTableIsReady,
    kTableDDBClient,
    kTableIndices,
    kTableGetBackoffDelayMs,

    kSchemaIndices,

    delayMs,
} = require('./shared.js');

const validTableName = /^[a-zA-Z0-9_.-]{3,255}$/;

const keySchemaEqual = (a, b) => {
    return a.AttributeName === b.AttributeName &&
           a.KeyType === b.KeyType;
};

const indexDescriptionsEqual = (a, b) => {
    // TODO: not comparing NonKeyAttributes here, but should be
    return a && b &&
           a.IndexName === b.IndexName &&
           a.KeySchema.length === b.KeySchema.length &&
           a.Projection.ProjectionType === b.Projection.ProjectionType &&
           a.KeySchema.every((el, i) => keySchemaEqual(el, b.KeySchema[i]));
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
    #retryOptions = {
        exponent:2,
        delayRandomness:0.75, // 0 = no jitter, 1 = full jitter
        maxRetries:5
    };

    // protected fields that other classes need direct access to
    [kTableIsReady] = false;
    [kTableDDBClient] = null;
    [kTableIndices] = [];

    constructor(options) {
        // TODO: The marshall options should be fixed?
        let { name, client, clientOptions, retry } = options;

        if (typeof name !== 'string') {
            throw new Error('Invalid table name: Must be a string.');
        } else if(!validTableName.exec(name)) {
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
        this.#retryOptions = Object.assign(this.#retryOptions, retry);
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
        const tableKeySchema = [
            { AttributeName: this.#idFieldName, KeyType: 'HASH' }
        ];

        try {
            await this[kTableDDBClient].send(new CreateTableCommand({
                TableName: this.name,
                // the id field (table key), as well as any attributes referred to
                // by indexes need to be defined at table creation
                AttributeDefinitions: uniqueRequiredAttributes,
                KeySchema: tableKeySchema,
                BillingMode: 'PAY_PER_REQUEST',
                ...(requiredIndexes.length && {GlobalSecondaryIndexes: requiredIndexes.map(x => x.index)})
            }));
        } catch (err) {
            // ResourceInUseException is only thrown if the table already exists
            /* c8 ignore next 3 */
            if (err.name !== 'ResourceInUseException') {
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
                await delayMs(500);
            } else if (response.Table.TableStatus === 'ACTIVE' || response.Table.TableStatus === 'UPDATING') {
                this.#logger.info('Table %s now %s', this.name, response.Table.TableStatus);
                created = true;
                /* c8 ignore next 3 */
            } else {
                throw new Error(`Table ${this.name} status is ${response.Table.TableStatus}.`);
            }

            // check if the table has the correct key schema (if it already
            // existed, then it might not):
            if (created) {
                if (!response.Table.KeySchema.some((el, i) => keySchemaEqual(el, tableKeySchema[i]))) {
                    throw new Error(`Table ${this.name} exists with incompatible key schema ${JSON.stringify(response.Table.KeySchema)}, the schemas require "${this.#idFieldName}" to be the hash key.`);
                }
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
            await this.#updateIndexes({differentIndexes, missingIndexes, existingIndexes: response.Table.GlobalSecondaryIndexes, createAll: waitForIndexes});
        }

        if (waitForIndexes) {
            await this.#waitForIndexesActive();
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
        // Not checking using 'instanceof' here, because the Schema might come
        // from a different realm, and we want to allow that.
        if (!(schema && schema.name && schema.idFieldName && schema.source && schema.methods)) {
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
        if (retryNumber >= this.#retryOptions.maxRetries) {
            throw new Error('Request failed: maximum retries exceeded.');
        }
        return (this.#retryOptions.exponent ** retryNumber) * ((1-this.#retryOptions.delayRandomness) + this.#retryOptions.delayRandomness * Math.random());
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
        const requiredIndexes = [];
        // Only require the type index if we have multiple schemas in this
        // table, to support single-model tables mode efficiently:
        if (this.#models.size > 1) {
            requiredIndexes.push({
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
            });
        }
        for (const schema of this.#models.keys()) {
            requiredIndexes.push(...schema[kSchemaIndices]);
        }
        return requiredIndexes;
    }

    #checkIndexCompatibility(allRequiredIndexes) {
        const uniqueRequiredAttributes = [];
        const attributeTypes = new Map();
        const indexNames = new Map();

        // The table hash key is always the ID field, and while it is not an
        // index field, it is a required attribute type that we need to check
        // compatibility with:
        allRequiredIndexes = [{
            requiredAttributes: [
                { AttributeName: this.#idFieldName, AttributeType: 'S' },
            ]
        }].concat(allRequiredIndexes);
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
            if (index) {
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
        }
        return {uniqueRequiredAttributes};
    }

    async #updateIndexes({differentIndexes, missingIndexes, existingIndexes, createAll}) {
        if (differentIndexes.length) {
            this.#logger.warn({existingIndexes, differentIndexes}, `WARNING: indexes "${differentIndexes.map(i => i.index.IndexName).join(',')}" differ from the current specifications, but these will not be automatically updated.`);
        }
        if (missingIndexes.length) {
            // Only one index can be added at a time:
            for (const missingIndex of missingIndexes) {
                const updates = {
                    TableName: this.name,
                    GlobalSecondaryIndexUpdates: [],
                    AttributeDefinitions: []
                };
                // we only need to include the attribute definitions required by
                // the indexes being created, existing attribute definitions used
                // by other indexes do not need to be repeated:
                updates.GlobalSecondaryIndexUpdates.push({Create: missingIndex.index});
                updates.AttributeDefinitions = updates.AttributeDefinitions.concat(missingIndex.requiredAttributes);

                this.#logger.info({updates}, 'Updating table %s.', this.name);
                await this[kTableDDBClient].send(new UpdateTableCommand(updates));

                if (createAll) {
                    await this.#waitForIndexesActive();
                } else {
                    break;
                }
            }
        }
    }

    async #waitForIndexesActive() {
        while (true) {
            const response = await this[kTableDDBClient].send(new DescribeTableCommand({TableName: this.name}));
            if (response.Table.TableStatus === 'ACTIVE' &&
                (response.Table.GlobalSecondaryIndexes || []).every(gsi => gsi.IndexStatus === 'ACTIVE')) {
                break;
            } else if (['UPDATING', 'ACTIVE'].includes(response.Table.TableStatus) &&
                (response.Table.GlobalSecondaryIndexes || []).every(gsi => ['CREATING', 'UPDATING', 'ACTIVE'].includes(gsi.IndexStatus))) {
                await delayMs(500);
            } else {
                throw new Error(`Table ${this.name} status is ${response.Table.TableStatus}, index statuses are [${(response.Table.GlobalSecondaryIndexes || []).map(gsi => gsi.IndexStatus).join(', ')}].`);
            }
        }
    }
}

module.exports = {
    Table
};

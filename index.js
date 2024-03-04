'use strict';
// dead simple Object Document Mapper for dynamodb, supporting single-table design.
const { Table } = require('./lib/table.js');
const { Schema } = require('./lib/schema.js');

const {
    DocId, Timestamp, Binary,
    DocIdField, TypeField, CreatedAtField, UpdatedAtField
} = require('./lib/shared.js');

const createLogger = (loggingOptions) => {
    if (typeof loggingOptions?.child === 'function') {
        // if we were passed an existing pino logger (or abstract logger), or
        // something that looks like one, create a child logger:
        return loggingOptions.child({module:'dynamodm'});
    } else if (loggingOptions){
        // otherwise if logging options have been passed, create a new pino
        // instance:
        loggingOptions = (typeof loggingOptions === 'object')? loggingOptions : {};
        return require('pino')(loggingOptions);
    } else {
        // otherwise logging is completely disabled
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
        Table: (name, tableOptions={}) => {
            // support both Table({name:...}) and Table(name, options);
            if (typeof name === 'object') {
                tableOptions = name;
            } else {
                tableOptions = Object.assign({name}, tableOptions);
            }
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

## DynamoDM: Simple Document Mapper for DynamoDB

[![CI](https://github.com/autopulated/DynamoDM/actions/workflows/test.yml/badge.svg)](https://github.com/autopulated/DynamoDM/actions/workflows/test.yml)
[![Coverage](https://coveralls.io/repos/github/autopulated/dynamodm/badge.svg?branch=main)](https://coveralls.io/github/autopulated/dynamodm?branch=main)
[![NPM version](https://img.shields.io/npm/v/dynamodm.svg?style=flat)](https://www.npmjs.com/package/dynamodm)

## Quickstart
```js
import DynamoDM from 'dynamodm'

// get an instance of the API (options can be passed here)
const ddm = DynamoDM()

// get a reference to a table:
const table = ddm.Table('my-dynamodb-table')

// Create User and Comment models with their JSON schemas in this table:
const UserSchema = ddm.Schema('user', {
    properties: {
        emailAddress: {type: 'string'},
        marketingComms: {type: 'boolean', default: false}
    },
})

const CommentSchema = ddm.Schema('c', {
    properties: {
        text: {type: 'string' },
        user: ddm.DocId,
        // identify a field to be used as the creation timestamp using a
        // built-in schema:
        createdAt: ddm.CreatedAtField
    },
    additionalProperties: true
}, {
    // The schema also defines the indexes (GSI) that this model needs:
    index: {
        findByUser: {
            hashKey: 'user',
            sortKey: 'createdAt'
        }
    }
})

const User = table.model(UserSchema)
const Comment = table.model(CommentSchema)

// wait for the table to be ready, all models should be added first.
await table.ready()

// create some documents (instances of models):
const aUser = new User({ emailAddress: "friend@example.com" })
await aUser.save()

const aComment = new Comment({ user: aUser.id, text: "My first comment." })
await aComment.save()

// query for some documents:
const commentsForUser = await Comment.queryMany({ user: aUser.id })
```

## Even Quicker Start: Just Save and Load Documents Without Schemas:
```js
import DynamoDM from 'dynamodm'

const ddm = DynamoDM()
const table = ddm.Table('my-dynamodb-table')

// a model that has no schema and will allow any data to be
// stored and loaded:
const Model = table.model(ddm.Schema('any'));

const doc = new Model({
    aKey: 'a value',
    'another key': {
        a: 123, b: { c: null }
    },
    anArray: [
        1, true, { x: 123 },
    ]
})
await doc.save();

// all dynamodm documents have an .id field by default, which is
// used as the table's primary (hash) key:
const loadedDoc = await Model.getById(doc.id);

// change the document and re-save:
loadedDoc.aKey = 'a different value';
await loadedDoc.save();
```

## Philosophy
DynamoDM is designed to make it easy to write simple, scalable, apps using
DynamoDB.

It supports Single Table Design, where different model types are stored in a
single DynamoDB table.

The table hash key is used as a unique id for each document, ensuring documents
are always evenly spread across all partitions.

Not all DynamoDB functions are available, but DynamoDM is designed to be
efficient, and make it easy to write apps that make the most of DynamoDB's
scalability, performance, and low cost.

The simple API is inspired by [Mongoose](https://mongoosejs.com), but there are
many differences between MongoDB and DynamoDB, in particular when it comes to
querying documents: DynamDB's indexing and query capabilities are much more
limited.

# API

Index to main classes and methods:
 * [DynamoDM(options)](#dynamodmoptions)
    * [Table(tableName, options)](#tabletablename-options)
        * [async .ready(options)](#async-tablereadyoptions)
        * [.model(schema)](#tablemodelschema)
    * [Schema(name, jsonSchema, options)](#schemaname-jsonschema-options)
        * [built-in schema fragments](#built-in-schema-fragments)
        * [indexing documents](#indexing-documents)
        * [.methods](#schemamethods)
        * [.statics](#schemastatics)
        * [.virtuals](#schemavirtuals)
        * [.converters](#schemaconverters-array)
    * [Models](#model-types)
        * [constructor](#modelconstructor-new-modelproperties)
        * [async .save()](#async-modelsave)
        * [async .remove()](#async-modelremove)
        * [async .toObject(options)](#async-modeltoobjectvirtuals-converteroptions)
        * [static async .getById()](#static-async-modelgetbyidid)
        * [static async .queryOne(query, optoins)](#static-async-modelqueryonequery-options)
        * [static async .queryMany(query, optoins)](#static-async-modelquerymanyquery-options)


## DynamoDM(options)
The DynamoDM() function returns an instance of the API. The API instance holds
default options (including logging), and provides access to create Tables and
Schemas, and to the built in schemas.

Schemas from one DynamoDM instance can be used with tables from another. Aside
from default options, all state is stored within Table instances.

```js
import DynamoDM from 'dynamodm'

const ddm = DynamoDM({
    logger: { level:'eror' },
    clientOptions: { endpoint:'http://localhost:8000' },
})

const table = ddm.Table('my-table-name')
const aSchema ddm.Schema('my-model-name', {}, {})
```

Options:
 * `logger`: valid values:
    * `false` / `undefined`: logging is disabled
    * A [`pino`](https://getpino.io) logger (or any other logger with a
      `.child()` method), in which case `logger.child({module:'dynamodm'})` is
      called to create a logger.
    * An [pino options
      object](https://getpino.io/#/docs/api?id=options-object), which will be
      used to create a new pino instance. For example `logger:{level:'trace'}`
      to enable trace-level logging.
 * ... all other options supported by [.Table](#table-tablename-options) or [.Schema](#schemaname-jsonschema-options).

## Table(tableName, options)
Create a handle to a DynamoDB table. The table stores connection options, model
types and indexes, and validates compatibility of all the different models
being used in the same table.

All models must be added to a table before calling either `.ready()` (for full
validation, including creating the table and indexes if necessary), or
`.assumeReady()` (for a quick compatibility check, without checking the
DynamoDB state).

```js
const table = ddm.Table('my-table-name', tableOptions)

// add models here ...

await table.ready()
```

Options:
 * `name`: The name of the dynamodb table (`tableName` may be passed as an
   `options.name` and `tableName` omitted).
 * `client`: The
   [`DynamoDBClient`](https://www.npmjs.com/package/@aws-sdk/client-dynamodb)
   to be used to connect to DynamoDB, if omitted then one will be created.
 * `clientOptions`: Options for `DynamoDBClient` creation (ignored if
   `options.client` is passed).
 * `retry`: Options for request retries, requests are re-tried when dynamodb
   [batching limits are
   exceeded](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff).
   Defaults to`{exponent: 2, delayRandomness: 0.75, maxRetries: 5}`.

### async Table.ready(options)
Wait for the table to be ready. The current state of the table is queried and
it is created if necessary. 

Options:
 * `waitForIndexes`: if true then any missing indexes that are required
   will also be created. This may take a long time, especially if indexes are
   being created that must be back-filled with existing data. Recommended for
   convenience during development only!

### Table.assumeReady()
Check the basic compatibility of the models in this table, and assume it has
been set up correctly already in dynamodb. Use this instead of `.ready()` if
using dynanamoDM in a short-lived environment like a lambda function.

### Table.model(schema)
Create and return a [`Model`](#model-types) in this table, using the specified
[schema](#schema). Or return the existing Model type for this schema if it has
already been added.

### async Table.deleteTable()
Delete the DynamoDB table (sends a `DeleteTableCommand` with the name of this
table). This will delete all data in the table! Will fail if deletion
protection has been enabled for the table.

### async Table.destroyConnection()
Clears the state of this table, and if the underlying [DynamoDB
client](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-lib-dynamodb/)
was created by this table (if it was not passed in as an option), calls and
awaits
[`client.destroy()`](https://nodejs.org/api/http.html#http_agent_destroy)
before returning.

Returns nothing and accepts no options.

### Table properties
 * `.name`: The name of the table, as passed to the constructor.
 * `.client`: The DynamoDB client for the table.
 * `.docClient`: The [DynamoDB document
   client](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-lib-dynamodb/)
   for the table.


## Schema(name, jsonSchema, options)
Create a Schema instance named `name`, with the schema (which may be empty), and options.

The jsonSchema is implied to be an object (`type:'object'`), and must define
[properties](https://json-schema.org/learn/getting-started-step-by-step#define).
Other schema keywords may not be used at the top-level of the schema, apart
from
[`additionalProperties`](https://json-schema.org/understanding-json-schema/reference/object#additional-properties),
and
[`required`](https://json-schema.org/understanding-json-schema/reference/object#required).

Schemas may define special fields using built-in schema fragments in
`.properties`. If multiple models are defined in the same table, the special
fields must all be compatible (for example all models must use the same names
for their ID fields and type fields).

Supported options:
 * `options.index`: The indexes for this schema, if any. See [Indexing
   Documents](#indexing-documents) for details.
 * `options.generateId`: A function used to generate a new id for documents of
   this type. Defaults to ``` () => `${schema.name}.${new ObjectId()}` ```

After creating a schema, [`.methods`](#schemamethods),
[`.statics`](#schemastatics), [`.virtuals`](#schemavirtuals), and
[`.converters`](#schemaconverters) may be defined. These will be added to the
model instances created from this schema.

### JSON schema for Schemas
Because DynamoDM uses the [DynamoDB Document
client](https://github.com/aws/aws-sdk-js-v3/tree/main/lib/lib-dynamodb#aws-sdklib-dynamodb),
native javascript types such as Arrays and Objects are converted to their
DynamoDB types [in the same
way](https://github.com/aws/aws-sdk-js-v3/tree/main/lib/lib-dynamodb#marshalling-input-and-unmarshalling-response-data).

The [built-in schema types](#built-in-schema-types) can also be used to
conveniently convert numbers to `Date` objects and binary data to `Buffer`
objects.

#### Examples:
Defining a model of type 'any', that has no restrictions on its fields:
```js
const AnythingSchema = table.Schema('any')
const Anything = table.model(AnythingSchema)
await (new Anything({ someField: 123 })).save()
await (new Anything({ someField: 'foo' })).save()
```

Defining a model with nested object fields (`M` map type in DynamoDB):
```js
const FooSchema = table.Schema('foo', {
    properties: {
        nested: {
            type: 'object',
            properties: {
                field1: {type: 'number'},
                field2: {type: 'string'},
            }
        },
    }
})
const Foo = table.model(FooSchema)
const f1 = await (new Foo({ nested: { field1: 123 } })).save()
const f2 = await (new Foo({ nested: { field2: 'a string' } })).save()

// { nested: {field1: 123}, type:'foo', id: ... }
console.log(await Foo.getById(f1.id))
```

Defining a model with a timestamp field (a Date object on the model which is
stored as a number in DynamoDB), which has an index that can be used for range
queries:
```js
const CommentSchema = table.Schema('comment', {
    properties: {
        text: {type: 'string'}
        commentedAt: DynamoDM().Timestamp,
    }
}, {
    index: {
        myFirstIndex: {
            // every index must have a hash key for which an exact
            // value is supplied to any query. The built-in .type
            // field is often a sensible choice of hash key:
            hashKey: "type",
            sortKey: "commentedAt"
        }
    }
})
const Comment = table.model(CommentSchema)
const c1 = await (new Comment({ text: 'some text', commentedAt: new Date() })).save()

// { text: 'some text', commentedAt: 2028-02-29T16:43:53.656Z, type:'comment', id: ... }
console.log(await Foo.getById(f1.id)) 

const recentComments = await Comment.queryMany({ 
    type: 'comment',
    commentedAt: { $gt: new Date(Date.now() - 60*60*24*1000) }
})
// [ { text: 'some text', commentedAt: 2028-02-29T16:43:53.656Z, type:'comment', id: ... } ]
console.log(recentComments) 

```

### Built-in schema types
 * `DynamoDM().Timestamp`: Converted to `Date` object on load, Saved as a
   DynamoDB `N` number type (the `.getTime()` value).
 * `DynamoDM().Binary`: Converted to a `Buffer` on load. Saved as DynamoDB `B`
   binary type. DynamoDB binary types are otherwise returned as `Uint8Array`s.

### Built-in schema fragments
Special fields are defined by using fragments of schema by value.
 * `DynamoDM().DocIdField`: used to indicate the id field, used by getById and
   other methods. The default id field name is `id`.
 * `DynamoDM().TypeField`: used to indicate the type field, which stores the
   name of the model that a saved document was created with. The default type
   field name is `type`.
 * `DynamoDM().CreatedAtField`: used to indicate a timestamp field that is
   updated when a model is first created by dynamodm. This field is not used
   unless you include this schema fragment in a model's schema.
 * `DynamoDM().UpdateAtField`: used to indicate a timestamp field that is
   updated whenever `.save()` is called on a document. This field is not used
   unless you include this schema fragment in a model's schema.

All models in the same `Table` must share the same .id and .type fields
identified by the built-in `DocIdField` and `TypeField` schemas. If they don't
then an error will be thrown when calling `table.ready()`.


For example, declaring models that use `._dynamodm_id` as the id field, instead of
the default `.id`:
```js
import DynamoDM from 'dynamodm'
const ddm = DynamoDM()

const table = ddm.Table('my-table-name');

const Model1 table.model(ddm.Schema('m1', {
    properties: {
        _dynamodm_id: ddm.DocIdField
    }
}));

const Model2 = table.model(ddm.Schema('m2, {
    properties: {
        _dynamodm_id: ddm.DocIdField
    }
}));

// if any models have been added to the table that use a different id field
// name, this will throw:
await table.ready();

const m1 = await (new Model1()).save();
const m2 = await (new Model2()).save();

console.log(m1._dynamodm_id);

```


## Schema.methods
Instance methods on a model may be defined by assigning to `schema.methods`:
```js
const CommentSchema = table.Schema('comment', {
    properties: {text: {type: 'string'}}
})
CommentSchema.methods.countWords = function() {
    return this.text.split().length
}
const Comment = table.model(CommentSchema)
const comment = new Comment({text:'text for my comment'})
const wc = comment.countWords()
```

## Schema.statics
Static methods on a model may be defined by assigning to `schema.statics`:
```js
const CommentSchema = table.Schema('comment', {
    properties: {text: {type: 'string'}, user: {type: ddm.DocId}}
})
CommentSchema.statics.createAndSaveForUser = async function(user, properties) {
    // in static methods 'this' is the model prototype:
    const comment = new this(properties)
    comment.user = user.id
    await comment.save()
    return comment
}
const Comment = table.model(CommentSchema)
const aComment = await Comment.createAndSaveForUser(
    aUser, {text: 'my comment text'}
)
```

## Schema.virtuals
Virtual properties for a model may be defined by assigning to
`schema.virtuals`. Virtual properties are useful for computing properties that
are required by the application, but which are not saved in the database. 

Virtual properties can either be a string alias for another property, in which
case a getter and setter for the property are defined automatically:
```js
const CommentSchema = table.Schema('comment', {
    properties: {text: {type: 'string'}}
})
CommentSchema.virtuals.someText = 'text'
const Comment = table.model(CommentSchema)

const comment = new Comment({text:'text for my comment'})
console.log(comment.someText) // 'text for my comment'

comment.someText = 'new text'
await comment.save()
console.log(comment.text) // 'new text'
```

Or a data descriptor or accessor descriptor that will be passed to
[`Object.defineProperties`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/defineProperties),
and which defines its own `get` and/or `set` methods:
```js
const CommentSchema = table.Schema('comment', {
    properties: {text: {type: 'string'}}
})
CommentSchema.virtuals.wordCount = {
    get: function() {
        return this.text.split().length
    }
}
const Comment = table.model(CommentSchema)
const comment = new Comment({text:'text for my comment'})
const wc = comment.wordCount
```


## Schema.converters (Array)
Virtual properties must be synchronous, but sometimes it's useful to
asynchronously compute field values. To enable this
[`.toObject()`](#async-modeltoobjectvirtuals-converteroptions) will
asynchronously iterate over the array of Schema.converters when converting a
document to a plain object.

Converters can also be used to redact fields that should be hidden from the
serialised versions of documents (for example when serialising for an API).

`.converters` is an array, and the converters are always executed in order:

```js
const UserSchema = table.Schema('user', {
    properties: {emailAddress: {type:'string'}, name: {type:'string'}
})

// converter to count the comments this user has made:
UserSchema.converters.push(async (value, options) => {
    // get a handle to a previously defined Comment Model from its schema:
    const Comment =  this.table.model(CommentSchema)
    // update value asynchronously
    value.commentCount = (await Comment.queryManyIds(
        { user: this.id },
        { limit: 100 }
    )).length
    // converters must return the new value
    return value
})

// converter to redact the email address:
UserSchema.converters.push((value, options) => {
    delete value.emailAddress
    // the converted value will no longer have .emailAddress, but
    // 'this.emailAddress' is still available to subsequent
    // converters if they need it
    return value
})

// converter that uses an option:
UserSchema.converters.push((value, options) => {
    value.newField = options.someOptionForConverters
    return value
})

const User = table.model(UserSchema)
const user = User.getById('user.someid')
const asPlainObj = await user.toObject({
    someOptionForConverters: 'foo'
})

// { commentCount: 4, newField: 'foo', type: 'user', id: ...}
console.log(asPlainObj) 
```

## Model Types
Model types are the main way that documents stored in dynamodb are accessed. A
unique class is created for each model type in a table, with the name
`Model_schemaname`. All methods are provided by an internal base class
(`BaseModel`), which is not directly accessible.

Instances of a model (`const doc = new MyModel(properties)`) are referred to as
Documents.

To set fields in the database, set properties on a document and then call
`doc.save()`. There are no limits on field names that can be used, apart from
the normal javascript reserved names like `constructor`.

### Creating models
Models are created by calling [table.model()](#tablemodelschema) with a
[schema](#schemaname-jsonschema-options).

#### static Model fields
Each model class that is created has static fields:
 * `Model.type`: The name of the schema that was used to create this model
   (which is the same as value of the built in type field for documents of this
   model type).
 * `Model.table`: the table in which this model was created.

For example:
```js
const MyFooModel = table.model(ddm.Schema('foo'));
// MyFooModel.table === table
// MyFooModel.type === 'foo'

// these are static, so only on the model class, not on its instances:
const fooDoc = new MyFooModel();
// fooDoc.table === undefined
```

## Creating, updating, and removing Documents.
### Model.constructor (new Model(properties)) 
Create a new document (a model instance) with the specified properties.

```js
const aCommment = new Comment({
    text: 'some text',
    user: aUser.id,
    commentTime: new Date()
});
```

### async Model.save() 
Save the current version of this document to the database, if this document was
loaded from the database then an existing document will be updated, otherwise a
new document will be created.

Save a new document:
```
const aCommment = new Comment({
    text: 'some text',
    user: aUser.id,
});
await aComment.save();
```

Update and save an existing document:
```
const aComment = await Comemnt.getById(someId);
aComment.text = 'new text';
await aComment.save();
```

### async Model.remove()
Delete a document.
```js
const aComment = await Comemnt.getById(someId);
await aComment.delete();
```

### async Model.toObject({virtuals, ...converterOptions})
Convert a document into a plain object representation (i.e. suitable for JSON
stringification):

Note that this method is asynchronous (returns a Promise that must be awaited),
because it may execute the [`.converters`](#schemaconverters-array) that the
schema defines for this model type.

```
const aCommment = new Comment({
    text: 'some text',
    user: aUser.id,
});
await aComment.save();

const stingified = JSON.stringify(await aComment.toObject());
```

## Getting Documents by ID
### static async Model.getById(id)
Get a document by its ID. By default models use `.id` as the ID field. It's
possible to change this by using the [built-in schema
fragments](#built-in-schema-fragments) in your model's schema.

With the default ID field (`.id`):
```js
const aComment = await Comemnt.getById(someId);
// aComment.id === someId
```

With a custom ID field:
```js
import DynamoDM from 'dynamodm'
const ddm = DynamoDM()

const table = ddm.Table('my-table-name');

const FooSchema = ddm.Schema('foo', {
    properties: {
        _dynamodm_id: ddm.DocIdField
    }
});
const Foo = table.model(FooSchema);

// if any models have been added to the table that use a different id field
// name, this will throw:
await table.ready();

const a = await (new Foo()).save();
const b = Foo.getById(a._dynamodm_id);
```

### static async Model.getByIds([id, ...])
As [`Model.getById`](#static-async-model-getbyid-id), but accepts an array of
up to 100 ids to be fetched in a batch.

## Finding and Querying Documents

### Query Format
The query API accepts mongo-like queries, of the form
```js
{ fieldName: valueToSearchFor }
```

For indexes over a single field (where the single field is the hash index)
values can only be queried by equality. However since Global Secondary Indexes
may contain multiple values for the same hash key multiple results may still
match the query.

A limited set of non-equality query operators are supported. They may be used
only on fields for which an index with a sort key (also known as a range key)
has been declared, and always require a value to be specified for the
corresponding index's hash key.

See [Indexing Documents](#indexing-documents) for declaring indexes.

 * `$gt`: Find items where the specified field has a value strictly greater than
   the supplied value.
   ```js
   {
       a: "some value", // the .a field must be the GSI hash key
       b: { $gt: 123 }  // the .b field must be the GSI sort key
   }
   ```
 * `$gte`: Find items where the specified field has a value greater than or
   equal to the supplied value.
   ```js
   {
       a: "some value", // the .a field must be the GSI hash key
       b: { $gte: 123 } // the .b field must be the GSI sort key
   }
   ```
 * `$lt`: Find items where the specified field has a value strictly less than
   the supplied value.
   ```js
   {
       a: "some value", // the .a field must be the GSI hash key
       b: { $lt: 123 }  // the .b field must be the GSI sort key
   }
   ```
 * `$lte` Find items where the specified field has a value less than or equal
   to the supplied value.
   ```js
   {
       a: "some value", // the .a field must be the GSI hash key
       b: { $lte: 123 } // the .b field must be the GSI sort key
   }
   ```
 * `$between` Find items where the specified field has a value greater than or
   equal to the first value, and less than or equal to the second value
   ```js
   {
       a: "some value", // the .a field must be the GSI hash key
       b: { $between: [123, 234] } // the .b field must be the GSI sort key
   }
   ```
 * `$begins` Find items where the specified field has a value greater than or
   equal to the first value, and less than or equal to the second value
   ```js
   {
       a: "some value", // the .a field must be the GSI hash key
       // the .b field must be the GSI sort key, and the type
       // of .b must be string.
       b: { $begins: "some prefix" }
   }
   ```

#### Query Format examples
**Querying for a single document property** (a dynamodb attribute) named
`someField`, equal to a value `"someValue"`. This requires an index that
includes `someField` as its hash key:`
```js
const result = await Comment.queryOne({
    someField: "someValue"
})
```

**Querying for a two properties** named `field1`, and `field2`, equal to
values `"v1"` and `2`. This requires an index that either:
 * has `field1` as its hash key, and `field2` as its sort key, or:
 * has `field2` as its hash key, and `field2` as its sort key.

Note that this query may return multiple results, since neither hash key nor
sort key values in global secondary indexes are necessarily unique.
```js
const results = await Comment.queryMany({
    field1: "v1",
    field2: 2
})
```

If you are always querying for equality on two fields, then consider combining
them into a single field, and using [`.virtuals`](#schemavirtuals) to make them
separately accessible.

**Querying for a value range.** Using the range operators `$lt`, `$lte`, `$gt`,
`$gte`, `$between` or `$begins` requires a sort key, and always also requires
that a hash key is specified by value.

```js
const MyModelSchema = ddb.Schema({
    properties: {
        field1: {type: 'string'},
        field2: {type: 'string'}
    }
}, {
    index: {
        myIndexName: {
            hashKey: 'field1',
            sortKey: 'field2'
        }
    }
})
const MyModel = table.model(MyModelSchema);
const results = await MyModel.queryMany({
    field1: "v1",
    field2: {
        $gt: "2013-01-28"
    }
})
```


#### Order of query results
If the query includes a sort key, then results will be ordered by the sort key.
Otherwise the order of query results is undefined. The order can be reversed by
setting `options.rawQueryOptions.ScanIndexForward: false`.

### static async Model.queryOne(query, options)
Query for a single document. See [query format](#query-format) for the
supported query format.

Supported options:
 * abortSignal: The `.signal` of an
   [`AbortController`](https://nodejs.org/api/globals.html#class-abortcontroller),
   which may be used to interrupt the asynchronous request.
 * startAfter: A document after which to search for the next query result. This
   can be used for pagination by returning the result from a previous query.
 * rawQueryOptions
 * rawFetchOptions

Resolves with a document instance of the model type on which this was called,
or null if there were no results. Rejects if there's an error.

### static async Model.queryOneId(query, options)
Query for the ID of a single model. See [query format](#query-format) for the
supported query format.

Supported options:
 * abortSignal: The `.signal` of an
   [`AbortController`](https://nodejs.org/api/globals.html#class-abortcontroller),
   which may be used to interrupt the asynchronous request.
 * startAfter: A document after which to search for the next query result. This
   can be used for pagination by returning the result from a previous query.
 * rawQueryOptions

Resolves with a document id (string), or null if no document matched the query.
Rejects if there's an error.

### static async Model.queryMany(query, options)
Query for an array of documents. See [query format](#query-format) for the
supported query format. 

Supported options:
 * limit: The maxuimum number of models to return. May be combined with
   `startAfter` to paginate restults.
 * abortSignal: The `.signal` of an
   [`AbortController`](https://nodejs.org/api/globals.html#class-abortcontroller),
   which may be used to interrupt the asynchronous request.
 * startAfter: A document after which to search for the next query result. This
   can be used for pagination by returning the result from a previous query.
 * rawQueryOptions
 * rawFetchOptions

Resolves with an array of document instances of the model type on which this was
called, or an empty array if there were no results. Rejects if there's an
error.

### static async Model.queryManyIds(query, options)
Query for an array of document Ids. See [query format](#query-format) for the
supported query format. 

Supported options:
 * limit: The maxuimum number of models to return. May be combined with
   `startAfter` to paginate restults.
 * abortSignal: The `.signal` of an
   [`AbortController`](https://nodejs.org/api/globals.html#class-abortcontroller),
   which may be used to interrupt the asynchronous request.
 * startAfter: A document after which to search for the next query result. This
   can be used for pagination by returning the result from a previous query.
 * rawQueryOptions

Resolves with an array of document ids (strings), or an empty array if there
were no results. Rejects if there's an error.


## The raw Query API
The raw query API allows queries to be executed with a raw [lib-dynamodb
query](https://www.npmjs.com/package/@aws-sdk/lib-dynamodb), of the form:

```js
{
    IndexName: <name of index to query against>,
    KeyConditionExpression: <key condition expression>,
    ExpressionAttributeValues: <expression attribute values>,
    ExpressionAttributeNames: <expression attribute names>,
    Limit: <query document limit>,
    ...
}
```
The index name is mandatory, since it cannot be determined automatically,
however the table name does not need to be provided.

The [key condition
expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.KeyConditionExpressions.html),
[expression attribute
values](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeValues.html),
and [expression attribute
names](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html)
must all be specified. Other values supported by the [query
command](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-client-dynamodb/Interface/QueryCommandInput/)
are optional.

### static async Model.rawQueryOneId(query, rawOptions)
Send a raw query command and return a single document ID.

Supported rawOptions:
 * `abortSignal`: The `.signal` of an
   [`AbortController`](https://nodejs.org/api/globals.html#class-abortcontroller),
   which may be used to interrupt the asynchronous request.

### static async Model.rawQueryManyIds(query, rawOptions)
Send a raw query command and return an array of document IDs.

Supported rawOptions:
 * `limit`: maximum number of IDs to return. Detauls to Infinity.
 * `abortSignal`: The `.signal` of an
   [`AbortController`](https://nodejs.org/api/globals.html#class-abortcontroller),
   which may be used to interrupt the asynchronous request.

### static async* Model.rawQueryIteratorIds(query, rawOptions)
An async generator that yields IDs (up to rawOptions.limit, which may be Infinity).

Supported rawOptions:
 * `limit`: maximum number of IDs to return. Detauls to Infinity.
 * `abortSignal`: The `.signal` of an
   [`AbortController`](https://nodejs.org/api/globals.html#class-abortcontroller),
   which may be used to interrupt the asynchronous request.


## Indexing Documents
DynamoDM supports only Global Secondary Indexes. Any document field name which
is indexed must have the same type in all documents in the table in which it
occurs (this is checked by `.ready()`).

An index may have either:
 * just a hash key (which need not be unique), which only supports queries by
   exact value.
 * Or a hash key and a sort key (range key), where the hash key must be
   specified by exact value, but the sort key supports range queries.

To specify an index, use the `.index` option when creating a
[Schema](https://github.com/autopulated/dynamodm?tab=readme-ov-file#schemaname-jsonschema-options):

The `.index` option is an object where the fields are the names of the indexes,
and the value is either an object specifing the hash key and optionally the
sort key for the index, or it may just be the value '1' or true indicating that
the index name is the same as the hash key of the index, and there is no sort
key:
```js
{
    // an index called anIndexName where .field1 is the 
    // hash key and .field2 is the sort key
    anIndexName: {
        hashKey: 'field1',
        sortKey: 'field2',
    },

    // an index called 'field3' wheres `field3` is the hash 
    // key, and there is no sort key:
    field3: 1
}
```

All fields referred to in the index option must be defined in the schema. This
is because the types of the fields need to be known to use and create the
index.

Example:
```js
const CommentSchema = ddm.Schema('c', {
    properties: {
        text: {type: 'string' },
        user: ddm.DocId,
        section: {type: 'string' },
        createdAt: ddm.CreatedAtField
    }
}, {
    index: {
        findByUser: {
            hashKey: 'user',
            sortKey: 'createdAt'
        },
        section: 1
    }
})

const Comment = table.model(CommentSchema)
const c1 = await (new Comment({ text: 'some text', commentedAt: new Date() })).save()

// { text: 'some text', commentedAt: 2028-02-29T16:43:53.656Z, type:'comment', id: ... }
console.log(await Foo.getById(f1.id)) 
```


### Caveats for Indexes
A dynamoDB table supports up to 20 global secondary indexes in the default
quota. DynamoDM creates one built-in index on the id field.

All documents in the same table share the same indexes, and all documents that
include a field that is used as the hash key of an index will be included in
that index, even if they are not the same type as the schema that declared the
index.

This can be an advantageous, by allowing multiple document types to share a
single index (if multiple models declare the same index, DynamoDM will only
create it once), but care must be taken to ensure that your query only returns
documents of the desired type.

The easiest way to share indexes between model types is by using the built-in
[type field](#built-in-schema-fragments) as the hash key of the index, for
example, to allow both `Comments` and `Uploads` belonging to a particular user
to be found using the same index:

```js
import DynamoDM from 'dynamodm'

// get an instance of the API (options can be passed here)
const ddm = DynamoDM()

// get a reference to a table:
const table = ddm.Table('my-dynamodb-table')

// Create User and Comment models with their JSON schemas in this table:
const UserSchema = ddm.Schema('user', { })

const CommentSchema = ddm.Schema('comment', {
    properties: {
        text: { type: 'string' },
        user: ddm.DocId
    }
}, {
    index: {
        findByUser: {
            hashKey: 'type',
            sortKey: 'user'
        }
    }
})

const UploadSchema = ddm.Schema('upload', {
    properties: {
        url: { type: 'string' },
        user: ddm.DocId
    }
}, {
    index: {
        findByUser: {
            hashKey: 'type',
            sortKey: 'user'
        }
    }
})

const User = table.model(UserSchema)
const Comment = table.model(CommentSchema)
const Upload = table.model(UploadSchema)

await table.ready()

// both these queries will use the findByUser index. Since the hash
// key of the index is `type`, we can be sure that only documents 
// of the correct type are returned to each query:
const commentsForUser = await Comment.queryMany({ 
    type: CommentSchema.name, user: aUser.id
})
const uploadsForUser = await Upload.queryMany({
    type: UploadSchema.name, user: aUser.id
})
```


## Bugs, Questions, Problems?
Please open a [github issue](https://github.com/autopulated/dynamodm/issues) :)


## Sponsors
This project is supported by:
 * [TraitorBird](https://traitorbird.com), simple canary tokens.
 * [Coggle](https://coggle.it), simple collaborative mind maps.


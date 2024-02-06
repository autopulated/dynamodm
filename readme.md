## DynamoDM: Simple Document Mapper for DynamoDB

[![CI](https://github.com/autopulated/DynamoDM/actions/workflows/test.yml/badge.svg)](https://github.com/autopulated/DynamoDM/actions/workflows/test.yml)
[![Coverage](https://coveralls.io/repos/github/autopulated/dynamodm/badge.svg?branch=main)](https://coveralls.io/github/autopulated/dynamodm?branch=main)
[![NPM version](https://img.shields.io/npm/v/dynamodm.svg?style=flat)](https://www.npmjs.com/package/dynamodm)


## Quickstart:
```js
import DynamoDM from 'dynamodm'

// get an instance of the API (options can be passed here)
const ddm = DynamoDM()

// get a reference to a table:
const table = ddm.Table({name: 'my-dynamodb-table'})

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

## Philosophy
DynamoDM is designed to make it easy to write simple, scalable, apps using
DynamoDB.

It supports Single Table Design, where different model types are stored in a
single DynamoDB table.

Not all DynamoDB functions are available, but DynamoDM is designed to be
efficient, and make it easy to write apps that make the most of DynamoDB's
scalability, performance, and low cost.

The simple API is inspired by [Mongoose](https://mongoosejs.com), but there are
many differences between MongoDB and DynamoDB, in particular when it comes to
querying documents: DynamDB's indexing and query capabilities are much more
limited.

# API

## DynamoDM()
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

Valid options:
 * `logger`: valid values:
    * False / undefined: logging is disabled
    * A pino [`pino`](https://getpino.io) logger (or other logger with a
      `.child()` method), in which case `logger.child({module:'dynamodm'})` is
      called to create a logger.
    * An [pino options
      object](https://getpino.io/#/docs/api?id=options-object), which will be
      used to create a new pino instance. For example `logger:{level:'trace'}`
      to enable trace-level logging.
 * ... all other options supported by [.Table](#table-tablename-options) or [.Schema](#schemaname-jsonschema-options).

## Table(tableName, options)
Create a handle to a dynamoDB table. The table stores connection options, model
types and indexes, and validates compatibility of all the different models
being used in the same table.

All models must be added to a table before calling either `.ready()` (for full
validation, including creating the table and indexes if necessary), or
`.assumeReady()` (for a quick compatibility check, without checking the
DynamoDB state.

```js
const table = ddm.Table('my-table-name', tableOptions)

// add models here ...

await table.ready()
```

### async Table.ready(options)
Wait for the table to be ready. The current state of the table is queried and
it is created if necessary. 

Options:
 * `options.waitForIndexes`: if true then any missing indexes that are required
   will also be created. This may take a long time, especially if indexes are
   being created that must be back-filled with existing data. Recommended for
   convenience during development only!

### Table.assumeReady()
Check the basic compatibility of the models in this table, and assume it has
been set up correctly already in dynamodb. Use this instead of .ready() if
using dynanamoDM in a short-lived environment like a lambda function.

### Table.model(schema)
Create and return a [`Model`](#model-types) in this table, using the specified
[schema](#schema). Or return the existing Model type for this schema if it has
already been added.

### async Table.deleteTable()
Delete the dynamoDB table (sends a `DeleteTableCommand` with the name of this
table). This will delete all data in the table! Will fail if deletion
protection has been enabled for the table.

### async Table.destroyConnection()
Clears the state of this table, and if the underlying [dynamoDB
client](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-lib-dynamodb/)
was created by this table (if it was not passed in as an option), calls and
awaits
[`client.destroy()`](https://nodejs.org/api/http.html#http_agent_destroy)
before returning.

Returns nothing and accepts no options.

### Table properties
 * `.name`: The name of the table, as passed to the constructor.
 * `.client`: The dynamoDB client for the table.
 * `.docClient`: The [dynamoDB document
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
 * `options.index`: The indexes for this schema, if any.
 * `options.generateId`: A function used to generate a new id for documents of this type. Defaults to ``` () => `${schema.name}.${new ObjectId()}` ```

After creating a schema, [`.methods`](#schemamethods),
[`.statics`](#schemastatics), [`.virtuals`](#schemavirtuals), and
[`.converters`](#schemaconverters) may be defined. These will be added to the
model instances created from this schema.

### JSON schema for Schemas
Because DynamoDM uses the [dynamoDB Document
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
stored as a number in DynamoDB):
```js
const CommentSchema = table.Schema('comment', {
    properties: {
        text: {type: 'string'}
        commentedAt: DynamoDM().Timestamp,
    }
})
const Comment = table.model(CommentSchema)
const c1 = await (new Comment({ text: 'some text', commentedAt: new Date() })).save()

// { text: 'some text', commentedAt: 2028-02-29T16:43:53.656Z, type:'comment', id: ... }
console.log(await Foo.getById(f1.id)) 

```

### Built-in schema types
 * `DynamoDM().Timestamp`: Converted to `Date` object on load, Saved as a
   DynamoDB `N` number type (the `.getTime()` value).
 * `DynamoDM().Binary`: Converted to a `Buffer` on load. Saved as DynamoDB `B`
   binary type. dynamoDB binary types are otherwise returned as `Uint8Array`s.

### Built-in schema fragments
Special fields are defined by using fragments of schema by value.
 * `DynamoDM().DocIdField`
 * `DynamoDM().TypeField`
 * `DynamoDM().CreatedAtField`
 * `DynamoDM().UpdateAtField`

Declaring models that use `._dynamodm_id` as the id field, instead of the default `.id`:

```js
import DynamoDM from 'dynamodm'
const ddm = DynamoDM()

const table = ddm.Table('my-table-name');

const Model1Schema = table.Schema('m1, {
    properties: {
        _dynamodm_id: ddm.DocIdField
    }
});

const Model2Schema = table.Schema('m2, {
    properties: {
        _dynamodm_id: ddm.DocIdField
    }
});

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
Convert a document into a plain object representation (e.g. suitable for JSON
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

## Getting Documents by id
### static async Model.getById(id)
### static async Model.getByIds([id, ...])

## Finding and Querying Documents
### static async Model.queryOne(query, options)
### static async Model.queryOneId(query, options)
### static async Model.queryMany(query, options)
### static async Model.queryManyIds(query, options)

## The raw Query API
### static async Model.rawQueryOneId(query, rawOptions)
### static async Model.rawQueryOneId(query, rawOptions)
### static async Model.rawQueryOneId(query, rawOptions)

## Indexing Documents

## Sponsors
This project is supported by:
 * [TraitorBird](https://traitorbird.com), simple canary tokens.
 * [Coggle](https://coggle.it), simple collaborative mind maps.


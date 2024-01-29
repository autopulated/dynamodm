## DynamoDM: Simple Document Mapper for DynamoDB

[![CI](https://github.com/autopulated/DynamoDM/actions/workflows/test.yml/badge.svg)](https://github.com/autopulated/DynamoDM/actions/workflows/test.yml)
[![NPM version](https://img.shields.io/npm/v/dynamodm.svg?style=flat)](https://www.npmjs.com/package/dynamodm)


## Quickstart:
```js
import DynamoDM from 'dynamodm';

// get an instance of the API (options can be passed here)
const ddm = DynamoDM();

// get a reference to a DynamoDM table:
const table = ddm.Table({name: 'my-dynamodb-table'});

// Create a User model with a JSON schema:
const UserModel = table.model(ddm.Schema('user', {
    properties: {
        // Identify the id field using the built-in schema. Every model in the same table must share the same id field name:
        id: ddm.DocIdField,
        emailAddress: {type: 'string'},
        marketingComms: {type: 'boolean', default: false}
    },
}));
// and a Comment model:
const CommentModel = table.model(ddm.Schema('c', {
    properties: {
        id: ddm.DocIdField,
        createdAt: ddm.CreatedAtField,
        text: {type: 'string' },
        user: ddm.DocId
    },
    additionalProperties: true
}, {
    index: {
        findByUser: {
            hashKey: 'user',
            sortKey: 'createdAt'
        }
    }
}));

// wait for the table to be ready (created if necessary, creation of index):
await table.ready();

// create some documents (instances of models):
const aUser = new UserModel({emailAddress:"friend@example.com"});
await aUser.save();

const aComment = new CommentModel({user: aUser.id, text: "My first comment."});
await aComment.save();

// query for some documents:
const commentsForUser = await CommentModel.queryMany({user: aUser.id});

```

## Philosophy
DynamoDM is designed to make it simple to write simple apps that use DynamoDB,
preferring the single-table-design approach, where different model types are
mixed into a single DynamoDB table.

Not all DynamoDB functions are available, nor will they be, but DynamoDM is
designed to be efficient, and make it easy to write apps that make the most of
DynamoDB's scalability, performance, and low cost.

The simple API is inspired by [Mongoose](https://mongoosejs.com), but there are
many differences between MongoDB and DynamoDB, in particular when it comes to
querying documents: DynamDB's indexing and query capabilities are much more
limited.


## API
...


### Sponsors
This project is supported by:
 * [TraitorBird](https://traitorbird.com), simple canary tokens.
 * [Coggle](https://coggle.it), simple collaborative mind maps.


## DynamoDM: Simple Document Mapper for DynamoDB

[![CI](https://github.com/autopulated/DynamoDM/actions/workflows/test.yml/badge.svg)](https://github.com/autopulated/DynamoDM/actions/workflows/test.yml)
[![Coverage](https://coveralls.io/repos/github/autopulated/dynamodm/badge.svg?branch=main)](https://coveralls.io/github/autopulated/dynamodm?branch=main)
[![NPM version](https://img.shields.io/npm/v/dynamodm.svg?style=flat)](https://www.npmjs.com/package/dynamodm)


## Quickstart:
```js
import DynamoDM from 'dynamodm';

// get an instance of the API (options can be passed here)
const ddm = DynamoDM();

// get a reference to a table:
const table = ddm.Table({name: 'my-dynamodb-table'});

// Create User and Comment models with their JSON schemas in this table:
const UserSchema = ddm.Schema('user', {
    properties: {
        emailAddress: {type: 'string'},
        marketingComms: {type: 'boolean', default: false}
    },
});

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

const User = table.model(UserSchema);
const Comment = table.model(CommentSchema);

// wait for the table to be ready, all models should be added first.
await table.ready();

// create some documents (instances of models):
const aUser = new User({ emailAddress: "friend@example.com" });
await aUser.save();

const aComment = new Comment({ user: aUser.id, text: "My first comment." });
await aComment.save();

// query for some documents:
const commentsForUser = await Comment.queryMany({ user: aUser.id });
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


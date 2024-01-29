## DynamoDM: Simple Document Mapper for DynamoDB

## Quickstart:
```js
const DynamoDM =  require('dynamodm')();

const table = DynamoDM.Table('my-dynamodb-table');

// Create a User model with a JSON schema:
const UserModel = table.model(DynamoDM.Schema('user', {
    properties: {
        // Identify the id field using the built-in schema. Every model in the same table must share the same id field name:
        id: DynamoDM.DocIdField,
        emailAddress: {type: 'string'},
        marketingComms: {type: 'boolean', default: false}
    },
}));
// and a Comment model:
const CommentModel = table.model(DynamoDM.Schema('c', {
    properties: {
        id: DynamoDM.DocIdField,
        createdAt: DynamoDM.CreatedAtField,
        text: {type: 'string' },
        user: DynamoDM.DocId
    },
    additionalProperties: true
}, index: {
    findByUser: {
        hashKey: 'user',
        sortKey: 'createdAt'
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
limited, 


## API
...

### Sponsors
This project is supported by:
 * [TraitorBird](https://traitorbird.com), simple canary tokens.
 * [Coggle](https://coggle.it), simple collaborative mind maps.


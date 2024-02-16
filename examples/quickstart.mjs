import DynamoDM from 'dynamodm';
import {inspect} from 'util';

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
const aUser = new UserModel({emailAddress:'friend@example.com'});
await aUser.save();

const aComment = new CommentModel({user: aUser.id, text: 'My first comment.'});
await aComment.save();

// query for some documents:
const commentsForUser = await CommentModel.queryMany({user: aUser.id});
console.log('comments are:', inspect(commentsForUser));


// This example demonstrates how to take advantage of sorting within the sort
// key of an index to efficiently find and return documents of a specific type
// by equality of one property, sorted by a separate property. To do this both
// properties are saved to the same string field in the database, with virtual
// getters and setters defined to allow transparent access as separate
// properties for users of the model:
//
//
import DynamoDM from 'dynamodm';

// get an instance of the API (options can be passed here)
const ddm = DynamoDM();

// get a reference to a table:
const table = ddm.Table('my-dynamodb-table');

// Create User and Comment models with their JSON schemas in this table:
const UserSchema = ddm.Schema('user', { });

const CommentSchema = ddm.Schema('comment', {
    properties: {
        text: { type: 'string' },
        user_and_time: { type: 'string', default: '/' }
    }
}, {
    index: {
        findByUserAndTime: {
            hashKey: 'type',
            sortKey: 'user_and_time'
        }
    }
});

const UploadSchema = ddm.Schema('upload', {
    properties: {
        url: { type: 'string' },
        user_and_time: { type: 'string', default: '/' }
    }
}, {
    // ...
});

// define virtual getters and setters for easy access to the
// separate parts of the compound fields
UploadSchema.virtuals.user = CommentSchema.virtuals.user = {
    get() { return this.user_and_time.split('/')[0]; },
    set(v) { this.user_and_time = [v, this.user_and_time.split('/')[1]].join('/'); },
};

UploadSchema.virtuals.time = CommentSchema.virtuals.time = {
    get() { return Date.parse(this.user_and_time.split('/')[1]); },
    set(v) { this.user_and_time = [this.user_and_time.split('/')[0], v.toISOString()].join('/'); },
};

// define static helper functions to make the details of
// composing the query internal to the models:
UploadSchema.statics.getForUserSince =
CommentSchema.statics.getForUserSince = async function(user, sinceTime) {
    return await this.queryMany({
        type: this.type,
        user_and_time: {
            $between: [ `${user}/${sinceTime.toISOString()}`, `${user}/9999` ]
        }
    });
};


const User = table.model(UserSchema);
const Comment = table.model(CommentSchema);
const Upload = table.model(UploadSchema);

console.log('waiting for the table...');
await table.ready({ waitForIndexes: true });

const u1 = await (new User()).save();
const u2 = await (new User()).save();

const firstCommentTime = Date.now();

for (const user of [u1, u2]) {
    console.log(`creating records for ${user.id}...`);
    for (let i = 0; i < 10; i ++) {
        const comment = new Comment({text: `Text of comment ${i} by user ${user.id}.`});
        comment.user = user.id;
        comment.time = new Date(firstCommentTime + i * 10000);
        await comment.save();

        const upload = new Upload({url: `https://example.com/example-url-${i}`});
        upload.user = user.id;
        upload.time = new Date(firstCommentTime + i * 10000);
        await upload.save();
    }
}

// both these queries will use the findByUser index. Since the hash
// key of the index is `type`, we can be sure that only documents
// of the correct type are returned to each query:
for (const user of [u1, u2]) {
    console.log(`querying records for ${user.id}...`);
    const sinceTime = new Date(firstCommentTime + 50000);
    const comments = await Comment.getForUserSince(user.id, sinceTime);
    const uploads = await Upload.getForUserSince(user.id, sinceTime);

    console.log(`User ${user.id} comments since ${sinceTime}:`, comments);
    console.log(`User ${user.id} uploads since ${sinceTime}:`, uploads);
}


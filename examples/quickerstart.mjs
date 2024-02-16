import DynamoDM from 'dynamodm';

const ddm = DynamoDM();
const table = ddm.Table({name: 'my-dynamodb-table'});

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
});
await doc.save();

// all dynamodm documents have an .id field by default, which is
// used as the table's primary (hash) key:
const loadedDoc = await Model.getById(doc.id);

console.log(loadedDoc);


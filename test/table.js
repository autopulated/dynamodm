const t = require('tap');
const { DescribeTableCommand } = require('@aws-sdk/client-dynamodb');

const clientOptions = {
    endpoint: 'http://localhost:8000'
};

const DynamoDMConstructor = require('../');
const DynamoDM = DynamoDMConstructor({clientOptions, logger:{level:'error'}});

t.pass('import ok');

t.test('incorrect usage throws', async t => {
    const DynamoDMConstructor = require('../');
    t.throws(() => {
        DynamoDMConstructor.Table({name: 'test-table-1'});
    }, {message: "DynamoDM must be called as a function to get an instance of the API, e.g. const DynamoDM = require('dynamodm')(options);"}, 'DynamoDM.Table() throws');

    t.throws(() => {
        DynamoDMConstructor.Schema('emptySchema');
    }, {message: "DynamoDM must be called as a function to get an instance of the API, e.g. const DynamoDM = require('dynamodm')(options);"}, 'DynamoDM.Schema() throws');
});

t.test('table initialisation', async t => {
    await t.test('create without schemas', async t => {
        const table = DynamoDM.Table({ name: 'test-table-1', clientOptions});
        await t.rejects(table.ready());
    });
    await t.test('create with schema', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2', clientOptions});
        table.model(DynamoDM.Schema('emptySchema'));
        await table.ready();

        await t.test('call ready multiple times', async () => {
            await table.ready();
        });
    });

    await t.test('with abstract logger', async t => {
        const table = require('../')({logger: false}).Table({ name: 'test-table-abstract-logger', clientOptions});
        table.model(DynamoDM.Schema('emptySchema'));
        await table.ready();

        await t.test('call ready multiple times', async () => {
            await table.ready();
        });
    });

    await t.test('with existing logger', async t => {
        const pinoInstance = require('pino')({level:'error'});
        const results = t.capture(pinoInstance, 'child', pinoInstance.child);
        const table = require('../')({logger: pinoInstance}).Table({ name: 'test-table-pino-logger', clientOptions});
        table.model(DynamoDM.Schema('emptySchema'));
        await table.ready();
        t.match(results(), [{args:[{module:'dynamodm'}]}, {args:[{table:'test-table-pino-logger'}]}], 'should use the provided pino logger to create child loggers');
    });

    await t.test('asumme ready', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'}, {clientOptions});
        table.model(DynamoDM.Schema('emptySchema'));
        const r = table.assumeReady();
        t.equal(r, undefined, 'should return undefined (not be async)');
        t.throws(() => {
            table.model(DynamoDM.Schema('emptySchema'));
        }, 'should prevent adding further schemas');
    });
});

t.test('waiting for table creation', async t => {
    const table = DynamoDM.Table({ name: 'test-table-slow-creation', clientOptions});
    table.model(DynamoDM.Schema('emptySchema'));

    const originalSend = table.client.send;
    let callNumber = 0;

    // to test slow table creation we have to mock the client send command,
    // because dynamodb-local always creates tables instantly.
    const commandSendResults = t.capture(table.docClient, 'send', async function(command){
        if (command instanceof DescribeTableCommand) {
            callNumber += 1;
            // return a dummy 'CREATING' response for the first DescribeTableCommand call
            if (callNumber < 2) {
                return {
                    Table: { TableStatus: 'CREATING' }
                };
            }
        }
        // eslint-disable-next-line
        return originalSend.apply(this, arguments);
    });

    await table.ready();

    t.equal(commandSendResults().length, 3, 'Should wait for success.');
});

t.test('table consistency:', async t => {
    t.test('throws on inconsistent id field names', async t => {
        const table = DynamoDM.Table({ name: 'test-table-errors'});
        table.model(DynamoDM.Schema('schema1'));
        table.model(DynamoDM.Schema('schema2'));
        table.model(DynamoDM.Schema('schema3', {
            properties:{
                idFieldA: DynamoDM.DocIdField
            }
        }));
        t.rejects(table.ready());
        t.throws(() => { table.assumeReady(); });
    });

    t.test('throws on inconsistent type field names', async t => {
        const table = DynamoDM.Table({ name: 'test-table-errors'});
        table.model(DynamoDM.Schema('schema1'));
        table.model(DynamoDM.Schema('schema2', {
            properties:{
                typeFieldA: DynamoDM.TypeField
            }
        }));
        table.model(DynamoDM.Schema('schema3', {
            properties:{
                typeFieldB: DynamoDM.TypeField
            }
        }));
        t.rejects(table.ready());
    });

    t.test('throws on existing table with different table hash key', async t => {
        const table1 = DynamoDM.Table({ name: 'test-table-hashkey1'});
        table1.model(DynamoDM.Schema('schema1'));
        await table1.ready();

        const table2 = DynamoDM.Table({ name: 'test-table-hashkey1'});
        table2.model(DynamoDM.Schema('schema1', {properties:{ different_id: DynamoDM.DocIdField }}));

        t.rejects(
            table2.ready(),
            { message: 'Table test-table-hashkey1 exists with incompatible key schema [{"AttributeName":"id","KeyType":"HASH"}], the schemas require "different_id" to be the hash key.' },
            'should reject if an incompatible table exists'
        );
    });

    t.test('rejects on ready for schema name collisions', async t => {
        const table = DynamoDM.Table({ name: 'test-table-errors'});
        table.model(DynamoDM.Schema('schema1'));
        table.model(DynamoDM.Schema('schema1'));
        t.rejects(table.ready());
    });

    await t.test('rejects on ready after destroyConnection', async t => {
        const table = DynamoDM.Table({ name: 'test-table-errors'});
        table.model(DynamoDM.Schema('schema1'));
        await table.ready();
        table.destroyConnection();
        t.rejects(table.ready(), {message: 'Connection has been destroyed.'}, 'rejects on ready()');
    });

    t.test('incorrect type index', async t => {
        const table1 = DynamoDM.Table({ name: 'incorrect-type-index'});
        const table2 = DynamoDM.Table({ name: 'incorrect-type-index'});
        table1.model(DynamoDM.Schema('schema1', {
            properties: {
                typefield_B: DynamoDM.TypeField
            }
        }));
        table2.model(DynamoDM.Schema('schema1', {
            properties: {
                typefield_A: DynamoDM.TypeField
            }
        }));

        t.teardown(async () => {
            await table1.deleteTable();
            table1.destroyConnection();
            table2.destroyConnection();
        });

        await table1.ready();
        // FIXME: currently this will log a warning, which we cannot easily check for
        await table2.ready();
    });

    await t.test('returns existing model for existing schema', async t => {
        const table = DynamoDM.Table({ name: 'test-table-errors'});
        const schema = DynamoDM.Schema('schema1');
        const m1 = table.model(schema);
        const m2 = table.model(schema);
        t.equal(m1, m2);
        await table.ready();
        const m3 = table.model(schema);
        t.equal(m1, m3);
    });

    await t.test('throws on adding schemas after ready', async t => {
        const table = DynamoDM.Table({ name: 'test-table-errors'});
        const schema1 = DynamoDM.Schema('schema1');
        const schema2 = DynamoDM.Schema('schema2');
        table.model(schema1);
        await table.ready();
        t.throws( () => { table.model(schema2); });
    });

    t.test('throws on invalid table name', async t => {
        t.throws(() => {
            DynamoDM.Table({ name: '1'});
        }, {message:'Invalid table name "1": Must be between 3 and 255 characters long, and may contain only the characters a-z, A-Z, 0-9, \'_\', \'-\', and \'.\'.'}, 'invalid table name as option');

        t.throws(() => {
            DynamoDM.Table('1');
        }, {message:'Invalid table name "1": Must be between 3 and 255 characters long, and may contain only the characters a-z, A-Z, 0-9, \'_\', \'-\', and \'.\'.'}, 'invalid table name as argument');

        t.throws(() => {
            DynamoDM.Table();
        }, {message:'Invalid table name: Must be a string.'}, 'missing table name');

        t.throws(() => {
            DynamoDM.Table({name: null});
        }, {message:'Invalid table name: Must be a string.'}, 'missing table name as option');
        t.end();
    });

    await t.test('throws on invalid index name', async t => {
        const table = DynamoDM.Table({ name: 'test-table-errors'});
        t.throws(() => {
            table.model(DynamoDM.Schema('schema2', {
                properties:{ a: {type:'string'} }
            }, {
                index: {
                    // index names need to be at least three chars for some reason, so this is expected to be invalid
                    a: 1
                }
            }));
        }, /Invalid index name.*/);
    });
    t.end();
});

t.test('wait for index creation', async t => {
    const Schema1_noIndexes = DynamoDM.Schema('1', {
        properties: {
            aString: {type:'string'},
            bNum: {type:'number'},
        }
    });
    const Schema1_withIndexes = DynamoDM.Schema('1', {
        properties: {
            aString: {type:'string'},
            bNum: {type:'number'},
        }
    }, {
        index: {
            aString:1,
            indexWithHashAndSortKey: {
                sortKey:'aString',
                hashKey:'bNum'
            }
        }
    });

    // first create a table, and wait for its built-in indexes to be created
    const table1 = DynamoDM.Table({ name: 'test-index-creation'}, {clientOptions});
    const Model1a = table1.model(Schema1_noIndexes);
    await table1.ready({waitForIndexes:true});

    // and delete it when the test is finished
    t.teardown(async () => {
        await table1.deleteTable();
    });

    // add lots of models, so that creating the indexes for this table takes some amount of time:
    for (let i = 0; i < 200; i++) {
        await (new Model1a({aString:`value ${i}`, bNum:i})).save();
    }
    t.pass('created models ok');

    // now create another reference to the existing table, which requires more indexes, and wait for them:
    const table2 = DynamoDM.Table({ name: 'test-index-creation'}, {clientOptions});
    const Model1b = table2.model(Schema1_withIndexes);

    // wait for the indexes to be created
    await table2.ready({waitForIndexes:true});

    // and check that we can query using the indexes immediately:
    t.equal((await Model1b.queryMany({bNum:4})).length, 1);
    t.equal((await Model1b.queryMany({aString:{$gt: 'value 50'}, bNum:99})).length, 1);

    t.end();
});

t.test('wait for index creation', async t => {
    const Schema1_noIndexes = DynamoDM.Schema('1', {
        properties: {
            aString: {type:'string'},
            bNum: {type:'number'},
        }
    });
    const Schema1_withIndexes = DynamoDM.Schema('1', {
        properties: {
            aString: {type:'string'},
            bNum: {type:'number'},
        }
    }, {
        index: {
            aString:1,
            indexWithHashAndSortKey: {
                sortKey:'aString',
                hashKey:'bNum'
            }
        }
    });

    // first create a table, and wait for its built-in indexes to be created
    const table1 = DynamoDM.Table({ name: 'test-index-creation-2'}, {clientOptions});
    const Model1a = table1.model(Schema1_noIndexes);
    await table1.ready({waitForIndexes:true});

    // and delete it when the test is finished
    t.teardown(async () => {
        await table1.deleteTable();
    });

    // add lots of models, so that creating the indexes for this table takes some amount of time:
    for (let i = 0; i < 200; i++) {
        await (new Model1a({aString:`value ${i}`, bNum:i})).save();
    }
    t.pass('created models ok');

    // now create another reference to the existing table, which requires more indexes, and wait for them:
    const table2 = DynamoDM.Table({ name: 'test-index-creation-2'}, {clientOptions});
    const Model1b = table2.model(Schema1_withIndexes);

    // wait for the indexes to be created
    await table2.ready({waitForIndexes:false});

    // the index shouldn't be ready yet
    t.rejects(Model1b.queryMany({bNum:4}), {message:'The table does not have the specified index: indexWithHashAndSortKey'}, "the index shouldn't be ready yet");

    t.end();
});

t.end();

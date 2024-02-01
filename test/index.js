const t = require('tap');

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
        const table = require('../')({logger: false}).Table({ name: 'test-table-2', clientOptions});
        table.model(DynamoDM.Schema('emptySchema'));
        await table.ready();

        await t.test('call ready multiple times', async () => {
            await table.ready();
        });
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

t.test('table consistency:', async t => {
    await t.test('throws on inconsistent id field names', async t => {
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

    await t.test('throws on inconsistent type field names', async t => {
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

    await t.test('rejects on ready for schema name collisions', async t => {
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

    await t.test('incorrect type index', async t => {
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

    await t.test('throws on invalid table name', async t => {
        t.throws(() => {
            DynamoDM.Table({ name: '1'});
        }, /Invalid table name.*/);
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
});

t.test('crud:', async t => {
    const table = DynamoDM.Table({ name: 'test-table-crud'});
    const ThingSchema = DynamoDM.Schema('namespace.thing', {
        properties: {
            id:           DynamoDM.DocIdField,
            aaaa:         {type: 'string'},
            bbbb:         {type: 'number'},
            cccc:         {type: 'string'},
            blob:         DynamoDM.Binary,
            createdAt:    DynamoDM.CreatedAtField,
            updatedAt:    DynamoDM.UpdatedAtField,
        },
        required: ['id', 'aaaa', 'bbbb'],
        additionalProperties: false
    });
    const Thing = table.model(ThingSchema);

    await table.ready();

    let x, y;
    await t.test('create', async t => {
        x = new Thing({aaaa: 'a', bbbb:1, blob: Buffer.from('hello crud'), });
        await x.save();

        y = await Thing.getById(x.id);
        t.strictSame(y, x);

        t.end();
    });

    await t.test('update', async t => {
        x.bbbb = 2;
        await x.save();

        y = await Thing.getById(x.id);
        t.strictSame(y, x);

        t.end();
    });

    await t.test('remove', async t => {
        const oldId = x.id;
        await x.remove();

        t.equal(await Thing.getById(oldId), null);

        t.end();
    });

    await t.test('creating with duplicate ids should fail', async t => {
        x = new Thing({aaaa: 'a', bbbb:1, blob: Buffer.from('hello crud'), id: 'duplicate'});
        y = new Thing({aaaa: 'b', bbbb:2, blob: Buffer.from('hello crud'), id: 'duplicate'});
        await x.save();
        t.rejects(y.save(), {}, 'creating duplicate');

        t.end();
    });

    await t.test('deleting buffer fields', async t => {
        t.test('delete buffer and re-save', async t => {
            x = new Thing({aaaa: 'a', bbbb:1, blob: Buffer.from('hello crud'), id: 'hadblob1'});
            await x.save();

            delete x.blob;
            await x.save();

            y = await Thing.getById('hadblob1');
            t.equal(y.blob, undefined, 'blob should be gone');

            t.end();
        });

        t.test('delete buffer by setting undefined', async t => {
            x = new Thing({aaaa: 'a', bbbb:1, blob: Buffer.from('hello crud'), id: 'hadblob2'});
            await x.save();

            x.blob = undefined;
            await x.save();

            y = await Thing.getById('hadblob2');
            t.equal(y.blob, undefined, 'blob should be gone');

            t.end();
        });

        t.test('delete buffer by saving over', async t => {
            x = new Thing({aaaa: 'a', bbbb:1, blob: Buffer.from('hello crud'), id: 'hadblob3'});
            await x.save();

            y = await Thing.getById('hadblob3');
            delete y.blob;
            await y.save();

            t.equal(y.blob, undefined, 'blob should be gone');

            y = await Thing.getById('hadblob3');
            t.equal(y.blob, undefined, 'blob should still be gone after load');

            t.end();
        });
    });

    await t.test('deleting string fields', async t => {
        t.test('delete string and re-save', async t => {
            x = new Thing({aaaa: 'a', bbbb:1, cccc: 'hello', id: 'hadstring1'});
            await x.save();

            delete x.cccc;
            await x.save();

            y = await Thing.getById('hadstring1');
            t.equal(y.cccc, undefined, 'cccc should be gone');

            t.end();
        });

        t.test('delete string by setting undefined', async t => {
            x = new Thing({aaaa: 'a', bbbb:1, cccc: 'hello', id: 'hadstring2'});
            await x.save();

            x.cccc = undefined;
            await x.save();

            y = await Thing.getById('hadstring2');
            t.equal(y.cccc, undefined, 'cccc should be gone');

            t.end();
        });

        t.test('delete string by saving over', async t => {
            x = new Thing({aaaa: 'a', bbbb:1, cccc: 'hello', id: 'hadstring3'});
            await x.save();

            y = await Thing.getById('hadstring3');
            delete y.cccc;
            await y.save();

            t.equal(y.cccc, undefined, 'cccc should be gone');

            y = await Thing.getById('hadstring3');
            t.equal(y.cccc, undefined, 'cccc should still be gone after load');

            t.end();
        });
    });

    t.teardown(async () => {
        await table.deleteTable();
        table.destroyConnection();
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
    for (let i = 0; i < 400; i++) {
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

t.end();

const t = require('tap');

const clientOptions = {
    endpoint: 'http://localhost:8000'
};

// get two unique instances by clearing the require cache:
const DynamoDMConstructor1 = require('../');
for (const k in require.cache) {
    delete require.cache[k];
}
const DynamoDMConstructor2 = require('../');

const DynamoDM1 = DynamoDMConstructor1({clientOptions, logger:{level:'error'}});
const DynamoDM2 = DynamoDMConstructor2({clientOptions, logger:{level:'error'}});

t.test('unique identity', async t => {
    t.ok(DynamoDMConstructor1 !== DynamoDMConstructor2, "check that we've created separate module instances for testing");
    t.notOk(DynamoDM1.Schema('test') instanceof DynamoDM2.Schema('test').constructor, 'check that Schema types are unique');
});


t.test('crud with schema from separate module', async t => {
    const table = DynamoDM1.Table({ name: 'test-table-multi-crud'});
    const ThingSchema = DynamoDM2.Schema('namespace.thing', {
        properties: {
            id:           DynamoDM2.DocIdField,
            aaaa:         {type: 'string'},
            bbbb:         {type: 'number'},
            cccc:         {type: 'string'},
            blob:         DynamoDM2.Binary,
            createdAt:    DynamoDM2.CreatedAtField,
            updatedAt:    DynamoDM2.UpdatedAtField
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
    });

    await t.test('update', async t => {
        x.bbbb = 2;
        await x.save();

        y = await Thing.getById(x.id);
        t.strictSame(y, x);
    });

    await t.test('query', async t => {
        const aThing = await Thing.queryOne({ type: 'namespace.thing' });
        t.equal(aThing.constructor, (new Thing({aaaa:'a',bbbb:2})).constructor, 'should have the correct constructor');
    });

    await t.test('remove', async t => {
        const oldId = x.id;
        await x.remove();

        t.equal(await Thing.getById(oldId), null);
    });

    t.teardown(async () => {
        await table.deleteTable();
        table.destroyConnection();
    });

    t.end();
});

t.end();

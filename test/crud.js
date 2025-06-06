const tap = require('tap');

const clientOptions = {
    endpoint: 'http://localhost:8000'
};

const DynamoDMConstructor = require('../');
const DynamoDM = DynamoDMConstructor({clientOptions, logger:{level:'error'}});

tap.test('crud:', async t => {
    const table = DynamoDM.Table({ name: 'test-table-crud'});
    const ThingSchema = DynamoDM.Schema('namespace.thing', {
        properties: {
            id:           DynamoDM.DocIdField,
            aaaa:         {type: 'string'},
            bbbb:         {type: 'number'},
            cccc:         {type: 'string'},
            blob:         DynamoDM.Binary,
            createdAt:    DynamoDM.CreatedAtField,
            updatedAt:    DynamoDM.UpdatedAtField
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

tap.end();

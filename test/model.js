const tap = require('tap');

const clientOptions = {
    endpoint: 'http://localhost:8000'
};

const DynamoDMConstructor = require('../');
const DynamoDM = DynamoDMConstructor({clientOptions, logger:{level:'error'}});

tap.test('model:', async t => {
    const table = DynamoDM.Table({ name: 'test-table-models'});
    const FooSchema = DynamoDM.Schema('namespace.foo', {
        properties: {
            id:           DynamoDM.DocIdField,
            fooVal:       {type: 'number'},
            blob:         DynamoDM.Binary,
            padding:      DynamoDM.Binary
        }
    });
    const BarSchema = DynamoDM.Schema('ambiguous.bar', {
        properties: {
            id:           DynamoDM.DocIdField,
            barVal:       {type: 'number'},
            barValStr:    {type: 'string'},
            blob:         DynamoDM.Binary
        },
        required:['barVal']
    }, {
        index: {
            barValRange: {
                hashKey: 'type',
                sortKey: 'barVal'
            },
            barValRangeStr: {
                hashKey: 'type',
                sortKey: 'barValStr'
            }
        }
    });
    const AmbiguousSchema = DynamoDM.Schema('ambiguous', {
        properties: {
            id:           DynamoDM.DocIdField,
        }
    });
    const Foo = table.model(FooSchema);
    table.model(BarSchema);
    table.model(AmbiguousSchema);

    const all_foos = [];
    for (let i = 0; i < 50; i++ ) {
        // padd the Foo items out to 350KBk each, so that we can test bumping up against dynamoDB's 16MB response limit
        let foo = new Foo({fooVal:i, blob: Buffer.from(`hello query ${i}`), padding: Buffer.alloc(3.5E5)});
        all_foos.push(foo);
        await foo.save();
    }

    t.after(async () => {
        await table.deleteTable();
        table.destroyConnection();
    });


    t.test('getById', async t => {
        t.rejects(Foo.getById(null), 'should reject null id');
        t.rejects(Foo.getById('someid', {foo:1}), 'should reject invalid option');
        t.rejects(Foo.getById(''), 'should reject empty id');
        t.rejects(Foo.getById(123), 'should reject numeric id');
        t.end();
    });

    t.test('getById options', async t => {
        t.test('ConsistentRead: true', async t => {
            const foo = await Foo.getById(all_foos[0].id, {ConsistentRead: true});
            t.ok(foo, 'should return a foo');
            t.equal(foo.constructor, Foo, 'should return the correct type');
            t.end();
        });
        t.test('ConsistentRead: false', async t => {
            const foo = await Foo.getById(all_foos[0].id, {ConsistentRead: false});
            t.ok(foo, 'should return a foo');
            t.equal(foo.constructor, Foo, 'should return the correct type');
            t.end();
        });

        t.test('empty options', async t => {
            const foo = await Foo.getById(all_foos[0].id, {});
            t.ok(foo, 'should return a foo');
            t.equal(foo.constructor, Foo, 'should return the correct type');
            t.end();
        });

        t.end();
    });

    t.test('table.getById', async t => {
        t.rejects(table.getById('blegh.someid'), new Error('Table has no matching model type for id "blegh.someid", so it cannot be loaded.'), 'should reject unknown type');
        t.rejects(table.getById('ambiguous.bar.someid'), new Error('Table has multiple ambiguous model types for id "ambiguous.bar.someid", so it cannot be loaded generically.'), 'should reject ambiguous type');
        const foo = await table.getById(all_foos[0].id);
        t.equal(foo.constructor, Foo, 'should get the correct type');
        t.equal(foo.id, all_foos[0].id, 'should get the correct document');
    });

    t.test('getByIds', async t => {
        t.rejects(Foo.getByIds([null]), 'should reject null id');
        t.rejects(Foo.getByIds(['someid'], {foo:1}), 'should reject invalid option');
        t.match(await Foo.getByIds(['nonexistent']), [null], 'should return null for nonexistent id');
        t.match(await Foo.getByIds(['nonexistent', all_foos[0].id]), [null, all_foos[0]], 'should return null along with extant model');
        const foos = await Foo.getByIds(all_foos.map(f => f.id));
        t.equal(foos.length, all_foos.length, 'should return all models');
        t.match(foos.map(f => f?.id), all_foos.map(f => f?.id), 'should return all models in order');
        t.rejects(Foo.getByIds(''), new Error('Invalid ids: must be array of strings of nonzero length.'), 'should reject non-array argument');
        t.end();
    });

    t.test('getByIds options', async t => {
        t.test('ConsistentRead: true', async t => {
            const foos = await Foo.getByIds([all_foos[0].id, all_foos[1].id], {ConsistentRead: true});
            t.equal(foos.length, 2, 'should return the right number of foos');
            t.equal(foos[0].constructor, Foo, 'should return the correct type');
            t.end();
        });
        t.test('ConsistentRead: false', async t => {
            const foos = await Foo.getByIds([all_foos[0].id, all_foos[1].id], {ConsistentRead: false});
            t.equal(foos.length, 2, 'should return the right number of foos');
            t.equal(foos[0].constructor, Foo, 'should return the correct type');
            t.end();
        });

        t.test('empty options', async t => {
            const foos = await Foo.getByIds([all_foos[0].id, all_foos[1].id], {});
            t.equal(foos.length, 2, 'should return the right number of foos');
            t.equal(foos[0].constructor, Foo, 'should return the correct type');
            t.end();
        });
        t.end();
    });

    t.test('getByIds exceeding retries', async t => {
        const table2 = DynamoDM.Table({ name: 'test-table-models', retry: { maxRetries:0 }});
        t.after(async () => { table2.destroyConnection(); });
        const Foo2 = table2.model(FooSchema);
        t.rejects(Foo2.getByIds(all_foos.map(f => f.id)), {message:'Request failed: maximum retries exceeded.'}, 'getByIds with a large number of large responses should require retries for BatchGetCommand.');
        t.end();
    });

    t.test('aborting getByIds', async t => {
        const ac0 = new AbortController();
        ac0.abort(new Error('my reason 0 '));
        // the AWS SDk doesn't propagate the abort reason (but it would be nice if it did in the future)
        t.rejects(Foo.getByIds(all_foos.map(f => f.id), {abortSignal: ac0.signal}), {name:'AbortError', message:'Request aborted'}, 'getByIds should be abortable with an AbortController that is already aborted');

        const ac1 = new AbortController();
        // the AWS SDk doesn't propagate the abort reason (but it would be nice if it did in the future)
        t.rejects(Foo.getByIds(all_foos.map(f => f.id), {abortSignal: ac1.signal}), {name:'AbortError', message:'Request aborted'}, 'getByIds should be abortable with an AbortController signal immediately');
        ac1.abort(new Error('my reason'));

        const ac2 = new AbortController();
        t.rejects(Foo.getByIds(all_foos.map(f => f.id), {abortSignal: ac2.signal}), {name:'AbortError', message:'Request aborted'}, 'getByIds should be abortable with an AbortController signal asynchronously');
        setTimeout(() => {
            ac2.abort(new Error('my reason 2'));
        }, 1);
        t.end();
    });

    t.test('aborting getById', async t => {
        const ac0 = new AbortController();
        ac0.abort(new Error('my reason 0 '));
        t.rejects(Foo.getById(all_foos[0].id, {abortSignal: ac0.signal}), {name:'AbortError', message:'Request aborted'}, 'getById should be abortable with an AbortController that is already aborted');

        const ac1 = new AbortController();
        t.rejects(Foo.getById(all_foos[0].id, {abortSignal: ac1.signal}), {name:'AbortError', message:'Request aborted'}, 'getById should be abortable with an AbortController signal immediately');
        ac1.abort(new Error('my reason'));

        const ac2 = new AbortController();
        t.rejects(Foo.getById(all_foos[0].id, {abortSignal: ac2.signal}), {name:'AbortError', message:'Request aborted'}, 'getById should be abortable with an AbortController signal asynchronously');
        setTimeout(() => {
            ac2.abort(new Error('my reason 2'));
        }, 1);
        t.end();
    });
});

tap.end();

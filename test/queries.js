const t = require('tap');

const clientOptions = {
    endpoint: 'http://localhost:8000'
};

const DynamoDMConstructor = require('../');
const DynamoDM = DynamoDMConstructor({clientOptions, logger:{level:'error'}});

async function arrayFromAsync(iter) {
    const r = [];
    for await (const x of iter) {
      r.push(x);
    }
    return r;
}

t.test('queries:', async t => {
    const table = DynamoDM.Table({ name: 'test-table-queries'});
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
    const IndexedStringSchema = DynamoDM.Schema('namespace.indexedString', {
        properties: {
            someString: {type: 'string'},
            someOtherString: {type: 'string'},
            someN: {type: 'number'}
        }
    }, {
        index: {
            someString: 1,
            someOtherString: 1
        }
    });
    const IndexedTsSchema = DynamoDM.Schema('namespace.indexedTs', {
        properties: {
            someTs: DynamoDM.Timestamp,
            someN: {type: 'number'}
        }
    }, {
        index: {
            someTs: 1
        }
    });
    const IncompatibleStringSchema = DynamoDM.Schema('namespace.incompatibleS', {
        properties: {
            someString: {type: 'number'},
        }
    }, {
        index: {
            someString: 1
        }
    });
    const IncompatibleString2Schema = DynamoDM.Schema('namespace.incompatibleS', {
        properties: {
            someString: {type: 'number'},
            foo: {type: 'string'}
        }
    }, {
        index: {
            someString: {
                hashKey: 'foo'
            }
        }
    });
    const CompatibleExtraStringSchema = DynamoDM.Schema('namespace.otherString', {
        properties: {
            otherString: {type: 'string'},
            num: {type: 'number'}
        }
    }, {
        index: {
            otherString: {
                hashKey: 'otherString',
                sortKey: 'num'
            }
        }
    });
    const IndexedNumberAndBinarySchema = DynamoDM.Schema('namespace.indexedNB', {
        properties: {
            num: {type: 'number'},
            blob: DynamoDM.Binary,
        }
    }, {
        index: {
            num: 1,
            myBinaryIndex: {
                hashKey: 'blob',
                sortKey: 'num'
            },
            myNumIndex: {
                hashKey: 'num',
                sortKey: 'blob'
            }
        }
    });
    const Foo = table.model(FooSchema);
    const Bar = table.model(BarSchema);
    const Ambiguous = table.model(AmbiguousSchema);
    const IndexedString = table.model(IndexedStringSchema);
    const IndexedTs = table.model(IndexedTsSchema);
    const IndexedNumberAndBinary = table.model(IndexedNumberAndBinarySchema);

    // this should be creating indexes
    await table.ready({waitForIndexes: true});

    const all_foos = [];
    for (let i = 0; i < 50; i++ ) {
        // padd the Foo items out to 350KBk each, so that we can test bumping up against dynamoDB's 16MB response limit
        let foo = new Foo({fooVal:i, blob: Buffer.from(`hello query ${i}`), padding: Buffer.alloc(3.5E5)});
        all_foos.push(foo);
        await foo.save();
    }
    const N = 10;
    for (let i = 0; i < N; i++) {
        await new Bar({barVal:i, barValStr:`bar value ${i%3}${i}`, blob: Buffer.from(`hello query ${i}`), }).save();
        await new IndexedString({someN:i, someString:`string number ${i}`, someOtherString:'constant value' }).save();
        await new IndexedTs({someN:i, someTs:(new Date(i*1e7)) }).save();
        await new IndexedNumberAndBinary({num:i, blob: Buffer.from(`hello query ${i%4}`), }).save();
        await new Ambiguous().save();
    }

    t.after(async () => {
        await table.deleteTable();
        table.destroyConnection();
    });

    // TODO: listAllIds removed from API for now
    //
    // await t.test('listAllIds', async t => {
    //     const allFoos = [];
    //     for await (const x of Foo.listAllIds({Limit:2})) {
    //         allFoos.push(x);
    //         t.equal(x.split('.')[1], 'foo');
    //     }

    //     const allBars = [];
    //     for await (const x of Bar.listAllIds()) {
    //         allBars.push(x);
    //         t.equal(x.split('.')[1], 'bar');
    //     }

    //     t.equal(allFoos.length, all_foos.length);
    //     t.equal(allBars.length, 10);
    //     t.ok(allFoos.every(x => x.split('.')[1] === 'foo'));
    //     t.ok(allBars.every(x => x.split('.')[1] === 'bar'));

    //     t.end();
    // });

    await t.test('rawQueryOneId', async t => {
        t.test('on type index', async t => {
            const foo_id = await Foo.rawQueryOneId({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.foo' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            });
            t.equal(foo_id.split('.')[1], 'foo');

            const nonExistent = await Foo.rawQueryOneId({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.doesNotExist' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            });
            t.equal(nonExistent, null, 'returns null for no matches');
        });
        t.test('on string index ', async t => {
            const indexedString_id = await IndexedString.rawQueryOneId({
                IndexName:'someString',
                KeyConditionExpression:'someString = :value',
                ExpressionAttributeValues: { ':value': 'string number 4' }
            });
            t.equal(indexedString_id.split('.')[1], 'indexedString');
            t.equal((await IndexedString.getById(indexedString_id)).someN, 4, 'should have other properties set correctly');
        });
        t.test('on binary index ', async t => {
            const nb_id = await IndexedNumberAndBinary.rawQueryOneId({
                IndexName:'myBinaryIndex',
                // 'blob' is a banned name, so need to use ExpressionAttributeNames...
                KeyConditionExpression:'#fieldName = :value',
                ExpressionAttributeValues: { ':value': Buffer.from('hello query 3') },
                ExpressionAttributeNames: { '#fieldName': 'blob' }
            });
            t.equal(nb_id.split('.')[1], 'indexedNB');
            t.equal((await IndexedNumberAndBinary.getById(nb_id)).num, 3, 'should return the lowest in sorted order');
        });
    });

    await t.test('getById', t => {
        t.rejects(Foo.getById(null), 'should reject null id');
        t.rejects(Foo.getById('someid', {foo:1}), 'should reject invalid option');
        t.rejects(Foo.getById(''), 'should reject empty id');
        t.rejects(Foo.getById(123), 'should reject numeric id');
        t.end();
    });

    await t.test('table.getById', async t => {
        t.rejects(table.getById('blegh.someid'), new Error('Table has no matching model type for id "blegh.someid", so it cannot be loaded.'), 'should reject unknown type');
        t.rejects(table.getById('ambiguous.bar.someid'), new Error('Table has multiple ambiguous model types for id "ambiguous.bar.someid", so it cannot be loaded generically.'), 'should reject ambiguous type');
        const foo = await table.getById(all_foos[0].id);
        t.equal(foo.constructor, Foo, 'should get the correct type');
        t.equal(foo.id, all_foos[0].id, 'should get the correct document');
    });

    await t.test('getByIds', async t => {
        await t.rejects(Foo.getByIds([null]), 'should reject null id');
        t.rejects(Foo.getByIds(['someid'], {foo:1}), 'should reject invalid option');
        t.match(await Foo.getByIds(['nonexistent']), [null], 'should return null for nonexistent id');
        t.match(await Foo.getByIds(['nonexistent', all_foos[0].id]), [null, all_foos[0]], 'should return null along with extant model');
        const foos = await Foo.getByIds(all_foos.map(f => f.id));
        t.equal(foos.length, all_foos.length, 'should return all models');
        t.match(foos.map(f => f?.id), all_foos.map(f => f?.id), 'should return all models in order');
        await t.rejects(Foo.getByIds(''), new Error('Invalid ids: must be array of strings of nonzero length.'), 'should reject non-array argument');
        t.end();
    });

    t.test('getByIds exceeding retries', async t => {
        const table2 = DynamoDM.Table({ name: 'test-table-queries', retry: { maxRetries:0 }});
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

    await t.test('rawQueryManyIds', async t => {
        t.test('on type index', async t => {
            const foo_ids = await Foo.rawQueryManyIds({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.foo' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            });
            t.equal(foo_ids.length, all_foos.length, 'should return all N of this type');
            t.match(foo_ids, all_foos.map(f => f.id), 'should return the correct Ids');
        });
        t.test('on string index ', async t => {
            const indexedStringIds = await IndexedString.rawQueryManyIds({
                IndexName:'someString',
                KeyConditionExpression:'someString = :value',
                ExpressionAttributeValues: { ':value': 'string number 4' }
            });
            t.equal(indexedStringIds.length, 1, 'should return one match');
            t.equal((await IndexedString.getById(indexedStringIds[0])).someN, 4, 'should return the correct item');
        });
        t.test('on binary index ', async t => {
            const nb = await IndexedNumberAndBinary.rawQueryManyIds({
                IndexName:'myBinaryIndex',
                KeyConditionExpression:'#fieldName = :value',
                ExpressionAttributeValues: { ':value': Buffer.from('hello query 3') },
                ExpressionAttributeNames: { '#fieldName': 'blob' }
            });
            t.equal(nb.length, 2, 'should return two matches');
            t.equal((await IndexedNumberAndBinary.getById(nb[0])).num, 3, 'should return the lowest in sorted order');
        });
        t.end();
    });

    await t.test('rawQueryIteratorIds', async t => {
        t.test('on type index', async t => {
            const foo_ids = await arrayFromAsync(Foo.rawQueryIteratorIds({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.foo' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            }));
            t.equal(foo_ids.length, all_foos.length, 'should return all N of this type');
            t.match(foo_ids, all_foos.map(f => f.id), 'should return the correct Ids');
        });
        t.test('on string index ', async t => {
            const indexedStringIds = await arrayFromAsync(IndexedString.rawQueryIteratorIds({
                IndexName:'someString',
                KeyConditionExpression:'someString = :value',
                ExpressionAttributeValues: { ':value': 'string number 4' }
            }));
            t.equal(indexedStringIds.length, 1, 'should return one match');
            t.equal((await IndexedString.getById(indexedStringIds[0])).someN, 4, 'should return the correct item');
        });
        t.test('on binary index ', async t => {
            const nb = await arrayFromAsync(IndexedNumberAndBinary.rawQueryIteratorIds({
                IndexName:'myBinaryIndex',
                KeyConditionExpression:'#fieldName = :value',
                ExpressionAttributeValues: { ':value': Buffer.from('hello query 3') },
                ExpressionAttributeNames: { '#fieldName': 'blob' }
            }));
            t.equal(nb.length, 2, 'should return two matches');
            t.equal((await IndexedNumberAndBinary.getById(nb[0])).num, 3, 'should return the lowest in sorted order');
        });
        t.test('with limit', async t => {
            const foo_ids = await arrayFromAsync(Foo.rawQueryIteratorIds({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.foo' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            }, {limit: 7}));
            t.ok(N > 7, 'N needs to be > 7 for this test');
            t.equal(foo_ids.length, 7, 'should return all N of this type');
            t.match(foo_ids, all_foos.slice(0,7).map(f => f.id), 'should return the correct Ids');
        });
        t.end();
    });

    await t.test('query api', async t => {
        // TODO:
        //  * tests for $gt $lt conditions on hash-only indexes to make sure we reject with a nice error (currently the error comes from dynamodb doc client and is not very helpful)
        t.test('queryMany', async t => {
            t.test('on type index', async t => {
                const foos = await Foo.queryMany({ type: 'namespace.foo' });
                t.equal(foos.length, all_foos.length, 'should return all N of this type');
                t.equal(foos[0].constructor, (new Foo()).constructor, 'should have the correct constructor');
                t.rejects(Foo.queryMany({ type: 123 }), {message:'Value does not match schema for type:  must be string.'}, 'passing an incompatible value should reject');
            });
            t.test('on string index ', async t => {
                const indexedStrings = await IndexedString.queryMany({someString: 'string number 4'});
                t.equal(indexedStrings.length, 1, 'should return one match');
                t.equal(indexedStrings[0].constructor, (new IndexedString()).constructor, 'should have the correct constructor');
                t.equal(indexedStrings[0].someN, 4, 'should have other properties set correctly');
            });
            t.test('on ts index ', async t => {
                const indexedStrings = await IndexedTs.queryMany({someTs: new Date(2e7)});
                t.equal(indexedStrings.length, 1, 'should return one match');
                t.equal(indexedStrings[0].constructor, (new IndexedTs()).constructor, 'should have the correct constructor');
                t.equal(indexedStrings[0].someN, 2, 'should have other properties set correctly');
            });
            t.test('on binary index ', async t => {
                const nb = await IndexedNumberAndBinary.queryMany({ 'blob': Buffer.from('hello query 3') });
                t.equal(nb.length, 2, 'should return two matches');
                t.equal(nb[0].constructor, (new IndexedNumberAndBinary()).constructor, 'should have correct constructor');
                t.equal(nb[0].num, 3, 'should return the lowest in sorted order');
            });
            t.test('on binary index with sort ', async t => {
                const nb = await IndexedNumberAndBinary.queryMany({ 'blob': Buffer.from('hello query 3'), 'num':7 });
                t.equal(nb.length, 1, 'should return one match');
                t.equal(nb[0].constructor, (new IndexedNumberAndBinary()).constructor, 'should have correct constructor');
                t.equal(nb[0].num, 7, 'should return the matching item');
            });
            t.test('invalid queries', async t => {
                t.rejects(Foo.queryMany({type: 'namespace.foo', fooVal:3 }), {message:'Unsupported query: "{ type: \'namespace.foo\', fooVal: 3 }". No index found for query fields [type, fooVal]'}, 'rejects non-queryable extra parameters');
                t.rejects(IndexedNumberAndBinary.queryMany({'blob': Buffer.from('hello query 3'), 'num':7, id:'123' }), {message:'Unsupported query: "{ blob: <Buffer 68 65 6c 6c 6f 20 71 75 65 72 79 20 33>, num: 7, id: \'123\' }" Queries must have at most two properties to match against index hash and range attributes.'}, 'rejects more than two parameters');
                t.rejects(IndexedNumberAndBinary.queryMany({num:{$gt:0, $lt:2}}), {message:'Only a single $gt/$gte/$lt/$lte/$between/$begins condition is supported in the simple query api.'}, 'rejects multiple conditions');
                t.rejects(IndexedNumberAndBinary.queryMany({num:{$gt:0}, blob:{$gte:Buffer.from('hello query 3')}}), {message:'Unsupported query: "{ num: { \'$gt\': 0 }, blob: { \'$gte\': <Buffer 68 65 6c 6c 6f 20 71 75 65 72 79 20 33> } }" Queries must include an equality condition for the index hash key.'}, 'rejects multiple conditions on separate keys');
                t.rejects(IndexedNumberAndBinary.queryMany({type:{$gt:'foo'}}), {message:'Unsupported query: "{ type: { \'$gt\': \'foo\' } }" Queries must include an equality condition for the index hash key.'}, 'rejects $conditions on hash index with a useful error message');

                t.rejects(Bar.queryMany({ type:'ambiguous.bar', barVal: { $between: {a:1} } }), {message:'Condition "$between" in query requires an array of 2 values.'}, 'rejects non-array value for between');
                t.rejects(Bar.queryMany({ type:'ambiguous.bar', barVal: { $between: [7] } }), {message:'Condition "$between" in query requires an array of 2 values.'}, 'rejects incorrect number of values');
                t.rejects(Bar.queryMany({ type:'ambiguous.bar', barVal: { $between: [7,9,10] } }), {message:'Condition "$between" in query requires an array of 2 values.'}, 'rejects incorrect number of values');
            });
            t.test('multiple possible indexes', async t => {
                const nb = await IndexedNumberAndBinary.queryMany({ num:7 });
                t.equal(nb.length, 1, 'should return one match');
                t.equal(nb[0].constructor, (new IndexedNumberAndBinary()).constructor, 'should have correct constructor');
                t.equal(nb[0].num, 7, 'should return correct item');
            });
            t.test('query conditions', async t => {
                t.test('$gt', async t => {
                    const r = await Bar.queryMany({ type:'ambiguous.bar', barVal: { $gt: 7 } });
                    t.equal(r.length, N-8, `should return ${N-8} matches`);
                    t.equal(r[0].constructor, Bar, 'should have correct constructor');
                    t.equal(r[0].barVal, 8, 'should return in ascending order');
                    t.end();
                });
                t.test('$gte', async t => {
                    const r = await Bar.queryMany({ type:'ambiguous.bar', barVal: { $gte: 7 } });
                    t.equal(r.length, N-7, `should return ${N-7} matches`);
                    t.equal(r[0].constructor, Bar, 'should have correct constructor');
                    t.equal(r[0].barVal, 7, 'should return in ascending order');
                    t.end();
                });
                t.test('$lt', async t => {
                    const r = await Bar.queryMany({ type:'ambiguous.bar', barVal: { $lt: 7 } });
                    t.equal(r.length, 7, 'should return 7 matches');
                    t.equal(r[0].constructor, Bar, 'should have correct constructor');
                    t.equal(r[0].barVal, 0, 'should return in ascending order');
                    t.end();
                });
                t.test('$lte', async t => {
                    const r = await Bar.queryMany({ type:'ambiguous.bar', barVal: { $lte: 7 } });
                    t.equal(r.length, 8, 'should return 8 matches');
                    t.equal(r[0].constructor, Bar, 'should have correct constructor');
                    t.equal(r[0].barVal, 0, 'should return in ascending order');
                    t.end();
                });
                t.test('$between', async t => {
                    const r = await Bar.queryMany({ type:'ambiguous.bar', barVal: { $between: [7,9] } });
                    t.equal(r.length, 3, 'should include the extremes');
                    t.equal(r[0].constructor, Bar, 'should have correct constructor');
                    t.equal(r[0].barVal, 7, 'should return in ascending order');
                    t.end();
                });
                t.test('$begins', async t => {
                    const r = await Bar.queryMany({ type:'ambiguous.bar', barValStr: { $begins: 'bar value 2' } });
                    t.equal(r.length, Math.floor(N/3), `should return ${Math.floor(N/3)} matches`);
                    t.equal(r[0].constructor, Bar, 'should have correct constructor');
                    t.equal(r[0].barVal, 2, 'should return in ascending order');
                    t.end();
                });
                await t.test('$unsupported', async t => {
                    await t.rejects(Bar.queryMany({ type:'ambiguous.bar', barVal: { $unsupported: 7 } }), {message:'Condition "$unsupported" is not supported.'}, 'should reject unsupported $condition');
                });
                t.end();
            });
            t.test('aborting queryMany', async t => {
                const ac0 = new AbortController();
                ac0.abort(new Error('my reason 0'));
                t.rejects(Foo.queryMany({ type: 'namespace.foo' }, {abortSignal: ac0.signal}), {name:'AbortError', message:'Request aborted'}, 'queryMany should be abortable with an AbortController that is already aborted');

                const ac1 = new AbortController();
                // the AWS SDk doesn't propagate the abort reason (but it would be nice if it did in the future)
                t.rejects(Foo.queryMany({ type: 'namespace.foo' }, {abortSignal: ac1.signal}), {name:'AbortError', message:'Request aborted'}, 'queryMany should be abortable with an AbortController signal immediately');
                ac1.abort(new Error('my reason'));

                const ac2 = new AbortController();
                t.rejects(Foo.queryMany({ type: 'namespace.foo' }, {abortSignal: ac2.signal}), {name:'AbortError', message:'Request aborted'}, 'queryMany should be abortable with an AbortController signal asynchronously');
                setTimeout(() => {
                    ac2.abort(new Error('my reason 2'));
                }, 1);

                // check that aborting after completion doesn't do anything bad:
                const ac3 = new AbortController();
                const foos = await Foo.queryMany({ type: 'namespace.foo' }, {abortSignal: ac3.signal});
                ac3.abort(new Error('my reason 3'));
                t.ok(foos[0] instanceof Foo, 'should have still returned genuine bona fide Foos');

                t.end();
            });
            t.test('with startAfter on hash and sort index', async t => {
                let startAfter;
                let count = 0;
                while(count < all_foos.length) {
                    const models = await Foo.queryMany({type: 'namespace.foo'}, {limit:7, ...(startAfter && {startAfter}) });
                    t.equal(models.length, Math.min(7, all_foos.length - count), '.limit:7, startAfter:previous, should always return the right number of models until there are none left');
                    t.equal(models[0].type, 'namespace.foo', 'should return models of the right type');
                    startAfter = models[models.length-1];
                    count += models.length;
                }
                const no_models = await Foo.queryMany({type: 'namespace.foo'}, {limit:1,  startAfter });
                t.equal(no_models.length, 0, 'should not return anything on startAfter last');
                t.end();
            });
            t.test('with startAfter on hash-only index', async t => {
                let startAfter;
                let count = 0;
                while(count < N) {
                    const models = await IndexedString.queryMany({someOtherString: 'constant value'}, {limit:1, ...(startAfter && {startAfter}) });
                    t.equal(models.length, 1, '.limit:1, startAfter:previous, should always return a model');
                    t.equal(models[0].type, 'namespace.indexedString', 'should return models of the right type');
                    startAfter = models[0];
                    count += models.length;
                }
                const no_models = await IndexedString.queryMany({someOtherString: 'constant value'}, {limit:1, startAfter });
                t.equal(no_models.length, 0, 'should not return anything on startAfter last');
                t.end();
            });
            await t.test('with invalid startAfter', async t => {
                await t.rejects(Foo.queryMany({type: 'namespace.foo'}, {limit:1, startAfter: {id:'123', type:'namespace.foo'} }), {message:'options.startAfter must be a Model_namespace.foo model instance. To specify ExclusiveStartKey directly use options.rawQueryOptions.ExclusiveStartKey instead.'}, 'should reject non-instance startAfter');

                t.end();
            });
            t.end();
        });

        t.test('queryManyIds', async t => {
            const allFooIds = await Foo.queryManyIds({ type: 'namespace.foo' });
            t.match(allFooIds.sort(), all_foos.map(f => f.id).sort(), 'should return all IDs');
            t.end();
        });

        t.test('queryOne', async t => {
            const aFoo = await Foo.queryOne({ type: 'namespace.foo' });
            t.equal(aFoo.constructor, (new Foo()).constructor, 'should have the correct constructor');
            t.end();
        });
        t.test('aborting queryOne', async t => {
            const ac0 = new AbortController();
            ac0.abort(new Error('my reason 0'));
            t.rejects(Foo.queryOne({ type: 'namespace.foo' }, {abortSignal: ac0.signal}), {name:'AbortError', message:'Request aborted'}, 'queryOne should be abortable with an AbortController that is already aborted');

            const ac1 = new AbortController();
            // the AWS SDk doesn't propagate the abort reason (but it would be nice if it did in the future)
            t.rejects(Foo.queryOne({ type: 'namespace.foo' }, {abortSignal: ac1.signal}), {name:'AbortError', message:'Request aborted'}, 'queryOne should be abortable with an AbortController signal immediately');
            ac1.abort(new Error('my reason'));

            const ac2 = new AbortController();
            t.rejects(Foo.queryOne({ type: 'namespace.foo' }, {abortSignal: ac2.signal}), {name:'AbortError', message:'Request aborted'}, 'queryOne should be abortable with an AbortController signal asynchronously');
            setTimeout(() => {
                ac2.abort(new Error('my reason 2'));
            }, 1);

            // check that aborting after completion doesn't do anything bad:
            const ac3 = new AbortController();
            const aFoo = await Foo.queryOne({ type: 'namespace.foo' }, {abortSignal: ac3.signal});
            ac3.abort(new Error('my reason 3'));
            t.ok(aFoo instanceof Foo, 'should have still returned a genuine bona fide Foo');

            t.end();
        });

        t.test('queryOneId', async t => {
            const aFooId = await Foo.queryOneId({ type: 'namespace.foo' });
            t.ok(all_foos.filter(x => x.id === aFooId).length === 1, 'should return a foo ID');
            t.end();
        });
        await t.test('aborting queryOneId', async t => {
            const ac1 = new AbortController();
            // the AWS SDk doesn't propagate the abort reason (but it would be nice if it did in the future)
            t.rejects(Foo.queryOneId({ type: 'namespace.foo' }, {abortSignal: ac1.signal}), {name:'AbortError', message:'Request aborted'}, 'queryOneId should be abortable with an AbortController signal immediately');
            ac1.abort(new Error('my reason'));

            const ac2 = new AbortController();
            t.rejects(Foo.queryOneId({ type: 'namespace.foo' }, {abortSignal: ac2.signal}), {name:'AbortError', message:'Request aborted'}, 'queryOneId should be abortable with an AbortController signal asynchronously');
            setTimeout(() => {
                ac2.abort(new Error('my reason 2'));
            }, 0);
            t.end();
        });

        await t.test('querying via a model of the wrong type', async t => {
            t.rejects(Bar.queryMany({type:'namespace.foo'}), {message:'Document does not match schema for ambiguous.bar. The loaded document has a different type "namespace.foo", and the schema is incompatible:  must have required property \'barVal\'.'}, 'should throw if the query returns a document of an incompatible type');
            t.end();
        });

        t.end();
    });

    t.test('options validation', async t => {
        t.test('queryOne', async t => {
            t.rejects(Foo.queryOne({ type: 'namespace.foo' }, {invalidOption:123}), {message:"Invalid options: [ { instancePath: '', schemaPath: '#/additionalProperties', keyword: 'additionalProperties', params: { additionalProperty: 'invalidOption' }, message: 'must NOT have additional properties' } ]."}, 'rejects invalid option');
            t.rejects(Foo.queryOne({ type: 'namespace.foo' }, {limit:2}), {message:"Invalid options: [ { instancePath: '/limit', schemaPath: '#/properties/limit/const', keyword: 'const', params: { allowedValue: 1 }, message: 'must be equal to constant' } ]"}, 'rejects incorrect option value');
            t.rejects(Foo.queryOne({ type: 'namespace.foo' }, {startAfter:'somestring'}), {message:"Invalid options: [ { instancePath: '/startAfter', schemaPath: '#/properties/startAfter/type', keyword: 'type', params: { type: 'object' }, message: 'must be object' } ]."}, 'rejects incorrect option type');
            t.end();
        });
        t.test('queryOneId', async t => {
            t.rejects(Foo.queryOneId({ type: 'namespace.foo' }, {rawFetchOptions:{}}), {message:"Invalid options: [ { instancePath: '', schemaPath: '#/additionalProperties', keyword: 'additionalProperties', params: { additionalProperty: 'rawFetchOptions' }, message: 'must NOT have additional properties' } ]."}, 'rejects invalid option');
            t.rejects(Foo.queryOneId({ type: 'namespace.foo' }, {limit:2}), {message:"Invalid options: [ { instancePath: '/limit', schemaPath: '#/properties/limit/const', keyword: 'const', params: { allowedValue: 1 }, message: 'must be equal to constant' } ]"}, 'rejects incorrect option value');
            t.rejects(Foo.queryOneId({ type: 'namespace.foo' }, {startAfter:'somestring'}), {message:"Invalid options: [ { instancePath: '/startAfter', schemaPath: '#/properties/startAfter/type', keyword: 'type', params: { type: 'object' }, message: 'must be object' } ]."}, 'rejects incorrect option type');
            t.end();
        });
        t.test('queryMany', async t => {
            t.rejects(Foo.queryMany({ type: 'namespace.foo' }, {invalidOption:123}), {message:"Invalid options: [ { instancePath: '', schemaPath: '#/additionalProperties', keyword: 'additionalProperties', params: { additionalProperty: 'invalidOption' }, message: 'must NOT have additional properties' } ]."}, 'rejects invalid option');
            t.rejects(Foo.queryMany({ type: 'namespace.foo' }, {startAfter:'somestring'}), {message:"Invalid options: [ { instancePath: '/startAfter', schemaPath: '#/properties/startAfter/type', keyword: 'type', params: { type: 'object' }, message: 'must be object' } ]."}, 'rejects incorrect option type');
            t.end();
        });
        t.test('queryManyIds', async t => {
            t.rejects(Foo.queryManyIds({ type: 'namespace.foo' }, {rawFetchOptions:{}}), {message:"Invalid options: [ { instancePath: '', schemaPath: '#/additionalProperties', keyword: 'additionalProperties', params: { additionalProperty: 'rawFetchOptions' }, message: 'must NOT have additional properties' } ]."}, 'rejects invalid option');
            t.rejects(Foo.queryManyIds({ type: 'namespace.foo' }, {startAfter:'somestring'}), {message:"Invalid options: [ { instancePath: '/startAfter', schemaPath: '#/properties/startAfter/type', keyword: 'type', params: { type: 'object' }, message: 'must be object' } ]."}, 'rejects incorrect option type');
            t.end();
        });
        await t.end();
    });

    await t.test('add missing indexes', async t => {
        const table3 = DynamoDM.Table({ name: 'test-compatible-indexes'});
        t.teardown(async () => {
            await table3.deleteTable();
            table3.destroyConnection();
        });
        await t.test('adding incompatible index types', async t => {
            const table2 = DynamoDM.Table({ name: 'test-table-incompidxtypes'});
            table2.model(FooSchema);
            table2.model(BarSchema);
            table2.model(IndexedStringSchema);
            table2.model(IndexedNumberAndBinarySchema);

            table2.model(IncompatibleStringSchema);
            table2.model(CompatibleExtraStringSchema);

            await t.rejects(table2.ready(), /Schema\(s\) .* define incompatible types \(S,N\) for "\.someString" in index\(es\) "someString, someString"\./, '.ready() should fail with incompatible index types');
        });

        await t.test('adding incompatible indexes', async t => {
            const table2 = DynamoDM.Table({ name: 'test-incompat-idx'});
            table2.model(FooSchema);
            table2.model(BarSchema);
            table2.model(IndexedStringSchema);
            table2.model(IndexedNumberAndBinarySchema);

            table2.model(IncompatibleString2Schema);
            table2.model(CompatibleExtraStringSchema);

            await t.rejects(table2.ready(), /Schema\(s\) .* define incompatible versions of index "someString"\./, '.ready() should fail with incompatible indexes');
        });

        await t.test('adding compatible indexes', async t => {
            table3.model(FooSchema);
            table3.model(BarSchema);
            table3.model(IndexedStringSchema);
            table3.model(IndexedNumberAndBinarySchema);

            const CompatibleExtraString = table3.model(CompatibleExtraStringSchema);

            // normally .ready() doesn't ordinarily wait for indexes to actually be ready, it just starts creating them, but in this case we need to wait
            await table3.ready({waitForIndexes: true});

            // even with waitForIndexes we seem to need a short delay with dynamodb-local here :(
            await new Promise(r => setTimeout(r, 600));

            // add some models
            for (var i = 0; i < 13; i++) {
                await new CompatibleExtraString({num:i, otherString: `string query ${i%4}`, }).save();
            }

            // check that we can query for them:
            await t.test('query on added index', async t => {
                await t.test('on string index ', async t => {
                    const extraStrings = await CompatibleExtraString.queryMany({
                        otherString: 'string query 2'
                    });
                    t.equal(extraStrings.length, 3, 'should return three matches');
                    t.equal(extraStrings[0].constructor, (new CompatibleExtraString()).constructor, 'should have the correct constructor');
                    t.equal(extraStrings[0].num, 2, 'should return values in sorted order');
                    t.equal(extraStrings[1].num, 6, 'should return values in sorted order');
                    t.equal(extraStrings[2].num, 10, 'should return values in sorted order');
                    t.end();
                });
                t.end();
            });
            t.end();
        });
        t.end();
    });

    t.end();
});

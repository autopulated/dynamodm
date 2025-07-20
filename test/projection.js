const tap = require('tap');

const clientOptions = {
    endpoint: 'http://localhost:8000'
};

const DynamoDMConstructor = require('../');
const DynamoDM = DynamoDMConstructor({clientOptions, logger:{level:'error'}});


tap.test('incompatible NonKeyAttributes', async t => {

    t.test('same table, different keys', async t => {
        // when we try to use different incompatible projected attributes in
        // the same table handle, the error we'll get should be a duplicate
        // index name error when validating the table:
        const table = DynamoDM.Table({ name: 'test-table-incompat-nonkey'});
        const properties = {
            id:           DynamoDM.DocIdField,
            a:            {type: 'string'},
            b:            {type: 'string'},
            c:            {type: 'string'},
            d:            {type: 'string'},
        };
        const Schema1 = DynamoDM.Schema('model1', { properties }, {
            index: {
                testIndex: {
                    hashKey: 'a',
                    project: ['b', 'c']
                }
            }
        });
        const Schema2 = DynamoDM.Schema('model2', { properties }, {
            index: {
                testIndex: {
                    hashKey: 'a',
                    project: ['c', 'd']
                }
            }
        });
        table.model(Schema1);
        table.model(Schema2);

        await t.rejects(table.ready(), {
            message: 'Schema(s) "model1, model2" define incompatible versions of index "testIndex"'
        }, 'incompatible projections within same table handle');
    });

    t.test('same table, different number of keys', async t => {
        const table = DynamoDM.Table({ name: 'test-table-incompat-nonkey'});
        const properties = {
            id:           DynamoDM.DocIdField,
            a:            {type: 'string'},
            b:            {type: 'string'},
            c:            {type: 'string'},
            d:            {type: 'string'},
        };
        const Schema1 = DynamoDM.Schema('model1', { properties }, {
            index: {
                testIndex: {
                    hashKey: 'a',
                    project: ['b', 'c']
                }
            }
        });
        const Schema2 = DynamoDM.Schema('model2', { properties }, {
            index: {
                testIndex: {
                    hashKey: 'a',
                    project: ['b']
                }
            }
        });
        table.model(Schema1);
        table.model(Schema2);

        await t.rejects(table.ready(), {
            message: 'Schema(s) "model1, model2" define incompatible versions of index "testIndex"'
        }, 'incompatible projections within same table handle');
    });


    t.test('different table (existing incompatible index)', async t => {
        // if a different index exists, we currently consider that a warning
        // not an error:

        const logger = require('pino')({level:'warn'});
        // stop child logger creation so we can intercept messages
        logger.child = () => { return logger; };


        const table1 = DynamoDM.Table({ name: 'test-table-incompat-nonkey2'});
        t.after(async () => {
            await table1.deleteTable();
            table1.destroyConnection();
        });

        const Schema1 = DynamoDM.Schema('model1', {
            properties: {
                id:           DynamoDM.DocIdField,
                a:            {type: 'string'},
                b:            {type: 'string'},
                c:            {type: 'string'},
                d:            {type: 'string'},
            }
        }, {
            index: {
                testIndex: {
                    hashKey: 'a',
                    project: ['b', 'c']
                }
            }
        });
        table1.model(Schema1);

        // create index version 1:
        await table1.ready({waitForIndexes: true});

        const table2 = DynamoDM.Table({ name: 'test-table-incompat-nonkey2', logger});

        const Schema2 = DynamoDM.Schema('model2', {
            properties: {
                id:           DynamoDM.DocIdField,
                a:            {type: 'string'},
                b:            {type: 'string'},
                c:            {type: 'string'},
                d:            {type: 'string'},
            }
        }, {
            index: {
                testIndex: {
                    hashKey: 'a',
                    project: ['c', 'd']
                }
            }
        });
        table2.model(Schema2);

        const results = t.capture(logger, 'warn');

        // now try to create a noncompatible index with a different handle to the same table:
        await table2.ready({waitForIndexes: true});

        t.match(
            results(),
            [{args:[
                {existingIndexes:[], differentIndexes:[]},
                'WARNING: indexes "testIndex" differ from the current specifications, but these will not be automatically updated.'
            ]}],
            'should have called logger.warn with a suitable message'
        );
    });


});

tap.test('projection', async t => {
    const table = DynamoDM.Table({ name: 'test-table-projection'});
    const ProjectAllSchema = DynamoDM.Schema('projectall', {
        properties: {
            numVal:       {type: 'number'},
            strVal:       {type: 'string'},
            blob:         DynamoDM.Binary,
            objVal:       {type: 'object'}
        }
    }, {
        index: {
            strVal: {
                hashKey: 'strVal',
                sortKey: 'numVal',
                project: 'all'
            }
        }
    });

    const ProjectSomeSchema = DynamoDM.Schema('projectsome', {
        properties: {
            a:            {type: 'string'},
            b:            {type: 'number'},
            c:            {type: 'string'},
            d:            {type: 'string'},
            notProjected: {type: 'string'},
        },
        required: [
            'a','b','c'
        ]
    }, {
        index: {
            aaa: {
                hashKey: 'a',
                sortKey: 'b',
                project: ['c', 'd']
            },
        }
    });

    const RequireNotProjectedSchema = DynamoDM.Schema('rnp', {
        properties: {
            a:            {type: 'string'},
            b:            {type: 'number'},
            c:            {type: 'string'},
            d:            {type: 'string'},
            notProjected: {type: 'string'},
        },
        required: [
            'a','b','c','notProjected'
        ]
    }, {
        index: {
            aaa: {
                hashKey: 'a',
                sortKey: 'b',
                project: ['c', 'd']
            },
        }
    });

    const ProjectAll = table.model(ProjectAllSchema);
    const ProjectSome = table.model(ProjectSomeSchema);
    const RNPModel = table.model(RequireNotProjectedSchema);

    t.before(async () => {
        let creating = [];
        for(let i = 0; i < 5; i++) {
            creating.push((new ProjectAll({
                numVal:i,
                strVal: 'str',
                blob: Buffer.from(`blob ${i}`),
                objVal: {
                    somekey: i,
                    [i]: 'x'
                },
                notInSchema: i

            })).save());

            creating.push((new ProjectSome({
                a: `a ${i%2}`,
                b: i,
                c: `c ${i}`,
                d: `d ${i}`,
                notProjected: `np ${i}`,
                notInSchema: i,
            })).save());
        }

        await Promise.all(creating);
    });

    t.after(async () => {
        await table.deleteTable();
        table.destroyConnection();
    });

    t.test('querying project-all index', async t => {
        t.test('queryOne', async t => {
            const m = await ProjectAll.queryOne({strVal:'str', numVal:3});
            t.match(m, {numVal:3, strVal:'str', blob: Buffer.from('blob 3'), objVal: { somekey: 3, 3: 'x' }, notInSchema: 3}, 'should return all attributes');

            m.newAttr = 123;
            await t.resolves(m.save(), 'should be able to save modification to model constructed from all projected attributes');
        });
        t.test('queryMany', async t => {
            const models = await ProjectAll.queryMany({strVal:'str'});
            t.equal(models.length, 5, 'should return all matching documents');
            for(const m of models) {
                const i = m.numVal;
                t.match(i, Number);
                t.match(m, {numVal:i, strVal:'str', blob: Buffer.from(`blob ${i}`), objVal: { somekey: i, [i]: 'x' }, notInSchema: i}, 'should return all attributes');

                m.newAttr = 123;
                await t.resolves(m.save(), 'should be able to save modification to model constructed from all projected attributes');
            }
        });
    });

    t.test('querying project-some index', async t => {
        t.test('queryOne', async t => {
            const m = await ProjectSome.queryOne({a:'a 1', b:3}, {onlyProjected: true});
            t.match(m, {
                a:'a 1', // index hash key
                b:3, // index range key
                c:'c 3', // also projected
                d:'d 3', // also projected
            }, 'should return projected attributes');
            t.notHas(m, { notProjected: 'np 3' }, 'should not have non-projected attributes');

            await t.rejects(
                m.save(),
                {name:'Error', message:'Document was created from partial query data and cannot be saved.'},
                'should not be able to save partial model'
            );
            await t.rejects(
                m.remove(),
                {name:'Error', message:/Version error: the model .id="projectsome\.[a-z0-9]*" was updated by another process between loading and removing./},
                'should not be able to remove partial that lacks required version field'
            );
        });
        t.test('queryMany', async t => {
            const models = await ProjectSome.queryMany({a:'a 0'}, {onlyProjected: true});
            t.equal(models.length, 3, 'should return all matching documents');
            for(const m of models) {
                const i = m.b;
                t.match(i, Number);
                t.match(m, { a:`a ${i % 2}`, b:i, c:`c ${i}`, d:`d ${i}`, }, 'should return projected attributes');
                t.notHas(m, { notProjected: `np ${i}` }, 'should not have non-projected attributes');
                m.c = 'updated';
                await t.rejects(
                    m.save(),
                    {name:'Error', message:'Document was created from partial query data and cannot be saved.'},
                    'should not be able to save partial model'
                );
            }
        });
    });

    // Currently the required schema attributes are applied even when
    // unmarshalling from projected indexes. This could potentially be relaxed
    // in the future:
    t.test('querying index that does not project required attribute', async t => {
        await t.rejects(
            RNPModel.queryOne({a:'a 1', b:3}, {onlyProjected: true}),
            {name:'Error', message:"Document does not match schema for rnp:  must have required property 'notProjected'"},
            'should reject with correct message'
        );
    });
});



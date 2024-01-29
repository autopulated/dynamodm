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

t.pass('import ok');

t.test('promiseAllWithCancellation', async t => {
    const delayMsThenResolve = async (ms) => new Promise(resolve => setTimeout(() => resolve(`resolve value ${ms}`), ms));
    const delayMsThenReject = async (ms) => new Promise((ignored_resolve, reject) => setTimeout(() => reject(`reject value ${ms}`), ms));
    const promiseAllWithCancellation = DynamoDMConstructor.promiseAllWithCancellation;

    t.test('without cancellation should wait for all', async t => {
        const results = await promiseAllWithCancellation([
            delayMsThenResolve(5),
            delayMsThenResolve(10),
            delayMsThenResolve(15),
        ]);
        const results2 = await Promise.all([
            delayMsThenResolve(5),
            delayMsThenResolve(10),
            delayMsThenResolve(15),
        ]);
        t.match(results, results2);
    });

    t.test('without cancellation should stop at first rejection', async t => {
        let results1, results2, rejection1, rejection2;
        try {
            results1 = await promiseAllWithCancellation([
                delayMsThenResolve(5),
                delayMsThenReject(10),
                delayMsThenResolve(100),
            ]);
        } catch(e) {
            rejection1 = e;
        }
        try{
            results2 = await Promise.all([
                delayMsThenResolve(5),
                delayMsThenReject(10),
                delayMsThenResolve(100),
            ]);
        } catch(e) {
            rejection2 = e;
        }
        t.match(results1, results2, 'should not have results');
        t.match(rejection1, rejection2, 'should reject with "reject value 10"');
    });

    t.test('cancellation by resolve should work', async t => {
        let cancel, cancelled = new Promise((resolve, ignored_reject) => {cancel = resolve;});

        const promises = [
            delayMsThenResolve(5),
            delayMsThenResolve(10),
            delayMsThenResolve(15),
        ];

        const pending = promiseAllWithCancellation(promises, cancelled);
        await promises[0];
        cancel();
        await t.rejects(pending, new Error('Operation cancelled.'), 'should reject on cancellation after fulfillment of a promise');
    });

    t.test('immediate cancellation by resolve should work', async t => {
        let cancel, cancelled = new Promise((resolve, ignored_reject) => {cancel = resolve;});

        const promises = [
            delayMsThenResolve(5),
            delayMsThenResolve(10),
            delayMsThenResolve(15),
        ];

        const pending = promiseAllWithCancellation(promises, cancelled);
        cancel(new Error('foo'));
        await t.rejects(pending, new Error('foo'), 'should reject on cancellation before fulfillment of any promises');
    });

    t.test('cancelation after resolve should be harmless', async t => {
        let cancel, cancelled = new Promise((resolve, ignored_reject) => {cancel = resolve;});

        const promises = [
            delayMsThenResolve(5),
            delayMsThenResolve(10),
            delayMsThenResolve(15),
        ];

        const results = await promiseAllWithCancellation(promises, cancelled);
        cancel(new Error('bar'));
        t.match(results, ['resolve value 5', 'resolve value 10', 'resolve value 15']);
    });

    t.test('cancelation after reject should be harmless', async t => {
        let cancel, cancelled = new Promise((resolve, ignored_reject) => {cancel = resolve;});

        const promises = [
            delayMsThenResolve(5),
            delayMsThenResolve(10),
            delayMsThenReject(15),
        ];

        await t.rejects(promiseAllWithCancellation(promises, cancelled), 'reject value 15', 'should reject with value 15');
        cancel(new Error('bar'));
        t.ok("shouldn't have thrown or caused unhandled rejection");
    });

    t.test('invalid cancelation', async t => {
        await t.rejects(promiseAllWithCancellation([], {then:123}), {message:'Cancellation must be thenable.'}, 'should reject invalid cancellation');
    });

    await t.end();
});

t.test('incorrect usage throws', async t => {
    const DynamoDMConstructor = require('../../dynamodm');
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
        const table = require('../../dynamodm')({logger: false}).Table({ name: 'test-table-2', clientOptions});
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

t.test('basic schemas:', async t => {
    const table = DynamoDM.Table({ name: 'test-table-schemas'});

    await t.test('empty schema', async t => {
        // models should have default .id, .type, .createdAt, .updatedAt properties
        const EmptySchema = DynamoDM.Schema('emptySchema');
        t.hasStrict(EmptySchema, { name:'emptySchema', idFieldName:'id', typeFieldName:'type' });

        const EmptyModel = table.model(EmptySchema);
        const EmptyDoc = new EmptyModel();

        t.hasOwnProps(EmptyDoc, ['id'], 'should have id');
        const asObj = await EmptyDoc.toObject();
        t.hasOwnProps(asObj, ['type', 'id'], 'should have id and type fields in toObject');
        t.equal(Object.keys(asObj).length, 2, 'should have no other fields in toObject');

        t.end();
    });

    await t.test('complex schema', async t => {
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
        }, {
            index: {
                // index called 'findByCreated', .type hash key, '.createdAt' sort key
                findByCreated: {
                    hashKey: 'type',
                    sortKey: 'createdAt'
                }
            }
        });

        const DefaultValueTestSchema = DynamoDM.Schema('namespace.defaultvalue', {
            properties: {
                a: {type: 'string', default:'foo'},
                b: {type: 'number', default: 123},
                c: {type: 'number', default: 456},
                d: {type: 'array', items: {type:'number'}, default:[1]}
            }
        });

        const ThingModel = table.model(ThingSchema);
        const DefaultValueTestModel = table.model(DefaultValueTestSchema);

        await table.ready({waitForIndexes: true});
        await table.ready();

        t.test('create a valid doc', async t => {
            const props = {
                aaaa: 'a',
                bbbb: 123,
                blob: Buffer.from('hello')
            };
            const ValidThingDoc = new ThingModel(props);
            t.hasStrict(ValidThingDoc, props);
            t.hasOwnProps(ValidThingDoc, ['id', 'type']);
            // createdAt and updatedAt are only set when saved
            t.notMatch(ValidThingDoc, ['createdAt', 'updatedAt']);
            await ValidThingDoc.save();
            t.hasOwnProps(ValidThingDoc, ['createdAt', 'updatedAt']);
            t.end();
        });

        t.test('invalid docs throw', async t => {
            t.throws(() => {new ThingModel({aaaa: 123});});
            t.throws(() => {new ThingModel({blob: 123});});
            t.end();
        });

        t.test('default values', async t => {
            const defaultTestDoc = new DefaultValueTestModel({c:3});
            t.hasStrict(defaultTestDoc, {a:'foo', b:123, c:3, d:[1]});

            const testDoc2 = new DefaultValueTestModel({c:4, d:[]});
            t.hasStrict(testDoc2, {a:'foo', b:123, c:4, d:[]});
            t.end();
        });

        t.test('setting default values on load', async t => {
            const doc = new ThingModel({ aaaa: 'a', bbbb: 123, blob: Buffer.from('hello') });
            await doc.save();
            const loaded = await DefaultValueTestModel.getById(doc.id);
            t.hasStrict(loaded, {a:'foo', b:123, c:456, d:[1]});
            t.end();
        });

        t.test('toObject', async t => {
            const defaultTestDoc = new DefaultValueTestModel({c:3});
            const asObj = await defaultTestDoc.toObject();
            t.hasStrict(asObj, {
                a:'foo', b:123, c:3, d:[1]
            }, 'should always have all schema fields');

            defaultTestDoc.z = 4;
            t.hasStrict(await defaultTestDoc.toObject(), {
                a:'foo', b:123, c:3, d:[1], z:4
            }, 'should have additional field');
        });

        t.test('undefined property should be treated as non-existent', async t => {
            const props = {
                aaaa: 'a',
                bbbb: 123,
                cccc: undefined
            };
            let doc = new ThingModel(props);
            t.notHas(doc, {cccc:1});
            t.equal(doc.cccc, undefined);
            t.hasStrict(doc, props);

            await table.ready({waitForIndexes: true});

            await doc.save();
            t.notHas(doc, {cccc:1});
            t.equal(doc.cccc, undefined);
            t.hasStrict(doc, props);

            doc = await ThingModel.getById(doc.id);
            t.notHas(doc, {cccc:1});
            t.equal(doc.cccc, undefined);
            t.hasStrict(doc, props);

            t.end();
        });

        t.end();
    });

    t.test('arrays', async t => {
        const arraysTable = DynamoDM.Table({ name: 'test-table-arrays'});
        const ArrayOfStringsSchema = DynamoDM.Schema('namespace.array1', {
            properties: {
                anArray: {
                    type: 'array',
                    items: {
                        type: 'string'
                    }
                }
            },
            required: ['anArray'],
            additionalProperties: false
        });
        const ArrayOfMixedTypesSchema = DynamoDM.Schema('namespace.array2', {
            properties: {
                anArray: {
                    type: 'array',
                }
            },
            required: ['anArray'],
            additionalProperties: false
        });
        const ArrayOfTimestampsSchema = DynamoDM.Schema('namespace.array3', {
            properties: {
                anArray: {
                    type: 'array',
                    items: DynamoDM.Timestamp
                }
            },
            required: ['anArray'],
            additionalProperties: false
        });

        const ArrayOfStrings = arraysTable.model(ArrayOfStringsSchema);
        const ArrayOfMixedTypes = arraysTable.model(ArrayOfMixedTypesSchema);
        const ArrayOfTimestamps = arraysTable.model(ArrayOfTimestampsSchema);

        await arraysTable.ready();
        await arraysTable.ready({waitForIndexes:true});

        t.test('create a valid doc', async t => {
            const props1 = {anArray:['a', 'b']};
            const props2 = {anArray:['a', 1, {b:3}]};
            const props3 = {anArray:[new Date(), new Date()]};
            const ValidArrayOfStringsDoc    = new ArrayOfStrings(props1);
            const ValidArrayOfMixedDoc      = new ArrayOfMixedTypes(props2);
            const ValidArrayOfTimestampsDoc = new ArrayOfTimestamps(props3);
            t.hasStrict(ValidArrayOfStringsDoc, props1);
            t.hasStrict(ValidArrayOfMixedDoc,   props2);
            t.hasStrict(ValidArrayOfTimestampsDoc, props3);
            await ValidArrayOfStringsDoc.save();
            await ValidArrayOfMixedDoc.save();
            await ValidArrayOfTimestampsDoc.save();
            t.end();
        });

        t.test('invalid docs throw', async t => {
            t.throws(() => {new ArrayOfStrings({anArray: [1]});},
                {message:'Document does not match schema for namespace.array1: /anArray/0 must be string.'}, 'number as string');
            t.throws(() => {new ArrayOfStrings({anArray: [{a:1}]});},
                {message:'Document does not match schema for namespace.array1: /anArray/0 must be string.'}, 'object as string');
            t.throws(() => {new ArrayOfStrings({anArray: [Buffer.from('test', 'ascii')]});},
                {message:'Document does not match schema for namespace.array1: /anArray/0 must be string.'}, 'buffer as string');
            t.throws(() => {new ArrayOfMixedTypes({anArray: {1:'a'}});},
                {message:'Document does not match schema for namespace.array2: /anArray must be array.'}, 'object as array');
            t.throws(() => {new ArrayOfMixedTypes({anArray: 2});},
                {message:'Document does not match schema for namespace.array2: /anArray must be array.'}, 'number as array');
            t.throws(() => {new ArrayOfMixedTypes({anArray: new Date()});},
                {message:'Document does not match schema for namespace.array2: /anArray must be array.'}, 'date as array');
            t.throws(() => {new ArrayOfMixedTypes({anArray: Buffer.from('test', 'ascii')});},
                {message:'Document does not match schema for namespace.array2: /anArray must be array.'}, 'buffer as array');
            t.throws(() => {new ArrayOfTimestamps({anArray: [new Date(), 'a']});},
                {message:'Document does not match schema for namespace.array3: /anArray/1 must be a Date.'}, 'string as timestamp');
            t.throws(() => {new ArrayOfTimestamps({anArray: [1]});},
                {message:'Document does not match schema for namespace.array3: /anArray/0 must be a Date.'}, 'number as timestamp');
            t.end();
        });

        t.end();
    });

    t.test('.methods', async t => {
        const methodsTable = DynamoDM.Table({ name: 'test-table-methods'});

        const FooSchema = DynamoDM.Schema('namespace.foo');
        FooSchema.methods.getMyId = function getMyId(){
            return this.id;
        };

        const ThingSchema = DynamoDM.Schema('namespace.thing', {
            properties: {
                id:           DynamoDM.DocIdField,
                aaaa:         {type: 'string'},
                bbbb:         {type: 'number'},
                blob:         DynamoDM.Binary,
                createdAt:    DynamoDM.CreatedAtField,
                updatedAt:    DynamoDM.UpdatedAtField,
            },
            required: ['id', 'aaaa', 'bbbb'],
            additionalProperties: false
        }, {
            index: {
                // index called 'findByCreated', .type hash key, '.createdAt' sort key
                findByCreated: {
                    hashKey: 'type',
                    sortKey: 'createdAt'
                }
            }
        });
        ThingSchema.methods.incrementThenSave = async function incSave(){
            this.bbbb += 1;
            return await this.save();
        };
        const InvalidMethodSchema = DynamoDM.Schema('namespace.invalidMethod');
        InvalidMethodSchema.methods.constructor = function notAllowed(){ };

        const FooModel = methodsTable.model(FooSchema);
        const ThingModel = methodsTable.model(ThingSchema);

        t.test('does not allow invalid method names', async t => {
            t.throws(() => {
                methodsTable.model(InvalidMethodSchema);
            }, {message:"The name 'constructor' is reserved and cannot be used for a method."}, 'should not allow .constructor() method');
        });

        const foo = new FooModel();
        t.equal(foo.getMyId, FooSchema.methods.getMyId, 'creates with methods');
        t.equal(foo.getMyId(), foo.id, 'methods are callable');

        const thing = new ThingModel({aaaa:'a', bbbb:1});
        t.equal(thing.incrementThenSave, ThingSchema.methods.incrementThenSave, 'creates with async methods');
        t.resolves(thing.incrementThenSave(), 'async methods work');
    });

    t.test('.statics', async t => {
        const staticsTable = DynamoDM.Table({ name: 'test-table-2'});

        const FooSchema = DynamoDM.Schema('namespace.foo', {b: {type: 'number', default: 0}});
        FooSchema.statics.testStatic = function getMyId(){
            return 4;
        };
        FooSchema.statics.testAsyncStatic = async function getMyId(){
            return 4;
        };

        const FooModel = staticsTable.model(FooSchema);

        t.equal(FooModel.testStatic, FooSchema.statics.testStatic, 'creates with statics');
        t.equal(FooModel.testStatic(), 4, 'statics are callable');
        t.resolves(FooModel.testAsyncStatic(), 'async statics work');
    });
});

t.test('virtuals:', async t => {
    t.test('string aliases', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'});
        const ASchema = DynamoDM.Schema('virtualsA', {
            properties: {
                foo: {type: 'number', default: 3}
            },
            additionalProperties: false
        });
        ASchema.virtuals.bar = 'foo';

        const AModel = table.model(ASchema);

        const aDoc = new AModel();
        const bDoc = new AModel();
        t.equal(aDoc.bar, 3, 'alias should be initialised on construction');
        aDoc.foo = 4;
        bDoc.foo = 5;
        t.equal(aDoc.bar, 4, 'alias should be set on assignment, and be unique amongst instances');
        t.equal(bDoc.bar, 5, 'alias should be set on assignment');
        await aDoc.save();

        const aDocAsObj = await aDoc.toObject();
        t.equal(aDocAsObj.bar, 4, 'alias should be included in toObject()');

        const loaded = await AModel.getById(aDoc.id);
        t.equal(loaded.bar, 4, 'alias should be initialised on load');

        loaded.bar = 7;
        t.equal(loaded.foo, 7, 'alias assignment should work');

        const asObjWithoutVirtuals = await loaded.toObject({virtuals: false});
        t.equal(asObjWithoutVirtuals.bar, undefined, 'should not be included with {virtuals: false}');
    });

    t.test('getters', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'});
        const ASchema = DynamoDM.Schema('testGetters', {
            additionalProperties: false
        });
        ASchema.virtuals.nonPrefixedId = {
            get: function() {
                return this.id.split('.')[1];
            }
        };

        const AModel = table.model(ASchema);

        const aDoc = new AModel();
        const id = aDoc.id.split('.')[1];
        t.equal(aDoc.nonPrefixedId, id, 'getter should work on construction');
        t.throws(() => { aDoc.nonPrefixedId = 'something'; }, 'setting should throw');
        await aDoc.save();

        const aDocAsObj = await aDoc.toObject();
        t.equal(aDocAsObj.nonPrefixedId, id, 'getter should be included in toObject()');

        const loaded = await AModel.getById(aDoc.id);
        t.equal(loaded.nonPrefixedId, id, 'getter should be initialised on load');

        const asObjWithoutVirtuals = await loaded.toObject({virtuals: false});
        t.equal(asObjWithoutVirtuals.nonPrefixedId, undefined, 'should not be included with {virtuals: false}');
    });

    t.test('invalid descriptors', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'});
        const ASchema = DynamoDM.Schema('testErrors', {
            properties: {
                foo: {type: 'number', default: 3}
            },
            additionalProperties: false
        });
        ASchema.virtuals.bar = 'unknown';

        t.throws(() => {
            table.model(ASchema);
        }, {message: 'Virtual property "bar" is an alias for an unknown property "unknown".'}, 'throw on invalid alias');


        ASchema.virtuals = {
            bar: {
                notValid: 1
            }
        };
        t.throws(() => {
            table.model(ASchema);
        }, {message: 'Virtual property "bar" invalid descriptor key "notValid" is not one of \'configurable\', \'enumerable\', \'value\', \'writable\', \'get\' or \'set\'.'}, 'throw on invalid descriptor');

        ASchema.virtuals = {
            bar: 123
        };
        t.throws(() => {
            table.model(ASchema);
        }, {message: 'Virtual property "bar" must be a string alias, or a data descriptor or accessor descriptor.'}, 'throw on invalid descriptor type');

    });

    t.test('enumerable', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'});
        const ASchema = DynamoDM.Schema('testEnumerable', {
            properties: { foo: { type: 'number'} },
            additionalProperties: false
        });
        ASchema.virtuals.someEnumerable = {
            value: 4,
            enumerable: true
        };

        const AModel = table.model(ASchema);

        const aDoc = new AModel();
        await aDoc.save();
        t.equal(aDoc.someEnumerable, 4, 'enumerable virtual should work');
        // only 'for ... in' actually iterates over inherited enumerable properties:
        let found = false;
        for (const prop in aDoc) {
            if (prop === 'someEnumerable') found = true;
        }
        t.equal(found, true, 'enumerable virtual should be enumerable');
        t.equal(Object.keys(aDoc).indexOf('someEnumerable'), -1, 'enumerable virtual is not an own-property, so should not be in Object.keys()');

        const aDocAsObj = await aDoc.toObject();
        t.equal(aDocAsObj.someEnumerable, 4, 'should be included in toObject()');

        const loaded = await AModel.getById(aDoc.id);
        t.equal(loaded.someEnumerable, 4, 'should be initialised on load');

        const asObjWithoutVirtuals = await loaded.toObject({virtuals: false});
        t.equal(asObjWithoutVirtuals.someEnumerable, undefined, 'should not be included with {virtuals: false}');
    });
});

t.test('converters:', async t => {
    t.test('sync field creation and deletion', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'});
        const ASchema = DynamoDM.Schema('testConverters', {
            properties: {
                foo: {type: 'number', default: 3},
                bar: {type: 'number', default: 4}
            },
            additionalProperties: false
        });
        let AModel;
        const optionAValue = {};
        ASchema.converters.push(function(value, options){
            // eslint-disable-next-line
            t.equal(this.constructor, AModel, 'this should be a model instance');
            // eslint-disable-next-line
            t.equal(this.constructor.table, table, 'this.constructor should provide access to model statics');
            t.equal(options.a, optionAValue, 'options values should be preserved');
            delete value.bar;
            return value;
        });
        ASchema.converters.push(function(value){
            value.newField = 3;
            return value;
        });
        ASchema.converters.push(function(value){
            value.newField2 = value.newField + 1;
            return value;
        });

        AModel = table.model(ASchema);

        const aDoc = new AModel();

        const aDocAsObj = await aDoc.toObject({a:optionAValue});
        t.equal(aDocAsObj.foo, 3, 'non deleted field should remain');
        t.equal(aDocAsObj.bar, undefined, 'deleted field should be deleted');
        t.equal(aDocAsObj.newField2, 4, 'converters should be executed in series');
        t.equal(aDocAsObj.newField, 3, 'converter should work');
    });

    t.test('async field creation', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'});
        const ASchema = DynamoDM.Schema('testConverters', {
            additionalProperties: false
        });
        ASchema.converters.push(async function(){
            await new Promise(resolve => setTimeout(resolve, 10));
            return {
                newField: 3
            };
        });
        ASchema.converters.push(async function(value){
            await new Promise(resolve => setTimeout(resolve, 10));
            value.newField2 = value.newField + 1;
            return value;
        });
        ASchema.converters.push(async function(value, options){
            await new Promise(resolve => setTimeout(resolve, 10));
            value.newField3 = options.newField3Value;
            return value;
        });

        const AModel = table.model(ASchema);

        const aDoc = new AModel();

        const aDocAsObj = await aDoc.toObject({newField3Value: 7});
        t.equal(aDocAsObj.newField, 3, 'async converter should work');
        t.equal(aDocAsObj.newField2, 4, 'async converters should be executed in series');
        t.equal(aDocAsObj.newField3, 7, 'async converters should be executed in series');
    });

    t.test('errors', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'});
        const ASchema = DynamoDM.Schema('testGetters', {
            additionalProperties: false
        });
        ASchema.converters.push('something');

        t.throws(() => {
            table.model(ASchema);
        }, {message:'Converters must be functions or async functions: typeof converters[0] is string.'}, 'pushing a non-function type should throw');
    });
});



t.test('custom field names', async t => {
    const table = DynamoDM.Table({ name: 'test-table-custom-names'});

    // models should have default .id, .type, .createdAt, .updatedAt properties
    const Schema = DynamoDM.Schema('schema1', {
        properties: {
            myId: DynamoDM.DocIdField,
            myType: DynamoDM.TypeField,
            myCreatedAt: DynamoDM.CreatedAtField,
            myUpdatedAt: DynamoDM.UpdatedAtField
        }
    });
    t.hasStrict(Schema, { name:'schema1', idFieldName:'myId', typeFieldName:'myType', createdAtFieldName:'myCreatedAt', updatedAtFieldName:'myUpdatedAt' });

    const Model = table.model(Schema);

    const doc = new Model();
    await doc.save();

    t.hasOwnProps(doc, ['myId', 'myType', 'myCreatedAt', 'myUpdatedAt']);
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

t.test('schema errors', async t => {
    const table = DynamoDM.Table({ name: 'test-table-schemas'});

    const AStringSchema            = DynamoDM.Schema('namespace.thing', { properties: { a: {type: 'string'}, } });
    const ARequiredStringSchema    = DynamoDM.Schema('namespace.thing', { properties: { b: {type: 'string'}, }, required:['b'] });
    const ABinarySchema            = DynamoDM.Schema('namespace.thing', { properties: { a: DynamoDM.Binary, } });
    const ATimestampSchema         = DynamoDM.Schema('namespace.thing', { properties: { a: DynamoDM.Timestamp, } });
    const ARequiredTimestampSchema = DynamoDM.Schema('namespace.thing', { properties: { b: DynamoDM.Timestamp, }, required:['b'] });
    const AnUnknownSchema          = DynamoDM.Schema('namespace.thing', { properties: { a: {'extendedType':'foo'} } });
    const ANestedStringSchema      = DynamoDM.Schema('namespace.thing2', { properties: { nested: { type:'object', properties: {a: {type: 'string'}, } } } });
    const ANestedBinarySchema      = DynamoDM.Schema('namespace.thing2', { properties: { nested: { type:'object', properties: {a: DynamoDM.Binary, } } } });
    const ANestedArrayStringSchema    = DynamoDM.Schema('namespace.thing3', { properties: { nestedArray: { type:'array', items: {type: 'string'} } } });
    const ANestedArrayUnknownSchema   = DynamoDM.Schema('namespace.thing3', { properties: { nestedArray: { type:'array', items: {'extendedType':'foo'} } } });
    const ANestedArrayTimestampSchema = DynamoDM.Schema('namespace.thing3', { properties: { nestedArray: { type:'array', items: DynamoDM.Timestamp } } });
    const AString                = table.model(AStringSchema);
    const ARequiredString        = table.model(ARequiredStringSchema);
    const ABinary                = table.model(ABinarySchema);
    const ATimestamp             = table.model(ATimestampSchema);
    const ARequiredTimestamp     = table.model(ARequiredTimestampSchema);
    const AnUnknown              = table.model(AnUnknownSchema);
    const ANestedString          = table.model(ANestedStringSchema);
    const ANestedBinary          = table.model(ANestedBinarySchema);
    const ANestedArrayString     = table.model(ANestedArrayStringSchema);
    const ANestedArrayUnknown    = table.model(ANestedArrayUnknownSchema);
    const ANestedArrayTimestamp  = table.model(ANestedArrayTimestampSchema);

    await t.test('invalid schema type', async t => {
        t.throws(() => { table.model({type:'object', properties:{a:{type:'string'}}} ); }, {message: 'The model schema must be a valid DynamoDM.Schema().'}, 'plain object schema');
        t.throws(() => { DynamoDM.Schema('invalid', 123); }, {message: 'Invalid schema: must be an object or undefined.'}, 'invalid Schema() construction');
    });

    await table.ready({allowAliasedSchemas: true});

    await t.test('invalid schemas', async t => {
        t.throws(() => { DynamoDM.Schema({ properties: { a: {type: 'string'}, } }); }, {message: 'Invalid name: must be a string.'}, 'unnamed schema');
        t.throws(() => { DynamoDM.Schema('somemodel', { type: 'string' }); }, {message: 'Schema type must be object (or can be omitted).'}, 'unnamed schema');
        t.throws(() => { DynamoDM.Schema('somemodel', { properties: { id: DynamoDM.DocIdField, _id: DynamoDM.DocIdField } }); }, {message: 'Duplicate id field.'}, 'duplicate id field');
    });

    await t.test('instantiating invalid types', async t => {
        t.throws(() => {new AString({a: 1 });}, {message: 'Document does not match schema for namespace.thing: /a must be string.'}, 'number as string');
        t.throws(() => {new ABinary({a: '' });}, {message: 'Document does not match schema for namespace.thing: /a must be a Buffer.'}, 'string as binary');
        t.throws(() => {new ATimestamp({a: '' });}, {message: 'Document does not match schema for namespace.thing: /a must be a Date.'}, 'string as timestamp');
        t.throws(() => {new AnUnknown({a: '' });}, {message: 'Document does not match schema for namespace.thing: /a is an unknown extended type.'}, 'string as invalid schema');
    });

    await t.test('saving invalid types', async t => {
        let x = new AString({a: '' });
        x.a = 1;
        t.rejects(x.save(), {message: 'Document does not match schema for namespace.thing: /a must be string.'}, 'save number as string');

        x = new ABinary({a: Buffer.from('hello') });
        x.a = '';
        t.rejects(x.save(), {message: 'Document does not match schema for namespace.thing: /a must be a Buffer.'}, 'save string as binary');

        x = new ATimestamp({a: new Date() });
        x.a = '';
        t.rejects(x.save(), {message: 'Document does not match schema for namespace.thing: /a must be a Date.'}, 'save string as timestamp');

        x = new AnUnknown();
        x.a = '';
        t.rejects(x.save(), {message: 'Document does not match schema for namespace.thing: /a is an unknown extended type'}, 'save string as unknown');
    });

    await t.test('loading invalid types', async t => {
        const x = new AString({a: 'a' });
        await x.save();

        t.rejects(ABinary.getById(x.id), {message: 'Expected marshalled type of Buffer property a to be a Uint8Array (got string).'}, 'string as Buffer');
        t.rejects(ATimestamp.getById(x.id), {message: 'Expected marshalled type of Date property a to be a number (got string).'}, 'string as Date');
        t.rejects(AnUnknown.getById(x.id), {message: 'Document does not match schema for namespace.thing: /a is an unknown extended type.'}, 'string as unknown');
    });

    await t.test('loading invalid nested types', async t => {
        const x = new ANestedString({ nested: { a: 'a' }});
        await x.save();

        t.rejects(ANestedBinary.getById(x.id), {message: 'Expected marshalled type of Buffer property a to be a Uint8Array (got string).'}, 'string as Buffer');
    });

    await t.test('loading invalid array nested types', async t => {
        const x = new ANestedArrayString({ nestedArray: ['a'] });
        await x.save();
        t.rejects(ANestedArrayTimestamp.getById(x.id), {message: 'Document does not match schema for namespace.thing3: /nestedArray/0 Expected marshalled type of Date property 0 to be a number (got string).'}, 'string as Timestamp');
        t.rejects(ANestedArrayUnknown.getById(x.id), {message: 'Document does not match schema for namespace.thing3: /nestedArray/0 is an unknown extended type.'}, 'string as unknown extended type');
    });

    await t.test('saving missing required fields', async t => {
        let x = new ARequiredString({b:''});
        x.b = undefined;
        t.rejects(x.save(), /^Document does not match schema for namespace.thing: {2}must have required property 'b'/, 'missing simple type');
        x = new ARequiredTimestamp({b:new Date()});
        x.b = undefined;
        t.rejects(x.save(), /^Document does not match schema for namespace.thing: {2}must have required property 'b'/, 'missing marshalled type');
    });

    await t.test('loading missing required fields', async t => {
        const x = new AString({a: 'a' });
        await x.save();

        t.rejects(ARequiredString.getById(x.id), /^Document does not match schema for namespace.thing: {2}must have required property 'b'/, 'missing simple type');
        t.rejects(ARequiredTimestamp.getById(x.id), /^Document does not match schema for namespace.thing: {2}must have required property 'b'/, 'missing marshalled type');
    });

    await t.test('invalid index specifiction', async t => {
        t.throws(() => {
            DynamoDM.Schema('namespace.shouldthrow', {
                properties: { aaa: {type: 'string'}, bbb: {type: 'string'}, boolField: {type: 'boolean'} }
            }, {
                index: {
                    type: {
                        hashKey: 'aaa'
                    }
                }
            });
        }, {message: 'Invalid index name "type": this name is reserved for the built-in type index.'} , 'should throw creating an index called type');

        t.throws(() => {
            DynamoDM.Schema('namespace.shouldthrow', {
                properties: { aaa: {type: 'string'}, bbb: {type: 'string'}, boolField: {type: 'boolean'} }
            }, {
                index: {
                    aaa: false
                }
            });
        }, {message: 'Invalid index specification ${indexSpec}: must be 1,true, or {hashKey:"", sortKey?:""}.'} , 'should throw with key:false');

        t.throws(() => {
            DynamoDM.Schema('namespace.shouldthrow', {
                properties: { aaa: {type: 'string'}, bbb: {type: 'string'}, boolField: {type: 'boolean'} }
            }, {
                index: {
                    ccc: 1
                }
            });
        }, {message: 'The schema must define the type of property .ccc used by index "ccc".'} , 'should throw referencing an unknown field');


        t.throws(() => {
            DynamoDM.Schema('namespace.shouldthrow', {
                properties: { aaa: {type: 'string'}, bbb: {type: 'string'}, boolField: {type: 'boolean'} }
            }, {
                index: {
                    boolField: 1
                }
            });
        }, {message: 'The schema type of property .boolField, "{"type":"boolean"}" used by index "boolField" is not indexable.'} , 'should throw on a non-indexable field type');
    });

    t.teardown(async () => {
        await table.deleteTable();
        table.destroyConnection();
    });
});

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
            blob:         DynamoDM.Binary
        },
        required:['barVal']
    }, {
        index: {
            barValRange: {
                hashKey: 'type',
                sortKey: 'barVal'
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
    await table.ready();

    const all_foos = [];
    for (let i = 0; i < 50; i++ ) {
        // padd the Foo items out to 350KBk each, so that we can test bumping up against dynamoDB's 16MB response limit
        let foo = new Foo({fooVal:i, blob: Buffer.from(`hello query ${i}`), padding: Buffer.alloc(3.5E5)});
        all_foos.push(foo);
        await foo.save();
    }
    const N = 10;
    for (let i = 0; i < N; i++) {
        await new Bar({barVal:i, blob: Buffer.from(`hello query ${i}`), }).save();
        await new IndexedString({someN:i, someString:`string number ${i}`, someOtherString:'constant value' }).save();
        await new IndexedTs({someN:i, someTs:(new Date(i*1e7)) }).save();
        await new IndexedNumberAndBinary({num:i, blob: Buffer.from(`hello query ${i%4}`), }).save();
        await new Ambiguous().save();
    }

    t.after(async () => {
        await table.deleteTable();
        table.destroyConnection();
    });

    await t.test('listAllIds', async t => {
        const allFoos = [];
        for await (const x of Foo.listAllIds({Limit:2})) {
            allFoos.push(x);
            t.equal(x.split('.')[1], 'foo');
        }

        const allBars = [];
        for await (const x of Bar.listAllIds()) {
            allBars.push(x);
            t.equal(x.split('.')[1], 'bar');
        }

        t.equal(allFoos.length, all_foos.length);
        t.equal(allBars.length, 10);
        t.ok(allFoos.every(x => x.split('.')[1] === 'foo'));
        t.ok(allBars.every(x => x.split('.')[1] === 'bar'));

        t.end();
    });

    await t.test('rawQueryOneId', async t => {
        const id = await Foo.rawQueryOneId({
            IndexName:'type',
            KeyConditionExpression:'#typeFieldName = :type',
            ExpressionAttributeValues: { ':type': 'namespace.foo' },
            ExpressionAttributeNames: { '#typeFieldName': 'type' }
        });
        t.equal(id.split('.')[1], 'foo');
    });

    await t.test('rawQueryOne', async t => {
        t.test('on type index', async t => {
            const foo = await Foo.rawQueryOne({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.foo' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            });
            t.equal(foo.constructor, Foo, 'constructor should be Foo');
            t.equal(foo instanceof Foo, true, 'should be an instance of Foo');

            const nonExistent = await Foo.rawQueryOne({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.doesNotExist' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            });
            t.equal(nonExistent, null, 'returns null for no matches');
        });
        t.test('on string index ', async t => {
            const indexedString = await IndexedString.rawQueryOne({
                IndexName:'someString',
                KeyConditionExpression:'someString = :value',
                ExpressionAttributeValues: { ':value': 'string number 4' }
            });
            t.equal(indexedString.constructor, (new IndexedString()).constructor, 'should have correct constructor');
            t.equal(indexedString.someN, 4, 'should have other properties set correctly');
        });
        t.test('on binary index ', async t => {
            const nb = await IndexedNumberAndBinary.rawQueryOne({
                IndexName:'myBinaryIndex',
                // 'blob' is a banned name, so need to use ExpressionAttributeNames...
                KeyConditionExpression:'#fieldName = :value',
                ExpressionAttributeValues: { ':value': Buffer.from('hello query 3') },
                ExpressionAttributeNames: { '#fieldName': 'blob' }
            });
            t.equal(nb.constructor, (new IndexedNumberAndBinary()).constructor, 'should have correct constructor');
            t.equal(nb.num, 3, 'should return the lowest in sorted order');
        });
    });

    await t.test('getById', t => {
        t.rejects(Foo.getById(null), 'should reject null id');
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
        t.match(await Foo.getByIds(['nonexistent']), [null], 'should return null for nonexistent id');
        t.match(await Foo.getByIds(['nonexistent', all_foos[0].id]), [null, all_foos[0]], 'should return null along with extant model');
        const foos = await Foo.getByIds(all_foos.map(f => f.id));
        t.equal(foos.length, all_foos.length, 'should return all models');
        t.match(foos.map(f => f?.id), all_foos.map(f => f?.id), 'should return all models in order');
        await t.rejects(Foo.getByIds(''), new Error('Invalid ids: must be array of strings of nonzero length.'), 'should reject non-array argument');
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

    await t.test('rawQueryIds', async t => {
        t.test('on type index', async t => {
            const foos = await arrayFromAsync(Foo.rawQueryIds({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.foo' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            }));
            t.equal(foos.length, all_foos.length, 'should return all N of this type');
            t.type(foos[0], 'string', 'should return String IDs');
        });
        t.end();
    });

    await t.test('rawQuery', async t => {
        t.test('on type index', async t => {
            const foos = await arrayFromAsync(Foo.rawQuery({
                IndexName:'type',
                KeyConditionExpression:'#typeFieldName = :type',
                ExpressionAttributeValues: { ':type': 'namespace.foo' },
                ExpressionAttributeNames: { '#typeFieldName': 'type' }
            }));
            t.equal(foos.length, all_foos.length, 'should return all N of this type');
            t.equal(foos[0].constructor, (new Foo()).constructor, 'should have the correct constructor');
        });
        t.test('on string index ', async t => {
            const indexedStrings = await arrayFromAsync(IndexedString.rawQuery({
                IndexName:'someString',
                KeyConditionExpression:'someString = :value',
                ExpressionAttributeValues: { ':value': 'string number 4' }
            }));
            t.equal(indexedStrings.length, 1, 'should return one match');
            t.equal(indexedStrings[0].constructor, (new IndexedString()).constructor, 'should have the correct constructor');
            t.equal(indexedStrings[0].someN, 4, 'should have other properties set correctly');
        });
        t.test('on binary index ', async t => {
            const nb = await arrayFromAsync(IndexedNumberAndBinary.rawQuery({
                IndexName:'myBinaryIndex',
                KeyConditionExpression:'#fieldName = :value',
                ExpressionAttributeValues: { ':value': Buffer.from('hello query 3') },
                ExpressionAttributeNames: { '#fieldName': 'blob' }
            }));
            t.equal(nb.length, 2, 'should return two matches');
            t.equal(nb[0].constructor, (new IndexedNumberAndBinary()).constructor, 'should have correct constructor');
            t.equal(nb[0].num, 3, 'should return the lowest in sorted order');
        });
        t.end();
    });

    await t.test('query() api', async t => {
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
                t.rejects(IndexedNumberAndBinary.queryMany({num:{$gt:0, $lt:2}}), {message:'Only a single $gt/$gte/$lt/$lte condition is supported in the simple query api.'}, 'rejects multiple conditions');
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

        // await t.test('querying via a model of the wrong type', async t => {
        //     console.log('starting query-wrong-type test');
        //     const result = t.rejects(Bar.queryMany({type:'namespace.foo'}), {message:'Document does not match schema for ambiguous.bar. The loaded document has a different type "namespace.foo", and the schema is incompatible:  must have required property \'barVal\'.'}, 'should throw if the query returns a document of an incompatible type');
        //     console.log('running query-wrong-type test');
        //     try {
        //         await result;
        //     } catch (e) {
        //         console.log('this error should not have been thrown!!!', e);
        //     }
        //     console.log('finished query-wrong-type test');
        //     t.end();
        // });

        t.end();
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
            await t.test('rawQuery on added index', async t => {
                await t.test('on string index ', async t => {
                    const extraStrings = await arrayFromAsync(CompatibleExtraString.rawQuery({
                        IndexName:'otherString',
                        KeyConditionExpression:'otherString = :value',
                        ExpressionAttributeValues: { ':value': 'string query 2' }
                    }));
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

t.end();

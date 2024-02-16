const t = require('tap');

const clientOptions = {
    endpoint: 'http://localhost:8000'
};

const DynamoDMConstructor = require('../');
const DynamoDM = DynamoDMConstructor({clientOptions, logger:{level:'error'}});

t.test('basic schemas:', async t => {
    const table = DynamoDM.Table({ name: 'test-table-schemas'});

    await t.test('empty schema', async t => {
        // models should have default .id, .type, .createdAt, .updatedAt properties
        const EmptySchema = DynamoDM.Schema('emptySchema');
        t.hasStrict(EmptySchema, { name:'emptySchema', idFieldName:'id', typeFieldName:'type' });

        const EmptyModel = table.model(EmptySchema);
        const EmptyDoc = new EmptyModel();

        t.equal(EmptyModel.type, 'emptySchema', "should expose the schema's name as .type");
        t.equal(EmptyModel.table, table, 'should expose the table this model belongs to');

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

    t.test('during construction', async t => {
        const table = DynamoDM.Table({ name: 'test-table-2'});
        const ASchema = DynamoDM.Schema('virtualsA', {
            properties: {
                foo: {type: 'number', default: 3},
                compound_a_b: {type: 'string'},
            }
        });
        ASchema.virtuals.bar = 'foo';
        ASchema.virtuals.a = {
            get() {
                return (this.compound_a_b || ':').split(':')[0];
            },
            set(a) {
                return this.compound_a_b = a.toString() + ':' + (this.compound_a_b || ':').split(':')[1];
            }
        };
        ASchema.virtuals.b = {
            get() {
                return (this.compound_a_b || ':').split(':')[1];
            },
            set(b) {
                return this.compound_a_b = (this.compound_a_b || ':').split(':')[0] + ':' + b.toString();
            }
        };

        const AModel = table.model(ASchema);

        t.match(new AModel({bar:3}), {foo:3}, 'alaises should work during construction');
        t.match(new AModel({a:'AA', b:'BB'}), {compound_a_b: 'AA:BB'}, 'setters should work during construction');
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

t.end();

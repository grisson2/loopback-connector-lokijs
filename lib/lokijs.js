
const lokijs = require('lokijs');
const util = require('util');
const Connector = require('loopback-connector').Connector;
const debug = require('debug')('loopback:connector:lokijs');
const lfsa = require('lokijs/src/loki-fs-structured-adapter');


/**
 * Connector definition
 * 
 * @param {*} settings 
 * @param {*} dataSource 
 */
function LokiJS(settings, dataSource) {
    Connector.call(this, 'lokijs', settings);
    this.dataSource = dataSource;

    // this.settings
    var adapter = new lfsa();
    this.db = new lokijs(settings.filename || "defaultDb.db", Object.assign({ 
        adapter : adapter,
        autoload: true,
        autosave: true, 
        autosaveInterval: 10000
    }, (settings || {})))

}
util.inherits(LokiJS, Connector);

/**
 * 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.connect = function(cb) {
    cb();
}

/**
 * 
 * @param {*} cb 
 */
LokiJS.prototype.disconnect = function (cb) {
    cb();
};


/**
 * 
 */
LokiJS.prototype.getTypes = function() {
    return ['db', 'nosql', 'lokijs'];
};

/**
 * 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.count = function(modelName, where, options, callback) {
    try{
        const collection = this.db.getCollection(modelName);
        if (!collection) return callback(null, 0);

        const _where = this.buildWhere(modelName, where, options);
        const count = collection.count(_where);
        callback(null, count);
    }catch(error){
        callback(error);
    }
}

/**
 * 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.create = function(modelName, data, options, callback) {
    try{
        // mongo do like this: https://github.com/strongloop/loopback-connector-mongodb/blob/4551cc0428a6954091981eb8e4861e7c1c9b37d8/lib/mongodb.js#L579
        const modelIdName = this.idName(modelName);

        // this.getIdValue(modelName)
        let collection = this.db.getCollection(modelName);
        if (!collection){
            const opt = {
                clone: true
            };
            if (modelIdName !== 'id'){
                opt.unique = [modelIdName]
            }
            collection = this.db.addCollection(modelName,opt);
        }

        let result = collection.insert(data);
        if (Array.isArray(result)){
            callback(null, result.map(r => r.$loki))
        }else{
            callback(null, result.$loki)
        }
    }catch(error){
        callback(error);
    }
}

/**
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} id Model id
 * @param {*} options Loopback options 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.all = function all(modelName, filter, options, callback){

    try{
        const collection = this.db.getCollection(modelName);
        if (!collection) return callback(null, []);

        let _where = null;
        if (filter.where){
            _where = this.buildWhere(modelName, filter.where, options);
        }

        let records = collection.chain().find(_where);
        if (filter.order) {
            // parse stuff like "fieldName desc", "fieldname asc"
            const regex = /^ *(.+) +([^ ]+) *$/gi;
            const found = regex.exec(filter.order);
            if (!found){
                records = records.simplesort(filter.order);
            }else {
                records = records.simplesort(found[1], {desc: found[2].toLowerCase() === 'desc'});
            }
        }

        const offset = filter.skip || filter.offset;
        if (offset) records = records.offset(offset);
        if (filter.limit) records = records.limit(filter.limit);

        records = records.data();

        this.fromDatabase(modelName, records, options, callback)
    }catch(error){
        callback(error);
    }
}

/**
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} where  Loopback where filter
 * @param {*} options Loopback options 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.destroyAll = function destroyAll(modelName, where, options, callback){
    
    if (!callback && 'function' === typeof where) {
        callback = where;
        where = undefined;
    }

    try{
        const collection = this.db.getCollection(modelName);
        if (!collection) return callback(null, {count: 0});
        let _where = null;
        if (where){
            _where = this.buildWhere(modelName, where, options);
        }

        const records = collection.chain().find(_where);
        const count = records.filteredrows.length;
        records.remove();

        callback(null, {count: count})
    }catch(error){
        callback(error);
    }
}

/**
 * Destroy a record by id
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} id Model id
 * @param {*} options Loopback options 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.destroy = function destroyAll(modelName, id, options, callback){
    const modelIdName = this.idName(modelName);
    this.destroyAll(modelName, {[modelIdName]: id}, options, callback);
}

/**
 * Base update function. Accept a where and "return" in the callback the array of updated records
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} where  Loopback where filter
 * @param {*} data 
 * @param {*} options Loopback options 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype._update = function updateAll(modelName, where, data, options, callback) {
    try{
        const collection = this.db.getCollection(modelName);
        if (!collection) return callback(null, []);

        const modelIdName = this.idName(modelName);

        let _where = null;
        if (where){
            _where = this.buildWhere(modelName, where, options);
        }

        // sanitize data(1st level only, valuate to do it recursively)
        Object.keys(data || {}).forEach(key=>{
            // don't allow id and meta updates. Also remove everything that starts with $
            if ((['meta',modelIdName].includes(key)) || key.startsWith("$")){
                delete data[key];
            }
        })

        const records = collection.chain().find(_where);
        const count = records.filteredrows.length;
        records.update((record)=>{
            Object.assign(record,data);
        })

        callback(null, records.data())
    }catch(error){
        callback(error);
    }
}

/**
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} where  Loopback where filter
 * @param {*} data 
 * @param {*} options Loopback options 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.update = function updateAll(modelName, where, data, options, callback) {
    this._update(modelName, where, data,options, (error, results)=>{
        if (error) return callback(error);
        callback(null, {count: results.length});
    })
}

/**
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} id Model id
 * @param {*} data 
 * @param {*} options Loopback options 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.updateAttributes = function updateAttrs(modelName, id, data, options, callback){
    // FIXME: test me!!
    console.warn("updateAttributes: i need to be checked for desired behavior and tested!");
    this._update(modelName, {id: id}, data, options, (error, results)=>{
        if (error) return callback(error);
        callback(null, results[0] || {});
    })
}

/**
 * Fully replace a record by id
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} id Model id
 * @param {*} data 
 * @param {*} options Loopback options 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.replaceById = function replace(modelName, id, data, options, callback) {

    try{
        const collection = this.db.getCollection(modelName);
        // no collection available. This obviously means we don't have any record.
        if (!collection) return callback(null, null);
        
        const modelIdName = this.idName(modelName);
        
        id = this.coerceId(id);

        // sanitize data(1st level only, valuate to do it recursively)
        Object.keys(data || {}).forEach(key=>{
            // don't allow id or meta updates.
            // don't allow stuff that starts with $
            if ((['meta', modelIdName].includes(key)) || key.startsWith("$")){
                delete data[key];
            }
        })


        let record = collection.chain().find({$loki: id});
        record.update((record)=>{
            // this will keep only id and meta while deleting all the other properties.
            const keep = ['meta', '$loki'];
            Object.keys(record).filter(k => !keep.includes(k)).forEach(k => {
                delete record[k];
            })
            Object.assign(record, data);
        })
        record = record.data();
        if (record.length>0){
            this.fromDatabase(modelName, record[0], options, callback);
        }else{
            callback(null, null);
        }

    }catch(error){
        callback(error);
    }
}

/**
 * 
 * @param {*} id Model id
 */
LokiJS.prototype.coerceId = function coerceId(id) {
    if (Number.isInteger(id) ) return id;
    if (typeof id === 'string' && id.match(/^[0-9]+$/g)) return parseInt(id);
    throw new Error("Invalid id "+id);
}
/**
 * Convert some db stuff to human readable stuff to hide the underlaying details
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} data 
 * @param {*} options Loopback options 
 * @param {*} callback Standard node callback (error,result)
 */
LokiJS.prototype.fromDatabase = function(modelName, data, options, callback){
    

    const modelIdName = this.idName(modelName);
    // this will remove some internal data and remap the id property
    const _map = (record) => {
        record[modelIdName] = record.$loki;
        delete record.$loki;
        delete record['meta'];
        return record;
    }

    if (Array.isArray(data)) return callback(null, data.map(_map));

    return callback(null, _map(data));
}

/**
 * converta loopback query to a lokijs query
 * NOTE: unsupported operators will cause this function to throw an exception.
 * User must take care of catching it.
 * 
 * @param {*} modelName Model name onto which the query is executed 
 * @param {*} where  Loopback where filter
 * @param {*} options Loopback options 
 */
LokiJS.prototype.buildWhere = function(modelName, where, options, parsingIds = false){
    
    // nothing specified. return an empty where (will match everything)
    if (Object.keys(where).length === 0) return null;

    const modelIdName = this.idName(modelName);

    // recoursive condition parser
    const _recourseCondition = (condition, parsingIds) => {
        
        if (Array.isArray(condition))
            return condition.map(c => _recourseCondition(c, parsingIds));
        
        const keepAsIsType = ['string','number'];
        if (keepAsIsType.includes(typeof condition))
            return parsingIds ? this.coerceId(condition) : condition;
        
        return this.buildWhere(modelName, condition, options, parsingIds);
    }

    
    let _where = {};

    // these ones don't need special cases, just translate as-is!
    const mapping = {
        'and': '$and',
        'or': '$or',
        'inq': '$in',
        'nin': '$nin',
        'between': '$between',
        'gt': '$gt',
        'gte': '$gte',
        'lt': '$lte'
    };

    // unsupported by lokijs.
    // not going to polyfill now (which btw how to do it?)
    const unsupported = ['nor'];
    
    Object.keys(where).forEach(key => {
        const condition = where[key];

        // simple key remapping
        if (mapping[key]){
            _where[mapping[key]] = _recourseCondition(condition, parsingIds);
            return;
        }

        // where contains an id condition. parse id
        if (key === modelIdName) {
            _where.$loki = _recourseCondition(condition, true);
            return;
        }

        // check for unsupported operators
        if (unsupported.includes(key)){
            throw(new Error("Unsupported operator "+key+" in where filter"));
        }

        // recourse over all the others values
        _where[key] = _recourseCondition(condition, parsingIds);
    
    });
    return _where;
}

exports.LokiJS = LokiJS;

/**
 * Initialize the  connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
module.exports.initialize = function initializeDataSource(dataSource, callback) {
  
    const settings = dataSource.settings || {};
    dataSource.connector = new LokiJS(settings, dataSource);  
    
    if (callback){
        dataSource.connector.connect(callback);
    }
};




/*

CHECKLIST:

[V] connector.create
connector.updateOrCreate (optional, but see below)
connector.replaceOrCreate (a new feature - work in progress)
connector.findOrCreate (optional, but see below)
connector.buildNearFilter
[V*] connector.all       *filter include?
[V] connector.destroyAll
[V] connector.count
connector.save
[V] connector.update
connector.destroy
[V] connector.replaceById (a new feature - work in progress)
[V*] connector.updateAttributes  TEST ME!!!!! 
If we implement a create() and an update() and don’t specify an updateOrCreate() will dao.js handle this for us automatically, through https://github.com/strongloop/loopback-datasource-juggler/blob/master/lib/dao.js#L437 ?

Yes, loopback-datasource-juggler will handle updateOrCreate and findOrCreate if they are not provided by the connector. However, I am strongly recommending to implement these methods, because only the connector can provide a version that guarantees atomicity. The juggler shim makes two database/datasource calls (find and then update/create depending on the result), which creates a race condition. I.e. if you callupdateOrCreate({ id: 123, name: 'foo' }) twice at the same time (in parallel), the second call is very likely to fail as the second “find” will return before the first “create” was called, and thus the second call will attempt to create a record that already exists.
*/
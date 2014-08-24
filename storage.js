//     storage.js v0.1.0
//     (c)(by) Vincent Richard (vrichard)
//        , S�bastien Biaudet (sbiaudet)

// The storage is the database driver for azure.
//
// __Example:__
//
//      require('[pathToStorage]/storage').createStorage({}, function(err, storage) {
//          ...
//      });

var azure = require('azure-storage')
  , uuid = require('node-uuid')
  , async = require('async')
  , root = this
  , azureTableStorage
  , Storage
  , eg = azure.TableUtilities.entityGenerator;

if (typeof exports !== 'undefined') {
    azureTableStorage = exports;
} else {
    azureTableStorage = root.azureStorage = {};
}

azureTableStorage.VERSION = '0.1.0';

// Create new instance of storage.
azureTableStorage.createStorage = function(options, callback) {
    return new Storage(options, callback);
};


// ## Azure Table storage
Storage = function(options, callback) {

    this.filename = __filename;
    this.isConnected = false;

    if (typeof options === 'function')
        callback = options;
    
    var azureConf = {
        storageAccount: process.env['AZURE_STORAGE_ACCOUNT'],
        storageAccessKey: process.env['AZURE_STORAGE_ACCESS_KEY'],
        storageTableHost: process.env['AZURE_TABLE_HOST']
    }    

    this.options = mergeOptions(options, azureConf);

    var defaults = {
        //storageAccount: "nodeeventstore",
        //storageAccessKey: "3OSNleBo4SpxymTHL6LD34PgHNI57tI545M1Klww1GNvSfTeOhDgX80Tk59uEQsOqGdfbpKFJCSaFjLlZPe22Q==",
        //storageTableHost : "http://nodeeventstore.table.core.windows.net/",
        eventsTableName: 'events',
        snapshotsTableName: 'snapshots',
        timeout: 1000
    };
    
    this.options = mergeOptions(options, defaults);

    var defaultOpt = {
        auto_reconnect: true,
        ssl: false
    };

    this.options.options = this.options.options || {};

    this.options.options = mergeOptions(this.options.options, defaultOpt);
    
    if (callback) {
        this.connect(callback);
    }
};

Storage.prototype = {

    // __connect:__ connects the underlaying database.
    //
    // `storage.connect(callback)`
    //
    // - __callback:__ `function(err, storage){}`
    connect: function(callback) {
        var self = this;
        var retryOperations = new azure.ExponentialRetryPolicyFilter();
        var server = azure.createTableService(this.options.storageAccount, this.options.storageAccessKey, this.options.storageTableHost).withFilter(retryOperations);;
        self.client = server;
        self.isConnected = true;
        
        var createEventsTable = function(callback) {          
                                 self.client.createTableIfNotExists(self.options.eventsTableName, callback);
                                };

        var createSnapshotTable = function(callback) {            
                                        self.client.createTableIfNotExists(self.options.snapshotsTableName, callback);
                                };

        async.parallel([createEventsTable,
                        createSnapshotTable,
        ],function(err){
            if (err) {
                if(callback) callback(err);
            } else {
                if (callback) callback(null, self);
            }
          });
        
    },

    // __addEvents:__ saves all events.
    //
    // `storage.addEvents(events, callback)`
    //
    // - __events:__ the events array
    // - __callback:__ `function(err){}`
    addEvents: function(events, callback) {

         if (!events || events.length === 0) {
            callback(null);
            return;
        }
        var self = this;
        var batch = new azure.TableBatch();

        async.each(events, function(event, callback) {
                                var item = new StoredEvent(event);
                                batch.insertEntity(item);
                                callback();
                            },
                            function(err)
                            {
                                if (err){
                                    if (callback) callback(err)
                                } else {
                                    self.client.executeBatch(self.options.eventsTableName, batch, function (err, performBatchOperationResponses, batchResponse)
                                    {
                                        console.log(err);
                                        callback();
                                    });
                                }
                            });
    },

    // __addSnapshot:__ stores the snapshot
    // 
    // `storage.addSnapshot(snapshot, callback)`
    //
    // - __snapshot:__ the snaphot to store
    // - __callback:__ `function(err){}` [optional]
    addSnapshot: function(snapshot, callback) {
        var self = this;
        
        self.client.insertEntity(self.options.snapshotsTableName, new StoredSnapshot(snapshot), callback);
        
    },

    // __getEvents:__ loads the events from _minRev_ to _maxRev_.
    // 
    // `storage.getEvents(streamId, minRev, maxRev, callback)`
    //
    // - __streamId:__ id for requested stream
    // - __minRev:__ revision startpoint
    // - __maxRev:__ revision endpoint (hint: -1 = to end) [optional]
    // - __callback:__ `function(err, events){}`
    getEvents: function(streamId, minRev, maxRev, callback) {
        var self = this;

        if (typeof maxRev === 'function') {
            callback = maxRev;
            maxRev = -1;
        }
        
        var query = new azure.TableQuery();

        query = query.where('PartitionKey eq ?', streamId);
        query = query.and('streamRevision >= ?', minRev);
        if (maxRev != -1) query = query.and('streamRevision < ?', maxRev);

        self.client.queryEntities(this.options.eventsTableName, query, null, function(err, entities){
            if (err) {
                    callback(err);
                } else {
                    callback(null, entities.entries.map(MapStoredEventToEvent));
                }
            });
    },


    // __getEventRange:__ loads the range of events from given storage.
    // 
    // `storage.getEventRange(match, amount, callback)`
    //
    // - __match:__ match query in inner event (payload)
    // - __amount:__ amount of events
    // - __callback:__ `function(err, events){}`
    getEventRange: function(match, amount, callback) {
        throw new Error("getEventRange is not implemented");
    },

    // __getSnapshot:__ loads the next snapshot back from given max revision or the latest if you 
    // don't pass in a _maxRev_.
    // 
    // `storage.getSnapshot(streamId, maxRev, callback)`
    //
    // - __streamId:__ id for requested stream
    // - __maxRev:__ revision endpoint (hint: -1 = to end)
    // - __callback:__ `function(err, snapshot){}`
    getSnapshot: function(streamId, maxRev, callback) {

        if (typeof maxRev === 'function') {
            callback = maxRev;
            maxRev = -1;
        }

        var query = new azure.TableQuery();

        query = query.where('streamId == ?', streamId);
        if (maxRev != -1) query = query.and('revision <= ?', maxRev);

        this.client.queryEntities(this.options.snapshotsTableName, query, null, function(err, entities){
            if (err) {
                callback(err);
            } else {
                entities = entities.entries.sort(function(a,b) {return a.revision < b.revision});
                callback(null, entities.map(MapStoredSnapshotToSnapshot)[0]);
            }
        });
    },

    // __getUndispatchedEvents:__ loads all undispatched events.
    //
    // `storage.getUndispatchedEvents(callback)`
    //
    // - __callback:__ `function(err, events){}`
    getUndispatchedEvents: function(callback) {
        var self = this;

        var query = new azure.TableQuery();

        query = query.where('dispatched == false');
        
            self.client.queryEntities(this.options.eventsTableName, query, null, function(err, entities){
                if (err) {
                    callback(err);
                } else {
                    callback(null, entities.entries.map(MapStoredEventToEvent));
                }
            });
    },

    // __setEventToDispatched:__ sets the given event to dispatched.
    //
    // __hint:__ instead of the whole event object you can pass: {_id: 'commitId'}
    //
    // `storage.setEventToDispatched(event, callback)`
    //
    // - __event:__ the event
    // - __callback:__ `function(err, events){}` [optional]
    setEventToDispatched: function(event, callback) {
        var self = this;
        var item = new StoredEvent(event);
        item.dispatched = true;
        self.client.updateEntity(self.options.eventsTableName, item, callback);
    },

    // __getId:__ loads a new id from storage.
    //
    // `storage.getId(callback)`
    //
    // - __callback:__ `function(err, id){}`
    getId: function(callback) {
        callback(null, uuid.v4());
    }
};

// helper
var mergeOptions = function(options, defaultOptions) {
    if (!options || typeof options === 'function') {
        return defaultOptions;
    }
    
    var merged = {};
    for (var attrname in defaultOptions) { merged[attrname] = defaultOptions[attrname]; }
    for (attrname in options) { if (options[attrname]) merged[attrname] = options[attrname]; }
    return merged;
};

function MapStoredEventToEvent(storedEvent)
{
    var event = {
        streamId: getEntityProperty(storedEvent.streamId),
        streamRevision: getEntityProperty(storedEvent.streamRevision),
        commitId: getEntityProperty(storedEvent.commitId),
        commitSequence: getEntityProperty(storedEvent.commitSequence),
        commitStamp: getEntityProperty(storedEvent.commitStamp) || null,
        header: getEntityProperty(storedEvent.header) || null,
        dispatched: getEntityProperty(storedEvent.dispatched),
        payload: JSON.parse(getEntityProperty(storedEvent.payload)) || null
       }
    
    return event;
}

function MapStoredSnapshotToSnapshot(storedSnapshot)
{
    var snapshot = {
            id: getEntityProperty(storedSnapshot.id),
            streamId: getEntityProperty(storedSnapshot.streamId),
            revision: getEntityProperty(storedSnapshot.revision),
            data: getEntityProperty(storedSnapshot.data)
        }

    return snapshot;
}

var StoredEvent = function(event) {
    this.PartitionKey = eg.Entity(event.streamId);
    this.RowKey = eg.Entity(event.commitId);
    this.streamId = eg.Entity(event.streamId);
    this.streamRevision = eg.Entity(event.streamRevision);
    this.commitId = eg.Entity(event.commitId);
    this.commitSequence = eg.Entity(event.commitSequence);
    this.commitStamp = eg.Entity(event.commitStamp) || null;
    this.header = eg.Entity(event.header) || null;
    this.dispatched = eg.Entity(event.dispatched);
    this.payload = eg.Entity(JSON.stringify(event.payload));
};

var StoredSnapshot = function(snapshot) {
    this.PartitionKey = eg.Entity(snapshot.streamId);
    this.RowKey = eg.Entity(snapshot.id);
    this.id = eg.Entity(snapshot.id);
    this.streamId = eg.Entity(snapshot.streamId);
    this.revision = eg.Entity(snapshot.revision);
    this.data = eg.Entity(snapshot.data);
};

var getEntityProperty = function(propertyField){
  if (propertyField != null)
    return propertyField['_'];
  else
    return null;
};
// attach public classes
Storage.StoredEvent = StoredEvent;
Storage.StoredSnapshot = StoredSnapshot;
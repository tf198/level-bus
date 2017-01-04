const debug = require('debug')('level-bus');

const SubLevel = require('level-sublevel/bytewise');
const LevelUp = require('levelup');

const bytewise = require('bytewise');
const timestamp = require('monotonic-timestamp');

const EventEmitter = require('events');
const util = require('util');
const stream = require('stream');
const path = require('path');
const crypto = require('crypto');
//const concat = require('concat-stream');
const async = require('async');
const LevelView = require('level-view');
const stringify = require('json-stable-stringify');

function noop() {}

function checkError(err) {
    if(err) throw err;
}

function timekey(ts) {
    if(typeof(ts) == 'undefined') ts = timestamp();
    return bytewise.encode(ts).toString('hex');
}

function dateRange(start, end, base) {
    if(typeof(base) == 'undefined') base = [];

    if(typeof(start) == 'undefined') start = 0;
    if(typeof(end) == 'undefined') end = 0;

    if(start < 0) start = Date.now() + (start * 1000);
    if(end < 0) end = Date.now() + (end * 1000);

    var range = {};
    
    if(start > 0) range.startkey = base.concat([timekey(start)]);
    if(end > 0) range.endkey = base.concat([timekey(end), undefined]);

    return range;
}

const MSGSTORE_ENCODING = {keyEncoding: 'utf8', valueEncoding: 'json'};
const JOB_ENCODING = {keyEncoding: bytewise, valueEncoding: 'utf8'};

/**
 *
 * TODO: drain in batches - link to async.queue.drain event
 */
function LevelBus(opts) {
    EventEmitter.call(this);

    this.opts = Object.assign({datadir: './bus', flushBatch: 10}, opts);
    debug("Creating bus", this.opts);

    this.tables = this.opts.db || LevelUp(this.opts.datadir);

    // we use one single bytewise compatible sublevel
    if(typeof(this.tables.sublevel) == 'undefined') {
        this.tables = SubLevel(this.tables);
    }
    
    this.jobs = this.tables.sublevel('j');
    this.messages = this.tables.sublevel('m');

    this.channels = {};

    this.indexer = new LevelView(this.tables.sublevel('i'));
    this.indexer.addView('msg', function(key, value, emit) {
        emit([value._id, value._ts]);
    });

}

util.inherits(LevelBus, EventEmitter);

/**
* Add a channel
*
* name: a unique string for the route
* handler: a function which accepts (message, meta, callback)
* outputs: a string or list of strings of the next route
* opts.concurrency: maximum parallel workers (default: 3)
*
* The callback should be called with the optional parameters
* (message, meta) if either have been modified.
*/
LevelBus.prototype.addChannel = function(name, handler, outputs, opts, cb) {
    
    if(typeof(outputs) === 'undefined') outputs = [];
    if(typeof(outputs) === 'string') outputs = [outputs];

    if(typeof(this.channels[name]) !== 'undefined') throw new Error("Route '" + name + "' already registered");

    opts = Object.assign({concurrency: 3}, opts);

    var q = this.channels[name] = async.queue(
            this._handle.bind(this, name, handler, outputs), 
            opts.concurrency
    );
    q.stats = {processed: 0, average: 0};
    /*
    q.drain = function() {debug("Drained " + name)};
    q.saturated = function() {debug("Saturated " + name)};
    q.unsaturated = function() {debug("Unsaturated " + name)};
    */

    debug("Added channel: " + name);
    this.flushChannel(name, cb);
}

/**
 * Flushes jobs that were not processed on a previous run for any reason.
 */
LevelBus.prototype.flushChannel = function(name, ts, cb) {
    
    var q = this.channels[name];

    if(typeof(ts) == 'undefined') {
        q.resume();
        return this.flushChannel(name, Date.now(), cb);
    }

    var self = this;

    var batchSize = this.opts.flushBatch;

    cb = cb || checkError;

    this.pending(name, ts, batchSize, (err, pending) => {
        if(err) return cb(err);
        debug("Flushing %s items", pending.length);
        async.eachSeries(
            pending, 
            (x, next) => {
                this.messages.get(x.sha, (err, data) => {
                    if(err) return cb(err);
                    this._enqueue([name], data, x.messageHash, next);
                });
            }, 
            (err) => {
                if(err) return cb(err);
                if(pending.length == batchSize) {
                    q.drain = this.flushChannel.bind(this, name, ts, cb);
                } else {
                    q.drain = noop;
                    cb();
                }
            }
        );
    });

}

/**
 * Remove a channel
 */
LevelBus.prototype.removeChannel = function(name) {
    if(this.channels[name]) {
        this.channels[name].kill();
        delete(this.channels[name]);
    }
};

/**
 * Return a list of available route names
 */
LevelBus.prototype.listChannels = function() {
    return Object.keys(this.channels);
}

/**
 * Check if a channel exists
 */
LevelBus.prototype.channelExists = function(name) {
    return typeof(this.channels[name]) !== 'undefined';
}

/**
 * The worker actually does the firing of handlers in response to jobs.
 */
LevelBus.prototype._handle = function(route, handler, outputs, message, queueCallback) {

    message._next = outputs;
    message._message = null;
    debug("Processing %s", message);
    var start = Date.now();

    var self = this;

    // need an extra dispatcher so that the handler can clean up after message is
    // safely stored
    var nextCalled = false;
    function next(handlerErr, data, handlerCallback) {
        if(typeof(data) == 'function') return next(handlerErr, undefined, data);

        if(data) message = Object.assign(message, data);

        message._route.push(route);

        if(nextCalled) {
            handlerCallback = queueCallback = noop;
            debug("Warning: next() already called");
        }
        nextCalled = true;

        var q = self.channels[route];

        q.stats.processed++;
        var time = Date.now() - start;
        q.stats.average = ((q.stats.processed-1) * q.stats.average + time) / q.stats.processed;

        if(handlerErr) {
            debug("Error in %s", message._activeJob, handlerErr);
            
            // leave the job on the queue for the next drain
            delete(message._activeJob);

            // set some error tracking info
            message._status = 'error';
            message._message = handlerErr.message;

            // dont accept anything else
            q.pause();

            self.pushMessage(null, message, function(pushErr) {
                queueCallback(pushErr);
                self.emit('failure', handlerErr, route);
            });
                
        } else {
            debug("Successfully processed %s", message._activeJob);
            self.pushMessage(message._next, message, function(pushErr) {
                handlerCallback && handlerCallback(pushErr, message);
                queueCallback(pushErr);
            });
        }
    }

    debug("Running job %s", message._activeJob);
    try {
        handler(message, next);
    } catch(e) {
        next(e);
    }
}

/**
 * Pushes a message onto the bus.
 */
LevelBus.prototype.pushMessage = function(routes, message, cb) {
    var self = this;
    var batch = [];

    if(!routes) routes = [];
    if(typeof(routes) === 'string') routes = [routes];

    // if the message has an active job attached - remove it in the batch
    if(message._activeJob) {
        debug("Removing active job: %s", message._activeJob);
        batch.push(Object.assign({
            type: 'del',
            prefix: self.jobs, 
            key: message._activeJob
        }, JOB_ENCODING));
        delete(message._activeJob);
    }

    // remove any sha that may have been injected
    delete(message._sha);

    var ts = timestamp();

    // update some fields on the message
    message._ts = ts;
    message._next = routes;

    // serialize the data and calculate the SHA1 hash
    var s = stringify(message);
    var hasher = crypto.createHash('sha1');
    hasher.update(s);
    var sha = hasher.digest('hex');

    // push to message store
    debug("Received message: %s [%s]", message._id, sha);
    batch.push({
        type: 'put', 
        prefix: this.messages, 
        key: sha, 
        value: s, 
        keyEncoding: 'utf8', 
        valueEncoding: 'utf8' // already serialized
    });

    // index the message
    batch = batch.concat(this.indexer.createBatch(sha, message));

    // add job entries for all the routes that will be enqueued
    routes.map((x) => {
        var job = [x, ts];
        batch.push(Object.assign({
            type: 'put', 
            prefix: this.jobs, 
            key: job, 
            value: sha
        }, JOB_ENCODING));
    });

    // push everything to the database and then enqueue
    self.tables.batch(batch, function(putErr) {
        if(putErr) return cb(putErr);

        message._sha = sha;
        self._enqueue(routes, s, sha, cb);
    });
}

/**
 * Retrieve a message
 */
LevelBus.prototype.getMessage = function(key, cb) {
    this.messages.get(key, MSGSTORE_ENCODING, (err, msg) => {
        if(err) return cb(err);
        msg._sha = key;
        cb(null, msg);
    });
}

/**
 * Helper function to create a message and push it
 */
LevelBus.prototype.dispatch = function(routes, data, dispatchCallback) {
    if(typeof(data) === 'function') return this.dispatch(routes, null, data);

    var message = Object.assign({
        _id: timekey(),
        _next: null,
        _route: [],
        _status: 'ok'
    }, data);

    this.pushMessage(routes, message, function(err, messageHash) {
        dispatchCallback && dispatchCallback(err, message);
    });
}

/**
 * Pushes the job onto the worker, if it is available.
 *
 * Messages are recreated from the JSON to ensure we dont have any references.
 */
LevelBus.prototype._enqueue = function(routes, messageJSON, sha, enqueueCallback) {
    var self = this;

    if(routes.length == 0) {
        return enqueueCallback(null);
    }

    var c = 0;

    routes.map((x) => {

        // in order to safely manipulate the message we recreate it from JSON
        // and set the previous hash
        var m = JSON.parse(messageJSON);
        m._previous = sha;
        m._activeJob = [x, m._ts];

        // only push if queue is accepting
        var q = self.channels[x]; 
        if(typeof(q) != 'undefined' && !q.paused) {
            q.push(m);
            c++;
        } else {
            debug("Job %s not queued - not accepting", m._activeJob);
        }
    });

    debug("Queued to %d routes", c);
    enqueueCallback(null);
    
}

/**
 * List the jobs that have been persisted but not yet executed.
 */
LevelBus.prototype.pending = function(channel, ts, limit, cb) {
    if(typeof(channel) == 'function') return this.pending(null, undefined, -1, channel);
    if(typeof(ts) == 'function') return this.pending(channel, undefined, -1, ts);
    if(typeof(limit) == 'function') return this.pending(channel, ts, -1, limit);

    var result = [];
    
    var query = Object.assign({limit}, JOB_ENCODING);

    if(channel) {
        query.gt = [channel];
        query.lt = [channel, timestamp(ts)];
    }

    this.jobs.createReadStream(query)
        .on('data', (data) => {
            result.push({channel: data.key[0], ts: data.key[1], sha: data.value});
        })
        .on('error', cb)
        .on('end', () => {
            cb(null, result);
        });
}

/**
 * Get the status of a channel, or all channels.
 *
 * This is only since channel startup - see messageStats() for historic data.
 */
LevelBus.prototype.channelStatus = function(name) {

    if(name == undefined) {
        var result = {};
        for(var i in this.channels) result[i] = this.channelStatus(i);
        return result;
    }

    var q = this.channels[name];

    return Object.assign({
        paused: q.paused,
        queued: q.length(),
        active: q.running(),
    }, q.stats);
}

/* HISTORICAL REPORTING FUNCTIONS */

/**
 * Gets all the stages of a particular message
 */
LevelBus.prototype.getMessageHistory = function(msgId, cb) {
    var events = [];

    this.indexer.createDocStream(this.messages, 'msg', {startkey: [msgId], endkey: [msgId, undefined]})
        .on('data', (msg) => events.push(msg))
        .on('error', cb)
        .on('end', () => cb(null, events))
}

/**
 * Get a list of messages that were processed in the given timeframe
 */
LevelBus.prototype.getMessages = function(start, end, cb) {
    if(typeof(start) == 'function') return this.getMessages(undefined, undefined, start);
    if(typeof(end) == 'function') return this.getMessages(start, undefined, end);

    var messages = [];
    var prev = null;

    var query = dateRange(start, end);
    this.indexer.createQueryStream('msg', query)
        .on('data', function(data) {
            if(data.key[0] != prev) {
                messages.push({id: data.key[0], ts: data.key[1], key: data.value});
                prev = data.key[0];
            }
        })
        .on('error', cb)
        .on('end', function() {
            messages.reverse();
            cb(null, messages);
        });

}

/**
 * Get statistics for a given timeframe
 */
LevelBus.prototype.messageStats = function(start, end, cb) {
    if(typeof(start) == 'function') return this.messageStats(undefined, undefined, start);
    if(typeof(end) == 'function') return this.messageStats(start, undefined, end); 

    var result = {};
    var prev = null;

    this.indexer.createDocStream(this.messages, 'msg', dateRange(start, end))
        .on('data', function(msg) {
            var success = msg._status == 'OK'; 
            var route = msg._route || [];
            var channel = route.length ? route[route.length-1] : 'default';

            if(typeof(result[channel]) == 'undefined') result[channel] = {};
            var stats = result[channel];
            if(typeof(stats[msg._status]) == 'undefined') stats[msg._status] = 0;

            stats[msg._status]++;
        })
        .on('error', cb)
        .on('end', function() {
            cb(null, result) 
        });

}

module.exports = LevelBus;


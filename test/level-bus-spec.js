const expect = require('chai').expect;

const fs = require('fs');
const LevelUp = require('levelup');
const MemDown = require('memdown');
const bytewise = require('bytewise');
const async = require('async');
const rm = require('rimraf');

const LevelBus = require('../lib/level-bus');

var _db = 0;

function testDB(opts) {
    MemDown.clearGlobalStore();
    return LevelUp('router-' + _db++, Object.assign({db: MemDown}, opts));
}

function testLevelBus(options) {
    var opts = Object.assign({db: testDB()}, options);

    var r = new LevelBus(opts);

    if(!options) {
        r.on('failure', console.log.bind(console, "FAILURE"));
    }
    return r;
}

function noop(msg, d) {
    d();
}

describe("LevelBus", function() {

    it("should have a clean scope", function() {
        var r = testLevelBus();
        expect(r.listChannels()).to.eql([]);
        expect(r.channelExists('channel_1')).to.be.false;
    });

    it("should add and remove channels correctly", function() {
        var r = testLevelBus();

        r.addChannel('channel_1', noop, 'channel_2');
        r.addChannel('channel_2', noop);
        expect(r.listChannels()).to.eql(['channel_1', 'channel_2']);
        expect(r.channelExists('channel_1')).to.be.true;

        try {
            r.addChannel('channel_1', noop);
        } catch(e) {
            expect(e.message).to.equal("Route 'channel_1' already registered");
        }

        r.removeChannel('channel_1');
        expect(r.listChannels()).to.eql(['channel_2']);
        expect(r.channelExists('channel_1')).to.be.false;

        r.addChannel('channel_1', noop);
        expect(r.listChannels()).to.eql(['channel_2', 'channel_1']);
        expect(r.channelExists('channel_1')).to.be.true;


    });

    it('should dispatch all callbacks for a single channel', function(done) {
        var r = testLevelBus();
        var stack = [];

        r.addChannel('channel_1', function(data, next) {
            stack.push('start_handler');
            data.foo = data.foo.toUpperCase();
            //expect(message._route).to.eql([]);

            next(null, function(err, msg) {
                stack.push('released');
                expect(stack).to.eql(['queued', 'start_handler', 'end_handler', 'released']);

                expect(msg._sha).to.be.string;
                expect(msg._sha.length).to.equal(40);
                
                r.getMessage(msg._sha, function(err, msg) {
                    if(err) return done(err);
                    expect(msg.foo).to.equal('BAR');
                    expect(msg._route).to.eql(['channel_1']);
                    r.pending(function(err, pending) {
                        expect(pending).to.eql([]);
                        done();
                    });
                });
            });
            stack.push('end_handler');
        });


        r.dispatch('channel_1', {foo: "bar"}, function(err, msg) {
            if(err) return done(err);
            stack.push('queued'); 
            expect(msg._route).to.eql([]);
            expect(msg._next).to.eql(['channel_1']);
            expect(msg._sha).to.be.string;
        });
    });

    it('should dispatch all callbacks for chained channels', function(done) {
        var r = testLevelBus();
        var stack = [];

        function handler_1(message, dispatcher) {
            stack.push('h1_init');
            message.first = 'h1';

            //expect(message._route).to.eql([]);
            //expect(message._next).to.eql(['channel_2']);

            dispatcher(null, function(err, header) {
                if(err) return done(err);
                stack.push('h1_queued');
            });
            stack.push('h1_post');
        }

        function handler_2(message, dispatcher) {
            stack.push('h2_init');

            expect(message.first).to.equal('h1');
            //expect(message._route).to.eql(['channel_1']);
            //expect(message._next).to.eql([]);

            dispatcher(null, function(err, msg) {
                if(err) return done(err);
                stack.push('h2_queued');
                expect(stack).to.deep.equal(['queued', 
                        'h1_init', 'h1_post', 'h1_queued',
                        'h2_init', 'h2_post', 'h2_queued']);

                r.getMessage(msg._sha, function(err, msg) {
                    if(err) return done(err);
                    expect(msg._route).to.eql(['channel_1', 'channel_2']);
                    done();
                });
            });
            stack.push('h2_post');
        }

        r.addChannel('channel_1', handler_1, 'channel_2');
        r.addChannel('channel_2', handler_2);

        r.dispatch('channel_1', {foo: 'bar'}, function(err, msg) {
            if(err) return done(err);
            stack.push('queued');
        });
    });

    it('should dispatch multiple channels', function(done) {
        var r = testLevelBus();
        var stack = []

        var waiting = 2;

        function results(err, key) {
            if(--waiting == 0) {
                expect(stack).to.contain('f1');
                expect(stack).to.contain('f2');
                done();
            }
        }

        function f1(msg, d) {stack.push('f1'); d(null, results)}
        function f2(msg, d) {stack.push('f2'); d(null, results)}

        r.addChannel('channel_1', f1);
        r.addChannel('channel_2', f2);

        r.dispatch(['channel_1', 'channel_2'], {foo: 'bar'});
    });

    it('can late bind channels', function(done) {
        var r = testLevelBus({flushBatch: 3});
        var c = 0;

        async.times(
            10,
            (i, next) => {
                c++;
                r.dispatch('channel_1', {item: i}, (err, msg) => {
                    if(i == 9) {
                        r.addChannel('channel_1', (data, next) => {

                            if(data.item == 3) {
                                c++;
                                r.dispatch('channel_1', {item: 10});
                            }

                            setTimeout(next, 5);
                            if(data.item == 9) {
                                expect(c).to.equal(11);
                                done();
                            }
                        });
                    }
                });
            });

    });

    it('can merge extra data', function(done) {
        var r = testLevelBus();

        r.addChannel('channel_1', function(data, next) {
            expect(data.foo).to.equal('FOO');
            next(null, {bar: 'BAR'});
        }, 'channel_2');

        r.addChannel('channel_2', function(data, next) {
            expect(data.foo).to.equal('FOO');
            expect(data.bar).to.equal('BAR');
            next();
            done();
        });

        r.dispatch('channel_1', {foo: 'FOO'});
    });

    it('can modify the channel', function(done) {
        var r = testLevelBus();

        r.addChannel('channel_1', function(data, d) {
            //expect(msg._next).to.eql(['channel_2']);
            data._next = 'channel_3';
            d();
        }, 'channel_2');

        r.addChannel('channel_2', function(msg, d) {
            d();
            done(new Error("Shouldn't get here"));
        });

        r.addChannel('channel_3', function(msg, d) {
            d();
            done();
        });

        r.dispatch('channel_1', {foo: 1});
    });

    it('should retry and log errors', function(done) {
        var r = testLevelBus({retries: 2, delay: 5});

        r.addChannel('channel_1', function(msg, d) {
            d(new Error("Test error"));
        });
        
        var expectedSha = "not set";
        var message = "not set";

        r.on('failure', function(workerErr, channel, payload) {
            expect(workerErr.message).to.equal('Test error');
            expect(channel).to.equal('channel_1');
            //expect(payload).to.equal(expectedId);
            r.messageStats(function(err, stats) {
                if(err) return done(err);
                expect(stats['default']).to.eql({ok: 1});
                expect(stats['channel_1']).to.eql({error: 1});
                done();
            });
        });

        
        r.dispatch('channel_1', {foo: 'bar'}, function(err, msg) {
            expectedSha = msg._sha;
            message = msg._id;
        });
    });

    it('should handle code errors', function(done) {
        var r = testLevelBus({retries: 3, delay: 5});

        r.addChannel('channel_1', function(msg, d) {
            nonExistentFunction();
            d();
        });
        
        r.on('failure', function(e, channel, payload) {
            expect(e.message).to.equal('nonExistentFunction is not defined');
            r.messageStats(function(err, stats) {
                expect(stats['default']).to.eql({ok: 1});
                // fails on first run
                expect(stats['channel_1']).to.eql({error: 1});
                expect(r.channels['channel_1'].paused).to.be.true;

                r.dispatch('channel_1', {foo: 'nar'}, () => {
                    //console.log(r.channelStatus('channel_1'));
                });
                //console.log(r.channelStatus('channel_1'));

                done(err);
            });
        });

       r.dispatch('channel_1', {foo: 'bar'});
    });

    it('should handle errors after next()', function(done) {
        var r = testLevelBus({});

        r.addChannel('channel_1', function(msg, d) {
            d();
            nonExistentFunction();
        });

        r.on('failure', function(e, channel, payload) {
            expect(e.message).to.equal('nonExistentFunction is not defined');
            done();
        });

        r.dispatch('channel_1', {foo: 'bar'});
    });

    it('should reject bad channels', function() {
        var r = testLevelBus();

        r.addChannel('channel_1', noop);
        expect(r.channelExists('channel_1')).to.be.true;
        expect(r.channelExists('channel_2')).to.be.false;
    });

    it('should handle calls to dispatch with 2 args', function(done) {
        var r = testLevelBus();
        var id = 'not set';

        r.addChannel('channel_1', function(msg, d) {
            expect(msg._id).to.equal(id);
            d(null, function(err, metaId) {
                if(err) return done(err);
                expect(metaId).to.be.string;
                done();           
            });

        });

        r.dispatch('channel_1', function(err, msg) {
            id = msg._id;
        });
    });

    it('should handle calls to dispatch with 3 args', function(done) {
        var r = testLevelBus();
        var id = 'not set';

        r.addChannel('channel_1', function(msg, d) {
            expect(msg._id).to.equal(id);
            d(null, function(err, msgId, metaId) {
                if(err) return done(err);
                expect(msg.foo).to.equal('bar');
                done();
            });
        });

        r.dispatch('channel_1', {foo: 'bar'}, function(err, msg) {
            id = msg._id;
        });
    });

    it('should recover from failure', function(done) {
        var r1 = testLevelBus();
        var r2 = null;

        var stack = [];

        function succeed(msg, d) {
            // this version succeeds
            expect(msg.foo).to.equal('bar');
            d();
            r2.pending((err, pending) => {
                expect(pending).to.eql([]);
                done(err);
            });
        }

        function fail(msg, d) {
            // dont call the dispatcher as if we failed
            
            // set up a new router
            
            var opts = {
                blobstore: r1.blobstore,
                db: r1.tables
            }

            delete(r1);
            r2 = testLevelBus(opts);
            r2.addChannel('channel_1', succeed);
            // shouldn't have to do anything else;

            setTimeout(d, 50);
            
        }

        r1.addChannel('channel_1', fail);
        r1.dispatch('channel_1', {foo: 'bar'});

    });

    it('should generate stats', function(done) {
        var r = testLevelBus();
        var waiting = 2;

        r.addChannel('channel_1', noop, 'channel_2');
        r.addChannel('channel_2', function(msg, d) {
            d(null, function() {
                if(--waiting == 0) {
                    r.messageStats(function(err, stats) {
                        expect(stats['default']).to.eql({ok: 2});
                        expect(stats['channel_1']).to.eql({ok: 1});
                        expect(stats['channel_2']).to.eql({ok: 2});
                        done();
                    });
                }
            });
        });
        
        r.dispatch('channel_1', {foo: 'nar'});
        r.dispatch('channel_2', {bar: 'far'});
    });

    it('can return history for a particular message', function(done) {
        var r = testLevelBus();

        var dispatched = {};

        r.addChannel('channel_1', function(msg, d) {
            setTimeout(function() {
                d();
                if(msg.foo == 3) {

                    r.getMessageHistory(dispatched.msgId, function(err, data) { 
                        if(err) return done(err);
                        expect(data[0]._id).to.equal(dispatched.msgId);
                        expect(data[0]._route).to.eql([]);
                        expect(data[1]._id).to.equal(dispatched.msgId);
                        expect(data[1]._route).to.eql(['channel_1']);
                        done();
                    });
                }
            }, 10);
        });

        r.dispatch('channel_1', {foo: 1});
        r.dispatch('channel_1', {foo: 2}, function(err, msg) {
            dispatched.msgId = msg._id;
            dispatched.metaId = msg._sha;
        });
        r.dispatch('channel_1', {foo: 3});
    });

    it('can list pending jobs', function(done) {
        var r = testLevelBus();

        r.addChannel('channel_1', noop);

        r.dispatch('channel_1', function(err, msg) {
            r.pending(function(err, result) {
                expect(result).to.eql([{channel: 'channel_1', ts: msg._ts, sha: msg._sha}]);
                done();
            });
        });
    });

    it('can list processed messages', function(done) {
        var r = testLevelBus();
        var id = 'not set';

        r.addChannel('channel_1', function(msg, d) {
            d(null, function(err) {
                r.getMessages(function(err, messages) {
                    expect(messages.length).to.equal(1);
                    expect(messages[0].id).to.equal(id);
                    done();
                });
            });
        });

        r.dispatch('channel_1', {foo: 'bar'}, function(err, msg) {
            if(err) return done(err);
            id = msg._id;
        });
    });

    it('can list processed messages in a range', function(done) {
        var r = testLevelBus();
        var dispatched = [];

        r.addChannel('channel_1', function(msg, d) {
            d();
            if(msg.item == 9) {
                // mid range
                r.getMessages(dispatched[3], dispatched[6], function(err, data) {
                    if(err) return done(err);
                    var result = data.map((x) => bytewise.decode(x.id));
                    var expected = dispatched.slice(3, 7);
                    expected.reverse();
                    expect(result).to.eql(expected);

                    // no end
                    r.getMessages(dispatched[4], function(err, data) {
                        if(err) return done(err);

                        var result = data.map((x) => bytewise.decode(x.id));
                        var expected = dispatched.slice(4);
                        expected.reverse();
                        expect(result).to.eql(expected);
                        done();
                    });

                });
            }
        });

        for(var i=0; i<10; i++) {
            r.dispatch('channel_1', {item: i}, function(err, msg) {
                dispatched.push(bytewise.decode(msg._id));
            });
        }
    });

    it('should run concurrenly', function(done) {
        var r = testLevelBus();
        var stack = [];
        var total = 0;

        r.addChannel('channel_1', function(msg, next) {
            var i = msg.item;
            total += i
            stack.push('A' + i);
            setTimeout(() => {
                stack.push('B' + i);
                total += i * 10
                next(null, () => {
                    stack.push('D' + i);
                });
                stack.push('C' + i);
            }, 10);
        }, 'channel_2');

        r.addChannel('channel_2', function(msg, next) {
            var i = msg.item;
            stack.push('E' + i);
            total += i * 100
            next();
            if(i==9) {
                //console.log(stack.join(' '));
                expect(stack.length).to.equal(50);
                expect(total).to.equal(4995);
                //console.log(r.channelStatus());
                done();
            }
        });

        for(var i=0; i<10; i++) {
            r.dispatch('channel_1', {item: i});
        }
    });

    it('should be able to process 1000 messages per second', function(done) {
        this.timeout(5000);
        this.slow(1000);

        var r = testLevelBus();
        var stats = {channel_1: 0, channel_2: 0};

        var count = 1000;
        var start = Date.now();

        r.addChannel('channel_1', function(msg, next) {
            stats.channel_1++;
            next();
        }, 'channel_2');

        r.addChannel('channel_2', function(msg, next) {
            stats.channel_2++;
            next();

            if(msg.item == count-1) {
                var mps = Math.floor(count * 2 * 1000 / (Date.now() - start));
                expect(stats.channel_1).to.equal(count);
                expect(stats.channel_2).to.equal(count);
                console.log("Messages per second: %d", mps);
                expect(mps).to.be.above(1000);
                done();
            }
        });

        async.times(count, (i) => {
            r.dispatch('channel_1', {item: i});
        });

    });

    it('should work with leveldown', function(done) {
        var db = "/tmp/level-view-test";
        expect(fs.existsSync(db)).to.be.false;

        var r = new LevelBus({datadir: db});

        expect(fs.existsSync(db)).to.be.true;
        rm(db, done);

    });
});

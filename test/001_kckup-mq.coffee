###
###

vows = require('vows')
assert = require('assert')

suite = vows.describe('Kckup MQ')

redis_config =
  host: "127.0.0.1"
  port: 6379
  db: null
  auth:
    password: null

# Redis config for use inside Heroku nodes
if process.env.REDISTOGO_URL
  rtg = require("url").parse(process.env.REDISTOGO_URL)
  redis_config.host = rtg.hostname
  redis_config.port = rtg.port
  redis_config.auth.password = rtg.auth.split(":")[1]

suite.addBatch
  'kckupMQ':
    topic: -> require("#{__dirname}/../lib/kckupmq")
    'should be object': (topic) ->
      assert.isObject topic
    'should have method instance': (topic) ->
      assert.isFunction topic.instance      
    'should have method KckupMQ': (topic) ->
      assert.isFunction topic.KckupMQ
    'should have method RedisMQ': (topic) ->
      assert.isFunction topic.RedisMQ
    'should have method RabbitMQ': (topic) ->
      assert.isFunction topic.RabbitMQ

suite.addBatch
  'kckupMQ.KckupMQ':
    topic: ->
      @instance = new (require("#{__dirname}/../lib/kckupmq").KckupMQ)()
      require("#{__dirname}/../lib/kckupmq").KckupMQ
    'should be function': (topic) ->
      assert.isFunction topic
    'should be instance of EventEmitter': (topic) ->
      assert.instanceOf @instance, require("events").EventEmitter
    'instance should have method initialize': (topic) ->
      assert.isFunction @instance.initialize
    'instance should have method subscribe': (topic) ->
      assert.isFunction @instance.subscribe
    'instance should have method unsubscribe': (topic) ->
      assert.isFunction @instance.unsubscribe
    'instance should have method publish': (topic) ->
      assert.isFunction @instance.publish    
    'instance should have method getSize': (topic) ->
      assert.isFunction @instance.getSize
    'instance should have method clearTopicQueue': (topic) ->
      assert.isFunction @instance.clearTopicQueue
    'instance should have method getTopics': (topic) ->
      assert.isFunction @instance.getTopics      
    'instance should have method disconnect': (topic) ->
      assert.isFunction @instance.disconnect
    'instance should throw error on unimplemented methods': (topic) ->
      assert.throws =>
        @instance.disconnect()
      , Error
    'instance constructor should pass extra arguments to initialization method': (topic) ->
      klass = require("#{__dirname}/../lib/kckupmq").KckupMQ
      assert.doesNotThrow ->
        new klass({}, 'arg1', 'arg2')
      , Error


suite.addBatch
  'kckupMQ.RedisMQ':
    topic: ->
      require("#{__dirname}/../lib/kckupmq").RedisMQ
    'should be function': (topic) ->
      assert.isFunction topic
    'instance should extend KckupMQ': (topic) ->
      assert.instanceOf (new topic()), require("#{__dirname}/../lib/kckupmq").KckupMQ
    'instance should generate clientId if not given': (topic) ->
      instance = new topic()
      assert.isNotNull instance.clientId
      assert.lengthOf instance.clientId, 36
    'instance should use given clientId': (topic) ->
      instance = new topic({}, 'test-client-id')
      assert.isNotNull instance.clientId
      assert.equal instance.clientId, 'test-client-id'

suite.addBatch
  'kckupMQ.RedisMQ instance':
    topic: ->
      @test_queue_name = 'test-queue'
      @config = redis_config
      @instance = new (require("#{__dirname}/../lib/kckupmq").RedisMQ)(@config)
      @instance
    'should subscribe to queue':
      topic: (topic) -> 
        topic.subscribe @test_queue_name, @callback
        return
      'and get list of current subscriptions': (topics) ->
        assert.isNotNull topics
        assert.isArray topics
    'should unsubscribe from queue':
      topic: (topic) ->
        topic.unsubscribe @test_queue_name, @callback
        return
      'and get list of remaining subscriptions': (topics) ->
        assert.isNotNull topics
        assert.isArray topics
    'should publish to queue':
      topic: (topic) ->
        topic.publish @test_queue_name, {hello: 'world'}, @callback
        return
      'with proper id': (id) ->
        assert.isNotNull id
        assert.lengthOf id, 36
      'and report queue size':
        topic: ->
          @instance.getSize @test_queue_name, @callback
          return
        'with correct size': (size) ->
          assert.isNotNull size
          assert.isNumber size
    'should catch published message':
      topic: ->
        @instance.subscribe @test_queue_name, () =>
          @instance.on @test_queue_name, @callback
          @instance.publish @test_queue_name, {hello: 'world'}
        return
      'with correct id and data': (id, data) ->
        assert.isNotNull id
        assert.isNotNull data
    'should be able to clear topic queue':
      topic: ->
        @instance.clearTopicQueue @test_queue_name, =>
          @instance.getSize @test_queue_name, @callback
        return
      'with correct size': (size) ->
        assert.isNotNull size
        assert.isNumber size
        assert.equal size, 0

suite.addBatch
  'kckupMQ.RabbitMQ':
    topic: ->
      require("#{__dirname}/../lib/kckupmq").RabbitMQ
    'should be function': (topic) ->
      assert.isFunction topic
    'instance should extend KckupMQ': (topic) ->
      assert.instanceOf (new topic()), require("#{__dirname}/../lib/kckupmq").KckupMQ

suite.addBatch
  'kckupMQ.instance':
    topic: -> 
      @config =
        host: "localhost"      
      require("#{__dirname}/../lib/kckupmq")
    'should return null with unknown type': (topic) ->
      assert.isNull topic.instance('unknown')
    'should return RedisMQ instance with type "redis"': (topic) ->
      assert.instanceOf topic.instance('redis'), require("#{__dirname}/../lib/kckupmq").RedisMQ
    'should return RabbitMQ instance with type "rabbit"': (topic) ->
      assert.instanceOf topic.instance('rabbit'), require("#{__dirname}/../lib/kckupmq").RabbitMQ

suite.export module

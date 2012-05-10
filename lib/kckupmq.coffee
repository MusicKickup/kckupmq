###
# MKS Kckup-MQ library #

This library can handle MessaQueues with different backends.

###

EventEmitter = require("events").EventEmitter
_ = require('underscore')._
uuid = require('node-uuid')
async = require('async')
util = require('util')

class KckupMQ extends EventEmitter
  ###
  Base MQ class.
  This class defines common interfaces.
  All backend implementations should extend this
  ###
  
  defaults: {}
  config: {}
  
  constructor: (config) ->
    super()
    @config = _.extend @defaults, config
    init_args = []
    if arguments.length > 1
      init_args = _.filter arguments, (v,k) ->
        return k > 0
    @initialize.apply(@, init_args)
  
  initialize: -> return
  subscribe: (topic, next) -> throw new Error("subscribe not implemented")
  unsubscribe: (topic) -> throw new Error("unsubscribe not implemented")
  publish: (topic, data, next) -> throw new Error("publish not implemented")
  getSize: (topic, next) -> throw new Error("getSize not implemented")
  clearTopicQueue: (topic, next) -> throw new Error("clearTopicQueue not implemented")
  getTopics: (next) -> throw new Error("getTopics not implemented")
  disconnect: (next) -> throw new Error("disconnect not implemented")

exports.KckupMQ = KckupMQ
    
class RedisMQ extends KckupMQ
  ###
  Redis implementation
  ###
  
  defaults:
    host: '127.0.0.1'
    port: 6379
    auth:
      password: null
    no_ready_check: false
  
  clientId: null
  pub_connected: -1
  sub_connected: -1
  
  initialize: (@clientId) ->
    ###
    ###
    @redis = require("redis")
    @redis.debug_mode = false
    
    @clientId = @_generateClientId() unless @clientId
    
    opts = {}
    if @config.db
      opts.db = @config.db
    if @config.no_ready_check
      opts.no_ready_check = @config.no_ready_check
    
    @_connectPubRedis(opts)
    @_connectSubRedis(opts)
    
  _connectPubRedis: (opts) ->
    ###
    ###
    return if @pub_connected == 1    
    @pub_connected = 0
    
    @pub = @redis.createClient @config.port, @config.host, opts    
    
    if @config.auth and @config.auth.password
      @pub.auth @config.auth.password
    
    @pub.on 'connect', () =>
      #console.log '"publish" redis client connected'
      @pub_connected = 1      
    @pub.on 'error', (error) ->
      console.error 'error in "publish" redis client',error
    @pub.on 'end', () =>
      #console.log '"publish" redis client disconnected',arguments
      @pub_connected = -1
      
  _connectSubRedis: (opts) ->
    ###
    ###
    return if @sub_connected == 1    
    @pub_connected = 0
    
    @sub = @redis.createClient @config.port, @config.host, opts
    
    if @config.auth and @config.auth.password
      @sub.auth @config.auth.password
    
    @sub.on 'connect', () =>
      #console.log '"subscribe" redis client connected'
      @sub_connected = 1
      
      @sub.on 'message', (channel, message) =>
        data = JSON.parse message
        @getTopics (err, topics) =>
          return unless topics
          return if topics.indexOf(data.topic) == -1
          @_popFromQueue data.topic, (err, item) =>
            return unless item
            @emit data.topic, item.id, item.value
    @sub.on 'error', (error) ->
      console.error 'error in "subscribe" redis client',error
    @sub.on 'end', () =>
      #console.log '"subscribe" redis client disconnected',arguments
      @sub_connected = -1
  
  subscribe: (topic, next) ->
    ###
    ###
    client_topics = []
    is_new = false
    
    self = @
    
    async.auto
      'getExistingTopics': (cb) =>
        @getTopics (err, topics) ->
          return cb(err) if err
          client_topics = topics
          cb()
      'addNewIfNecessary': ['getExistingTopics', (cb) =>
        @sub.subscribe @clientId

        if client_topics.indexOf(topic) == -1
          is_new = true
          client_topics.push topic
        cb()
      ]
      'saveTopics': ['addNewIfNecessary', (cb) =>
        @pub.hset '_kckupmq_map', @clientId, JSON.stringify(client_topics), (err, res) ->
          return cb(err) if err
          cb()
      ]
    , (err, results) ->
      next?(err, client_topics)
      
      return unless is_new
      self.getSize topic, (err, size) ->
        return unless size
        [1..size].forEach (idx) ->
          self._popFromQueue topic, (err, item) ->
            return unless item
            self.emit topic, item.id, item.value
  
  unsubscribe: (topics, next) ->
    ###
    ###
    client_topics = []

    unless util.isArray(topics)
      topics = [topics]

    async.auto
      'getExistingTopics': (cb) =>
        @getTopics (err, topics) ->
          return cb(err) if err
          client_topics = topics
          cb()
      'removeFromExisting': ['getExistingTopics', (cb) =>
        topics.forEach (topic) ->
          index = client_topics.indexOf(topic)
          if index != -1
            client_topics.splice index, 1
        cb()
      ]
      'saveTopics': ['removeFromExisting', (cb) =>
        unless client_topics.length          
          @sub.unsubscribe @clientId
            
          @pub.hdel '_kckupmq_map', @clientId, (err, res) ->
            return cb(err) if err
            cb()
        else
          @pub.hset '_kckupmq_map', @clientId, JSON.stringify(client_topics), (err, res) ->
            return cb(err) if err
            cb()
      ]
    , (err, results) ->
      next?(err, client_topics)
  
  publish: (topic, data, next) ->
    ###
    ###
    id = @_getNewItemId topic

    rdata =
      id: id
      topic: topic
      value: data

    queue_name = @_getTopicQueueName(topic)

    @pub.lpush queue_name, JSON.stringify(rdata), (err, res) =>
      return next(err) if err

      pub_data =
        id: id
        topic: topic
      @pub.publish @clientId, JSON.stringify(pub_data)

      next?(null, id)
  
  getSize: (topic, next) -> 
    ###
    ###
    queue_name = @_getTopicQueueName(topic)
    size = 0
    @pub.llen queue_name, (err, res) ->
      return next(err) if err    
      size = parseInt(res, 10) if res
      next(null, size)
  
  clearTopicQueue: (topic, next) ->
    ###
    ###
    queue_name = @_getTopicQueueName(topic)
    @pub.del queue_name, (err, res) ->
      return next(err) if err    
      next(null)
  
  getTopics: (next) -> 
    ###
    ###
    topics = []
    @pub.hget '_kckupmq_map', @clientId, (err, res) ->
      return next(err) if err
      topics = JSON.parse(res) if res
      next(null, topics)
  
  _popFromQueue: (topic, next) ->
    ###
    ###
    queue_name = @_getTopicQueueName(topic)
    @pub.lpop queue_name, (err, res) ->
      return next(err, null) if err or !res
      next(null, JSON.parse(res))
  
  _getTopicQueueName: (topic) ->
    ###
    ###
    "_kckup|#{@clientId}:#{topic}"
  
  _generateClientId: ->
    ###
    ###
    uuid()
  
  _getNewItemId: (topic) ->
    ###
    ###
    uuid()
  
  disconnect: (next) ->
    @pub.quit()
    @sub.quit()
    next?(null)

exports.RedisMQ = RedisMQ

class RabbitMQ extends KckupMQ
  ###
  Rabbit implementation
  ###
  
  defaults:
    host: '127.0.0.1'
    port: 6379
    auth:
      username: null
      password: null
  
  context_ready: false
  sockets: {}
  
  initialize: ->
    @context = require('rabbit.js').createContext @_createConnectionString()
    @context.on 'ready', =>
      @context_ready = true
  
  _createConnectionString: ->
    return @config.url if @config.url
    
    str = "amqp://"
    if @config.auth and @config.auth.username
      str += "#{@config.auth.username}:#{@config.auth.password}@"
    str += "#{@config.host}"
    if @config.port
      str += ":#{@config.port}"
    return str

  subscribe: (topic, next) ->
    connected = =>
      @sockets[topic].SUB.on 'data', (data) ->
        next(null, JSON.parse(data), {})
    
    unless @sockets[topic].SUB
      @sockets[topic] = {} unless @sockets[topic]
      @sockets[topic].SUB = @context.socket('SUB')
      @sockets[topic].SUB.setEncoding('utf8')
    
      @sockets[topic].SUB.connect topic, ->
        connected()
    else
      connected()

  publish: (topic, data, next) ->
    connected = =>
      @sockets[topic].PUB.write JSON.stringify(data), 'utf8'
      next?()
      
    unless @sockets[topic].PUB
      @sockets[topic] = {} unless @sockets[topic]
      @sockets[topic].PUB = @context.socket('PUB')
    
      @sockets[topic].PUB.connect topic, ->
        connected()
    else
      connected()

exports.RabbitMQ = RabbitMQ

_instance = null
_instance_type = null

exports.instance = (type, config, client_id) ->
  ###
  Main entry point which creates new instance
  of given type of implementation
  ###
  
  config ?= {}
  
  if _instance_type and _instance_type == type
    return _instance
  
  _instance_type = type
  if type == 'redis'
    _instance = new RedisMQ(config, client_id)
  if type == 'rabbit'
    _instance = new RabbitMQ(config, client_id)
  
  return _instance

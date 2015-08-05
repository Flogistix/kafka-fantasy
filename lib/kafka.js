const
  K = require ('kafka-node'),
  R = require ('ramda'),
  T = require ('data.task'),
  B = require ('baconjs'),

  NO_COMPRESSION = 0,
  GZIP = 1,
  SNAPPY = 2,

// All of these datatypes are described here
// https://github.com/SOHU-Co/kafka-node/blob/master/README.md

// data ProducerRequest = ProducerRequest { topic :: String
//                                        , messages :: [String]
//                                        , partition :: Int
//                                        , attributes :: 2 }

// data FetchRequest = FetchRequest { topic :: String, offset :: Int }

// data ConsumerOptions = ConsumerOptions { groupId :: String
//                                        , autoCommit :: Boolean
//                                        , autoCommitIntervalMs :: Int
//                                        , fetchMaxWaitMs :: Int
//                                        , fetchMinBytes :: Int
//                                        , fromOffset :: Boolean
//                                        , encoding :: String }

//mkClient :: String -> String -> Client
  mkClient = R.curry (function (appName, connStr) {
    return new K.Client (connStr, appName);
  }),

//closeClient :: Producer -> Task Unit
  closeClient = function (client) {
    return new T (function (reject, resolve) {
      client.zk.close();
      client.close ();
      resolve ({});
    });
  },

// -- Producer related -- //

//mkProducer :: Client -> Producer
  mkProducer = function (c) { return new K.HighLevelProducer (c); },

//producerOnReady :: Producer -> Int -> Task Unit
  producerOnReady = R.curry (function (producer, timeoutMs) {
    return new T (function (reject, resolve) {
      const t = setTimeout ( function () { resolve = id;
                                           reject (new Error ("TIMEOUT")); }
                           , timeoutMs);

      producer.on ('ready', function (x) {
        clearTimeout (t);
        resolve ({});
      });

      producer.on ('error', function (err) {
        clearTimeout (t);
        reject (err);
      });
    });
  }),

//createTopics :: Producer -> [String] -> Task Unit
  createTopics = R.curry (function (producer, topics) {
    return new T (function (reject, resolve) {
      producer.createTopics (topics, true, function (err, data) {
        if (err) { return reject (err);  }
        else     { return resolve ({}); }
      });
    });
  }),

//closeProducer :: Producer -> KafkaM Unit
  closeProducer = function (producer) {
    return new T (function (reject, resolve) {
      producer.close ();
      resolve ({});
    });
  },

//mkProducerRequest :: Topic -> Int -> [String] -> Payload
  mkProducerRequest = R.curry (function (topic, i, xs) {
    return { topic: topic
           , messages: xs
           , partition: i
           , attributes: NO_COMPRESSION };
  }),

//send :: Producer -> [ProducerRequest] -> Task Unit
  send = R.curry (function (producer, payload) {
    return new T (function (reject, resolve) {
      producer.send (payload, function (err, data) {
        if (err) { return reject  (err);  }
        else     { return resolve (data); }
      });
    });
  }),


// -- Consumer related -- //

//mkConsumer :: Client -> [FetchRequest] -> ConsumerOptions -> Consumer
  mkConsumer = R.curry (function (client, requests, opts) {
    return new K.Consumer (client, requests, opts);
  }),

//mkFetchRequest :: String -> Int -> FetchRequest
  mkFetchRequest = R.curry (function (topic, offset) {
    return { topic: topic, offset: offset };
  }),

//mkStreamFromTopic :: Consumer -> String -> EventStream
  mkStreamFromTopic = R.curry (function (consumer, topic) {
    const stream = new B.Bus ();
    consumer.on ('message', function (x) { if (x.topic === topic) { stream.push (x); }});
    return stream;
  }),

  nil = null;
module.exports = {
  createTopics: createTopics,
  closeClient: closeClient,
  closeProducer: closeProducer,
  emptyOpts: {},

  mkClient: mkClient,
  mkConsumer: mkConsumer,
  mkFetchRequest: mkFetchRequest,
  mkProducer: mkProducer,
  mkProducerRequest: mkProducerRequest,
  mkStreamFromTopic: mkStreamFromTopic,

  producerOnReady: producerOnReady,

  send: send,
};

const
  K = require ('../lib/kafka'),
  M = require ('control.monads'),
  A = require ('async'),
  R = require ('ramda'),
  T = require ('data.task'),

  IO = require ('fantasy-io'),

/* Commonly used functions */
  id      = function (x) { return x; },
  konst   = R.curry (function (a, b) { return a; }),
  du      = function (M) { return function () { return R.apply (R.pipe, arguments)(M.of ({})); }; },
  bind    = function (a) { return R.chain (konst (a)); },
  chain   = R.chain,
  map     = R.map,
  ap      = R.curry (function (ma, mf) { return mf.ap (ma); }),

//These are stateful, but for this example we are going to pretend they're not.
  log     = function (x) { console.log (x); return {}; },
  logI    = function (a) { log (a); return a; },

  random  = R.curry (function (min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }),

//handleSIGINT :: Process -> IO a -> IO Unit
  handleSIGINT = R.curry (function (process, f) {
    return IO (function () {
      process.on ('SIGINT', function () { f.unsafePerform (); process.exit (); });
      return {};
    });
  }),

//runTask :: (Err b -> IO) -> (a -> IO) -> Task a -> IO a
  runTask = R.curry (function (rej, res, t) { return IO (function () { t.fork (rej, res); }); }),

//runStream :: (a -> IO) -> Stream a -> IO
  runStream = R.curry (function (f, stream) {
    return IO (function () {
      stream.onValue (f);
    });
  }),

//\\//\\ MAIN //\\//\\
//main :: IO
  main = function () {
    const

    vals = [ [2,0,1,1,1,1,2,1,1,1,0,0,0,10367,0,0,995,0,10262,0,847,20,794,0,1344,700,0,0,0,206,196,0,0,53,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
           , [2,0,1,1,1,1,2,1,1,1,0,0,0,10367,0,0,995,0,10262,0,855,20,797,0,1325,697,0,0,0,207,196,0,0,58,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
           , [2,0,1,1,1,1,2,1,1,1,0,0,0,10367,0,0,995,0,10262,0,916,19,852,0,1342,717,0,0,0,209,198,0,0,64,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
           , [2,0,1,1,1,1,2,1,1,1,0,0,0,10367,0,0,995,0,10263,0,878,22,822,0,1312,705,0,0,0,211,201,0,0,56,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
           , [2,0,1,1,1,1,2,1,1,1,0,0,0,10367,0,0,995,0,10263,0,859,19,800,0,1269,692,0,0,0,211,202,0,0,59,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0] ],

    r       = random (0, 4),
    dataset = R.join (',', vals[r]),

    zkStr    = '172.16.123.99:2181,172.16.123.98:2181,172.16.123.97:2181',
    client   = K.mkClient ('test-app', zkStr),
    producer = K.mkProducer (client),

    payload  = [ K.mkProducerRequest ('test', 0, ["9566:" + dataset]) ],

  //closeOnSigInt :: Task {}
    closeOnSigInt = du(T) ( bind  (K.closeProducer (producer))
                          , chain (R.compose (T.of, handleSIGINT (process))) ),


  //closeClientAndProducer :: Task {}
    closeClientAndProducer = du (T) ( bind (K.closeProducer (producer))
                                    , bind (K.closeClient   (client)) ),

  //createTopicsT :: Task {}
    createTopicsT = du (T) ( bind (K.producerOnReady (producer, 10000))
                           , bind (K.createTopics    (producer, ['test', 'blah']))
                           , bind (K.send            (producer, payload))
                           , bind (closeClientAndProducer) );

    return runTask (logI, id, createTopicsT);
  }(),

  nil = null;
(function runIO (args) {
  main.unsafePerform ();
})(process.argv);

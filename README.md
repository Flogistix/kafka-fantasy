#Data

    data ProducerRequest = ProducerRequest { topic :: String
                                           , messages :: [String]
                                           , partition :: Int
                                           , attributes :: 2 }

    data FetchRequest = FetchRequest { topic :: String, offset :: Int }

    data ConsumerOptions = ConsumerOptions { groupId :: String
                                           , autoCommit :: Boolean
                                           , autoCommitIntervalMs :: Int
                                           , fetchMaxWaitMs :: Int
                                           , fetchMinBytes :: Int
                                           , fromOffset :: Boolean
                                           , encoding :: String }

#Functions

    mkClient :: String -> String -> Client

    closeClient :: Producer -> Task Unit

    mkProducer :: Client -> Producer

    producerOnReady :: Producer -> Int -> Task Unit

    createTopics :: Producer -> [String] -> Task Unit

    closeProducer :: Producer -> Task Unit

    mkProducerRequest :: Topic -> Int -> [String] -> Payload

    send :: Producer -> [ProducerRequest] -> Task Unit

    mkConsumer :: Client -> [FetchRequest] -> ConsumerOptions -> Consumer

    mkFetchRequest :: String -> Int -> FetchRequest

    mkStreamFromTopic :: Consumer -> String -> EventStream

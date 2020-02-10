module.exports = Kafka;
const kafka = require('kafka-node');
function Kafka(_kafkaHost = process.env.kafkaHost) {
    this.kafkaHost = _kafkaHost;
    if (!this.kafkaHost)
        throw new Error('need to set env variable for kafkaHost or set kafkaHost parameter  ! ');
    this.consumer = null;
    this.producer = null;
    return (async () => {
        if (!(this instanceof Kafka)) {
            return new Kafka(this.kafkaHost);
        }
        this.client = await new kafka.KafkaClient({ kafkaHost: this.kafkaHost });
        return this;
    })();
}

Kafka.prototype.initConsume = function (_topic, _options = { autoCommit: true, fetchMaxWaitMs: 5000, fetchMaxBytes: 1024 * 1024 }) {
    this.topic = _topic;
    this.options = _options;
    //this.options = { autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };
    this.consumer = new kafka.Consumer(this.client, [{ topic: _topic, partition: 0 }], this.options);
}

Kafka.prototype.consume = function (cb) {
    if (!this.topic) throw new Error('you need to run initConsume() first ! ');
    this.client.refreshMetadata([this.topic], err => { //function (err) {
        const offset = new kafka.Offset(this.client);
        if (err) {
            throw err;
        }
        this.consumer.on('message', function (message) {
            // var messageObject = JSON.parse(message.value);
            // cb(messageObject);
            cb(message.value);
        });
        /*
         * If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
         */
        this.consumer.on('offsetOutOfRange', topic => {
            offset.fetch([topic], function (err, offsets) {
                if (err) {
                    return console.info(err);
                }
                const min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
                this.consumer.setOffset(topic.topic, topic.partition, min);
            });
        }
        );
    })//.bind(this));
}

Kafka.prototype.initProduce = function (_topic, cb) {
    const config =
    {
        requireAcks: 0,
        ackTimeoutMs: 100
    };
    this.producer = new kafka.Producer(this.client, config);
    this.topic = _topic;
    this.producer.on('ready', function (err) {
        if (err) throw new Error('Producer is not ready !');
        cb(err, 'connected');
    });
}

Kafka.prototype.initProduceAsync = async function (_topic) {
    return await asPromise(this, this.initProduce, _topic);
}

Kafka.prototype.publish = function (_message, cb) {
    var confiq = { topic: this.topic, messages: [_message] }
    Array.isArray(_message) ? confiq = { topic: this.topic, messages: _message } : confiq
    if (!this.producer) throw new Error('you need to run initProduce() first ! ');
    this.client.refreshMetadata([this.topic], err => {
        if (err) {
            console.error(err);
        }
        this.producer.send([confiq], function (err, data) {
            //console.log(data || err);
            if (err)
                console.log(err);
            err ? cb({ status: false }) : cb({ status: true })
        });
    }//.bind(this)
    );
    this.producer.on('error', function (err) {
        console.log(err);
    })
}

Kafka.prototype.close = async function () {
    return await new Promise((resolve, reject) => {
        this.consumer.close(true, function (err) {
            if (err) return reject(err)
            resolve(true)
        });
    })
}
function asPromise(context, callbackFunction, ...args) {
    return new Promise((resolve, reject) => {
        args.push((err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
        if (context) {
            callbackFunction.call(context, ...args);
        } else {
            callbackFunction(...args);
        }
    });
}
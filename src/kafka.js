const EventEmitter = require('events').EventEmitter;
const kafka = require('kafka-node');
class MyKafKa extends EventEmitter {
    constructor(_kafkaHost = process.env.kafkaHost ) {
        super();
        this.kafkaHost = _kafkaHost;
        this.client = null;
        this.consumer = null;
        this.producer = null;
        this.initClient();
    }

    initClient() {
        var self = this;
        this.client = new kafka.KafkaClient({ kafkaHost: this.kafkaHost });
        this.client.on('connect', function () {
            self.emit('clientReady');
        });
    }
    initConsumer(_topic, _options = { autoCommit: true, fetchMaxWaitMs: 5000, fetchMaxBytes: 1024 * 1024 }) {
        var self = this;
        if (!this.client) throw new Error('you need to run initClient() first ! ');
        this.consumer = new kafka.Consumer(this.client, [{ topic: _topic, partition: 0 }], _options);
        this.consumer.topic = _topic;
        this.consumer.options = _options;
        this.emit('consumerReady', this);
        this.consumer.on('error', function (err) {
            self.emit('error', err);
        })
    }
    initProducer(_topic, _config = { requireAcks: 0, ackTimeoutMs: 100 }) {
        var self = this;
        if (!this.client) throw new Error('you need to run initClient() first ! ');
        this.producer = new kafka.Producer(this.client, _config);
        this.producer.topic = _topic;
        this.producer.config = _config
        this.producer.on('ready', function (err) {
            if (err) return self.emit('error', err)
            self.emit('producerReady', this);
        });
        this.producer.on('error', function (err) {
            self.emit('error', err);
        })
    }
    consume(cb) {
        var self = this;
        if (!this.consumer.topic) throw new Error('you need to run initConsumer() first ! ');
        this.consumer.on('message', function (message) {
            cb(message.value);
        });
    }
    produce(_message, cb) {
        var self = this;
        let confiq = { topic: this.producer.topic, messages: [_message] }
        Array.isArray(_message) ? confiq = { topic: this.producer.topic, messages: _message } : confiq
        this.producer.send([confiq], function (err, data) {
            if (err) {
                self.emit('error', err);
                cb(false);
            }
            cb(true);
        });

    }

}

module.exports = MyKafKa;

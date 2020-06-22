const EventEmitter = require('events').EventEmitter;
const Kafka = require('./kafka');
const _ = require("lodash");
const topic = "TRACELOG";
let instance;


class    extends EventEmitter {
    constructor(_kafkaHost = process.env.kafkaHost) {
        super();
        this.kafkaHost = _kafkaHost;
    }
    init() {
        this.on('TraceLogData', function (msg) {
            let kafka = new Kafka(this.kafkaHost);
            kafka
                .on('error', function (err) {
                    console.log('KAFKA ERROR    ' + err)
                    this.emit('error', err)
                })
                .on('producerReady', function () {
                    kafka.produce(msg, result => {
                        kafka.producer.close(function () { });
                    });
                })
                .on('clientReady', function () {
                    kafka.initProducer(topic);
                });
        })
        return this
    }
    add(_error, _keyValue, _className) {
        try {
            let data = _.pick(_error, ["code", "name", "message", "stack"]);
            let messageToQueue = {
                errorName: data.name,
                errorCode: data.code,
                errorMessage: data.message,
                keyValue: _keyValue,
                errorStack: data.stack.toString().substring(0, 300),
                className: _className,
                methodName: findMethodName(_error),
                lineOfError: findLineOfError(_error),
                serviceName: process.env.APP_ID,
                createdAt: Date.now()
            };
            let msg = JSON.stringify(messageToQueue);
            this.emit('TraceLogData',msg);
            return true;
        }
        catch (error) {
            this.emit('error', error)
        }
    }

}

TraceLog.getInstance =  function (_kafkaHost) {
    if (!instance) {
        const errorlog = new TraceLog(_kafkaHost);
        instance = errorlog.init()
    }
    return instance;
};

module.exports = TraceLog;

function findMethodName(_error) {
    try {
        const firstLine = _error.stack.split("\n")[1].toString()
        const methodName = firstLine.trim().split(" ")[1]
        return methodName ? methodName : ' '
    }
    catch (error) {
        this.emit('error', error)
    }
}

function findLineOfError(_error) {
    try {
        const firstLine = _error.stack.split("\n")[1].toString()
        const lineOfError = firstLine.trim().split(":")[firstLine.trim().split(":").length - 2]
        return lineOfError ? lineOfError : ' '
    }
    catch (error) {
        this.emit('error', error)
    }
}


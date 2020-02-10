const EventEmitter = require('events').EventEmitter;
const Kafka = require('./kafka');
const _ = require("lodash");
const topic = "ERRORLOG";

class ErrorLog extends EventEmitter {
    constructor(_kafkaHost = process.env.kafkaHost) {
        super();
        this.kafka = new Kafka(_kafkaHost);
        this.initial();
    }
    initial() {
        var self = this;
        this.kafka
            .on('clientReady', function () {
                self.kafka.initProducer(topic);
            })
            .on('producerReady', function () {
                self.on('ErrorLogData', function (_ErrorLog) {
                    _ErrorLog.kafka.produce(_ErrorLog.msg, result => {
                        return result ? true : false;
                    });
                })
            })
            .on('error', function (err) {
                console.log(err)
            })
            ;
    }
    addDataLog(_error, _keyValue, _className) {
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
            this.msg = JSON.stringify(messageToQueue);
            this.emit('ErrorLogData', this);
        }
        catch (error) {
            this.emit('error', error)
        }
    }
}


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

module.exports = ErrorLog;
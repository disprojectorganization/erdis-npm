const EventEmitter = require('events').EventEmitter;
const Kafka = require('./kafka');
const _ = require("lodash");
const topic = "EVENTLOG";
let instance;


class    extends EventEmitter {
    constructor(_kafkaHost = process.env.kafkaHost) {
        super();
        this.kafkaHost = _kafkaHost;
    }
    init() {
        this.on('EventLogData', function (msg) {
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
            let data = _.pick(_error, ["remoteAddr", "remoteUser", "method", "url", "status", "responseSize", "responseTime", "token", "createdAt", "appId" ]);
            let messageToQueue = {
                remoteAddress: data.remoteAddr,
                remoteUser: data.remoteUser,
                method: data.method,
                url: data.url,
                status: data.status,
                responseSize:  data.responseSize,
                responseTime: data.responseTime,
                token: data.token,
                eventAt : data.createdAt,
                country: country.countryName,
                appId: data.appId ? data.appId : data.url.split('/')[2].toLowerCase()
            };
            let msg = JSON.stringify(messageToQueue);
            this.emit('EventLogData',msg);
            return true;
        }
        catch (error) {
            this.emit('error', error)
        }
    }

}

EventLog.getInstance =  function (_kafkaHost) {
    if (!instance) {
        const eventlog = new EventLog(_kafkaHost);
        instance = eventlog.init()
    }
    return instance;
};

module.exports = EventLog;

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


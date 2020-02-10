

module.exports = ErrorLog;
const _ = require("lodash");
const Kafka = require('./kafka');
const topic = "ERRORLOG";

function ErrorLog(_kafkaHost = process.env.kafkaHost) {
    return (async () => {
        if (!(this instanceof ErrorLog)) {
            return new ErrorLog();
        }
        this.kafka  = await new Kafka(_kafkaHost);
        this.connected = await this.kafka.initProduceAsync(topic)
        return this;
    })();
}
ErrorLog.prototype.addDataLog = async function (_error, _keyValue, _className) {
    try {
        if (this.connected == "connected" && _error) {
            var data = _.pick(_error, ["code", "name", "message", "stack"]);
            var messageToQueue = {
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
            var msg = JSON.stringify(messageToQueue);
            this.kafka.publish(msg, result => {
                return result ? true : false
            })
        }
    } catch (error) {
        console.log(error);
    }
};

function findMethodName(_error) {
    const firstLine = _error.stack.split("\n")[1].toString()
    const methodName = firstLine.trim().split(" ")[1]
    return methodName ? methodName : ' '
}

function findLineOfError(_error) {
    const firstLine = _error.stack.split("\n")[1].toString()
    const lineOfError = firstLine.trim().split(":")[firstLine.trim().split(":").length - 2]
    return lineOfError ? lineOfError : ' '
}
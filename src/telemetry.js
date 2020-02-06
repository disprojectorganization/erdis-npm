
module.exports = Telemetry;

const Kafka = require('./kafka');
const topic = "TELEMETRY";
const ProcessType = {
    consumed: 0,
    produced: 1,
    inserted: 2,
    processing: 3,
    Idle: 4
}

function Telemetry(_kafkaHost = process.env.kafkaHost) {
    return (async () => {
        if (!(this instanceof Telemetry)) {
            return  new Telemetry();
        }
        this.kafka = await new Kafka(_kafkaHost);
        this.connected = await this.kafka.initProduceAsync(topic)
        console.log(this.connected);
        return this;
    })();
}

Telemetry.prototype.addDataLog = async function (input = { dataType: '', dataValue: '', processType: ProcessType.Idle }) {
    try {
        if (this.connected == "connected")
            var inputStr = JSON.stringify(input);
        this.kafka.publish(inputStr, result => {
            return result ? true : false
        })
    } catch (error) {
        console.log(error);
    }
};

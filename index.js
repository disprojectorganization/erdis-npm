var ErrorLog = require('./src/errorLog');
var EventLog = require('./src/eventLog');
var Kafka = require('./src/kafka');
var Telemetry = require('./src/telemetry');
var TraceLog = require('./src/traceLog');

module.exports = {
    ErrorLog: ErrorLog,
    EventLog: EventLog,
    Kafka: Kafka,
    Telemetry: Telemetry,
    TraceLog: TraceLog
};
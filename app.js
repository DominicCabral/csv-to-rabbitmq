const fs = require('file-system');
const stream = fs.createReadStream("data.csv");
const csv = require('fast-csv');
const celery = require('node-celery');
const client = celery.createClient({
    CELERY_BROKER_URL: "amqp://guest:guest@localhost:5672//",
    CELERY_ROUTES: {
        'daemon.run': {
            queue: 'task'
        }
    },
    IGNORE_RESULT: true
});

/**
 * Send the message to rabbit
 */
var csvStream = csv()
    .on("data", function(data) {
        var messageToRabbit = {
            "data": data[0]
        };
        console.log(messageToRabbit);
        var daemon = client.createTask("daemon.run");
        daemon.call([messageToRabbit]);
    })
    .on("end", function() {
        console.log("done");
        process.exit();
    });

/**
 * Once rabbit connected, begin CSV stream
 */
client.on('connect', () => {
    stream.pipe(csvStream);
});
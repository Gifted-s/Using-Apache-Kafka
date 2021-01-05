
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['192.168.99.100:9092']
});

const topicName = 'createOrderEvent';

const msg = JSON.stringify({ customerId: 1, orderId: 1 });
async function processProucer() {
    const producer = kafka.producer()
    await producer.connect()
    for (let i = 0; i < 3; i++) {
       await producer.send({
            topic: topicName,
            messages: [
                { value: msg }
            ]
        })
    }
}

processProucer().then(() => {
    console.log('done')
    process.exit()
})



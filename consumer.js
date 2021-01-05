const { Kafka } = require('kafkajs')
const kafka = new Kafka({
    clientId: 'app-id',
    brokers: ['192.168.99.100:9092']
})
const topicName = "createOrderEvent"
const consumerNumber = process.argv[2] || '1'

const processConsumer = async () => {
    const orderGroup = kafka.consumer({ groupId: 'orderGroup' })
    const notificationGroup = kafka.consumer({ groupId: 'notificationGroup' })
    const paymentGroup = kafka.consumer({ groupId: 'paymentGroup' })

    await Promise.all([
        orderGroup,
        notificationGroup,
        paymentGroup
    ])


    await Promise.all([
        orderGroup.connect(),
        notificationGroup.connect(),
        paymentGroup.connect()
    ])

    await Promise.all([
        orderGroup.subscribe({ topic: topicName }),
        notificationGroup.subscribe({ topic: topicName }),
        paymentGroup.subscribe({ topic: topicName })
    ])

    let orderCounter = 1;
    let paymentCounter = 1;
    let notificationCounter = 1;

    await orderGroup.run({
        eachMessage: async ({ topic, message, partition }) => {
            throw new Error('some error got in the way which didnt let the message be consumed successfully');
            logMessage(orderCounter, `ordersConsumer#${consumerNumber}`, topic, partition, message);
            orderCounter++
        }
    })
    await paymentGroup.run({
        eachMessage: async ({ topic, message, partition }) => {
            logMessage(orderCounter, `paymentssConsumer#${consumerNumber}`, topic, partition, message);
            paymentCounter++
        }
    })
    await notificationGroup.run({
        eachMessage: async ({ topic, message, partition }) => {
         
            logMessage(orderCounter, `notificationConsumer#${consumerNumber}`, topic, partition, message);
            notificationCounter++
        }
    })
}
const logMessage = (counter, consumerName, topic, partition, message) => {
    console.log(`received a new message number: ${counter} on ${consumerName}: `, {
        topic,
        partition,
        message: {
            offset: message.offset,
            headers: message.headers,
            value: message.value.toString()
        },
    });
};

processConsumer();

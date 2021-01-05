const {Kafka} = require('kafkajs')
const kafka = new Kafka({
    clientId:'app-id',
    brokers:['192.168.99.100:9092']  
})
const topicName= "createOther"
const process = async ()=>{
   const admin = kafka.admin()
   await  admin.connect()
   await  admin.createTopics({
       topics: [
        {
            topic: topicName,
            numOfPartitions:2,
            replicationFactor: 1
        }
    ]
   })
   await  admin.disconnect()
}

process().then(()=> console.log('Kafka Connected'))
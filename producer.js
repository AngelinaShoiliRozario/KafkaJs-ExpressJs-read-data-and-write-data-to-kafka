const { Kafka } = require('kafkajs')
const { Partitioners } = require('kafkajs')
const Chance = require('chance');
const chance = new Chance;

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })
const produceMessage = async () => {
  const value  =chance.city();
  const topic = 'test';
  console.log(value);
  try {
    await producer.send({
      topic,
      messages: [
        { value: value }
      ]
    });
  } catch (error) {
    console.log(error);
  }
}

const run = async () => {
  // Producing
  await producer.connect()
  await producer.send({
    topic: 'test',
    messages: [
      { value: 'text' },
    ],
  })
  // setInterval(produceMessage, 1000);
}



run().catch(console.error)

module.exports = {  run };
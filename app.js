const { Kafka } = require('kafkajs');
const express = require('express');
const bodyParser = require('body-parser');
const { Partitioners } = require('kafkajs')


const app = express();
const port = 3000;

// configure Kafka producer
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});


const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
const consumer = kafka.consumer({ groupId: 'test-consumerGroup' });
const messages = []; // array to store the messages
// const consumeMessage = async () => {
//   let message = [];
//   // Consuming
//   await consumer.connect()
//   await consumer.subscribe({ topic: 'test', fromBeginning: true })
//   await consumer.run({
//     eachMessage: async ({ topic, partition, message }) => {
//       console.log({
//         partition,
//         offset: message.offset,
//         value: message.value.toString(),
//       });
//       // if(message.length == 2){
//       //   return message;
//       // }
//     },
//   })
// }
async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'test', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      messages.push(message.value.toString()); // add message to the array
    },
  });
};

startConsumer().catch(console.error);

app.get('/messages', (req, res) => {
  res.json(messages); // return the array of messages
});
const produceMessage = async () => {

  const topic = 'test';
  try {
    await producer.send({
      topic,
      messages: [
        { value: 'this is value from app js' }
      ]
    });
  } catch (error) {
    console.log(error);
  }
}
// create topic
async function createTopic() {
  await producer.connect();
  await producer.send({
    topic: 'test',
    messages: [{ value: 'Hello KafkaJS' }]
  });
  console.log('Topic created');
}

// createTopic();

// use body-parser middleware to parse request body
app.use(bodyParser.json());

// route to receive data and send it to Kafka producer
app.post('/send-data', async (req, res) => {
  try {

    const run = async () => {
      // Producing
      await producer.connect()
      await producer.send({
        topic: 'test',
        messages: [
          { value: 'this is text from app.js again' },
        ],
      })
      // setInterval(produceMessage, 1000);
    }
    run().catch(console.error)
  } catch (error) {
    // console.error(error);
    res.status(500).send('Error sending message to Kafka producer');
  }
});


app.get('/consume_data', (req, res) => {
  try {
    consumeMessage();
  } catch (error) {
    console.log(error);
  }
});

// start server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
const net = require('net');
const crypto = require('crypto');
const mongoose = require('mongoose'); // Require mongoose and set up the MongoDB connection

const passphrase = 'my-secret-passphrase';

// Function to generate a SHA-256 hash
const computeHash = (data) => crypto.createHash('sha256').update(data).digest('hex');

// Connect to MongoDB
mongoose.connect('mongodb://localhost:27017/encrypted_timeseries', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).catch(console.error);

// const db = mongoose.connection;

// Define a schema for time-series data
const timeSeriesSchema = new mongoose.Schema({
  timestamp: { type: Date, default: Date.now },
  data: [{ name: String, origin: String, destination: String }],
});
timeSeriesSchema.index({ timestamp: 1 }, { unique: true });
const TimeSeries = mongoose.model('TimeSeries', timeSeriesSchema);

const server = net.createServer((socket) => {
  console.log('Listener connected');

  socket.on('data', (data) => {
    console.log(data);
    const encryptedMessages = data.toString().split('|');
    const decryptedMessages = [];

    for (const encryptedMessage of encryptedMessages) {
      const decryptedMessage = decryptMessage(encryptedMessage, passphrase); // Implement your decryption function here

      if (decryptedMessage) {
        try {
          const {secret_key, ...rest} = JSON.parse(decryptedMessage);
          const secretKey = computeHash(JSON.stringify(rest));
          if (secret_key == secretKey) {
              decryptedMessages.push(rest);
          }
        } catch(err) {
          // console.error('Parse error: ' + err.message);
        }
      }
    }
    saveToMongo(decryptedMessages);
    console.log('Received: ' + encryptedMessages.length + ' Saved: ' + decryptedMessages.length);

    socket.emit('message-saved', decryptedMessages);
  });

  socket.on('end', () => {
    console.log('Listener disconnected');
  });
});

server.listen(3001, () => {
  console.log('Listener server is listening on port 3001');
});

// Placeholder for decryption function
function decryptMessage(encryptedMessage, passphrase) {
  try {
    const key = crypto.createHash('sha256').update(passphrase).digest();
    const iv = Buffer.from(encryptedMessage.slice(0, 32), 'hex'); // Extract IV from the encrypted message
    const encryptedData = Buffer.from(encryptedMessage.slice(32), 'hex'); // Extract encrypted data

    const decipher = crypto.createDecipheriv('aes-256-ctr', key, iv);
    
    const decryptedData = Buffer.concat([
      decipher.update(encryptedData),
      decipher.final()
    ]);

    return decryptedData.toString('utf8');
  } catch (error) {
    // console.error('Decryption error:', error.message);
    return null;
  }
}

async function findOneOrCreate(condition, doc) {
  const one = await TimeSeries.findOne(condition);
  return one || TimeSeries.create(doc);
}

async function getRecord(timestamp, doc) {
  const found = await findOneOrCreate({ timestamp: timestamp }, doc);
  return found;
}

// Save decrypted messages to MongoDB
async function saveToMongo(decryptedMessages) {
  const timestamp = new Date();
  timestamp.setSeconds(0,0);

  const timeSeriesData = new TimeSeries({
    timestamp,
    data: [],
  });

  const record = await getRecord(timestamp, timeSeriesData);
  // console.log(record._id);
  try {
    await TimeSeries.updateOne(
      { _id: record._id },
      {
        $push: {
          data: {
            $each: decryptedMessages
          },
        },
      }
    );
    // console.log('Saved to MongoDB');
  } catch (error) {
    // console.error('Error saving to MongoDB:', error.message);
  }
}

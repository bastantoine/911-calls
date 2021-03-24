const {
  MongoClient
} = require('mongodb');
const csv = require('csv-parser');
const fs = require('fs');
const {
  mainModule
} = require('process');

const MONGO_URL = 'mongodb://localhost:27017/';
const DB_NAME = '911-calls';
const COLLECTION_NAME = 'calls';

const insertCalls = async function (db, callback) {
  const collection = db.collection(COLLECTION_NAME);
  await dropCollectionIfExists(db, collection);

  const calls = [];
  fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {

      const titleSplited = data.title.split(': ');
      const timeStampSplited = data.timeStamp.split(' ');
      const dateSplited = timeStampSplited[0].split('-');
      const timeSplited = timeStampSplited[1].split(':');

      const call = {
        "latitude": data.lat,
        "longitude": data.lng,
        "location": {type: "Point", coordinates: [parseFloat(data.lng), parseFloat(data.lat)]},
        "description": data.desc,
        "postalCode": data.zip,
        "type": titleSplited[0],
        "title": titleSplited[1],
        "time": {
          "date": {
            "day": dateSplited[2],
            "month": dateSplited[1],
            "year": dateSplited[0]
          },
          "time": {
            "hour": timeSplited[0],
            "minutes": timeSplited[1],
            "seconds": timeSplited[2],
          }
        },
        "township": data.twp,
        "address": data.addr
      };

      calls.push(call);
    })
    .on('end', () => {
      collection.insertMany(calls, (err, result) => {
        callback(result)
      });
    });
}

MongoClient.connect(MONGO_URL, {
  useUnifiedTopology: true
}, (err, client) => {
  if (err) {
    console.error(err);
    throw err;
  }
  const db = client.db(DB_NAME);
  insertCalls(db, result => {
    console.log(`${result.insertedCount} calls inserted`);
    client.close();
  });
});

async function dropCollectionIfExists(db, collection) {
  const matchingCollections = await db.listCollections({name: COLLECTION_NAME}).toArray();
  if (matchingCollections.length > 0) {
    await collection.drop();
  }
}

const dotenv = require('dotenv');
dotenv.config()

const fs = require('fs');
const objectSize = require('object-sizeof');
const radash = require('radash');
const JsonlParser = require('stream-json/jsonl/Parser');
const {MongoClient, ServerApiVersion} = require("mongodb");
const parser = new JsonlParser();

const PATH_7MIL = '../dataset/7m-yelp-reviews.json';
const COLLECTION_NAME = '7mil-collection';
const DB_NAME = 'performance7m';

const BYTE_IN_MB = 0.00000095367432;

(async () => {
    let arrayToInsert = [];
    let objectCounter = 0;
    let started = Date.now()
    const col = await getCollection();

    const pipeline = fs.createReadStream(PATH_7MIL).pipe(parser);

    pipeline.on('data', async data => {
        objectCounter++;
        arrayToInsert.push(data.value)

        if (objectCounter % 100_000 === 0) {
            pipeline.pause()
            console.log('arrayToInsert size -', objectSize(arrayToInsert) * BYTE_IN_MB, 'mb');
            console.time(`Inserting time - ${objectCounter}`);
            await col.insertMany(arrayToInsert);
            console.timeEnd(`Inserting time - ${objectCounter}`);
            arrayToInsert = []

            console.log('--------------\n');
            await radash.sleep(100);
            pipeline.resume()
        }
    });

    pipeline.on('end', async () => {
        console.log('Operation took - ', (Date.now() - started) * 0.001, ' seconds\n');
        process.exit()
    });
})()


async function getCollection() {
    const client = new MongoClient(process.env.MONGO_CLUSTER_M30, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        serverApi: ServerApiVersion.v1
    });
    await client.connect();

    console.log('Connected successfully to server');
    const db = client.db(DB_NAME);
    return db.collection(COLLECTION_NAME)
}
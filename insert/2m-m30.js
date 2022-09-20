const dotenv = require('dotenv');
dotenv.config()
const fs = require('fs');
const objectSize = require('object-sizeof');

const PATH_7MIL = '../dataset/7m-yelp-reviews.json';
const COLLECTION_NAME = '4mil-collection';
const DB_NAME = 'performance';

const JsonlParser = require('stream-json/jsonl/Parser');
const {MongoClient, ServerApiVersion} = require("mongodb");

const jsonlParser = new JsonlParser();

const pipeline = fs.createReadStream(PATH_7MIL).pipe(jsonlParser);

const BYTE_IN_MB = 0.00000095367432;

let arrayToInsert = [];
let objectCounter = 0;
const started = Date.now()

pipeline.on('data', async data => {
    objectCounter++;
    arrayToInsert.push(data.value)

    if (objectCounter % 100_000 === 0) {
        console.time('Reading chunk');
        console.log(objectCounter, ' ', objectSize(arrayToInsert) * BYTE_IN_MB, 'mb');
        console.timeEnd('Reading chunk');
        console.log('\n');
    }

    if (objectCounter % 2_000_000 === 0) {
        pipeline.pause()
        console.log('Read took - ', (Date.now() - started) * 0.001, ' seconds\n');
        const col = await getCollection();

        console.log('Inserting...\n');
        console.time('Inserting time');
        await col.insertMany(arrayToInsert);
        console.timeEnd('Inserting time');
        process.exit()
    }
});

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
const dotenv = require('dotenv');
dotenv.config()

const fs = require('fs');
const {MongoClient, ServerApiVersion} = require("mongodb");
const JsonlParser = require('stream-json/jsonl/Parser');
const parser = new JsonlParser();
const objectSize = require('object-sizeof');
const radash = require('radash');
const lodash = require('lodash');

const PATH_7MIL = '../dataset/7m-yelp-reviews.json';
const COLLECTION_NAME = '7mil-collection-parallel';
const DB_NAME = 'performance7m';

const CHUNK = 200_000;
const PARALLEL_EXECUTIONS = 5;
const PARALLEL_EXECUTION_CHUNK = 20_000;

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

        if (arrayToInsert.length % CHUNK === 0) {
            console.log('arrayToInsert size -', objectSize(arrayToInsert) * BYTE_IN_MB, 'mb\n');
            pipeline.pause()
            const chunks = lodash.chunk(arrayToInsert, PARALLEL_EXECUTION_CHUNK);
            await radash.parallel(PARALLEL_EXECUTIONS, chunks, async (chunk) => {
                const now = Date.now()
                const stats = `size ${(objectSize(chunk) * BYTE_IN_MB).toFixed(3)} mb, records: ${chunk.length}`
                console.time(`id: ${now} - stats: ${stats} - took: `);
                await col.insertMany(chunk);
                console.timeEnd(`id: ${now} - stats: ${stats} - took: `);
            })
            console.log('--------------\n');
            arrayToInsert = []
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
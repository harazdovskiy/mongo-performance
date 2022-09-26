const dotenv = require('dotenv');
dotenv.config()
const {promises: fs} = require('fs');
const path = require('path');
const {MongoClient, ServerApiVersion} = require('mongodb');

const DB_NAME = 'performance-1m';
const COLLECTION_NAME = '1m-collection';

// const PATH_1MIL = path.resolve('../dataset/1m-generated.json');
const PATH_1MIL = path.resolve('../dataset/7m-yelp-reviews.json');

(async () => {
    try {
        console.time('Reading json')
        const records = JSON.parse(await fs.readFile(PATH_1MIL));
        console.timeEnd('Reading json')

        // const col = await getCollection(process.env.MONGO_CLUSTER_SHARED);
        const col = await getCollection(process.env.MONGO_CLUSTER_M30);

        console.log('Started insertion process successfully to server');
        console.time('Inserting records')
        await col.insertMany(records);
        console.timeEnd('Inserting records')
        process.exit();
    } catch (e) {
        console.error(e);
    }
})()

async function getCollection(url) {
    const client = new MongoClient(url, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        serverApi: ServerApiVersion.v1
    });
    await client.connect();

    console.log('Connected successfully to server');
    const db = client.db(DB_NAME);
    return db.collection(COLLECTION_NAME)
}
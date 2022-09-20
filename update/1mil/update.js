const dotenv = require('dotenv');
dotenv.config()
const {MongoClient, ServerApiVersion} = require('mongodb');

const DB_NAME = 'performance1m';
const COLLECTION_NAME = 'one-million';

(async () => {
    try {

        const col = await getCollection();

        console.time('Requesting 1mil data');
        const res = await col.updateMany({}, {$set: {addedNumber: 1}});
        console.timeEnd('Requesting 1mil data');

        console.log(res)

        process.exit();
    } catch (e) {
        console.error(e);
        client.close()
    }
})();

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
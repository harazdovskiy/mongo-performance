const dotenv = require('dotenv');
dotenv.config()
const {MongoClient, ServerApiVersion} = require('mongodb');

const DB_NAME = 'performance63m';
const COLLECTION_NAME = '63mil-collection';

(async () => {
    try {

        const col = await getCollection();

        console.time('Updating 1,5mil data');
        const res = await col.updateMany({language: 'polish'}, {$unset: {isRussian: true}});
        console.timeEnd('Updating 1,5mil data');

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
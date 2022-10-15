const dotenv = require('dotenv');
dotenv.config()
const {MongoClient, ServerApiVersion} = require('mongodb');

const DB_NAME = 'performance1m';
const COLLECTION_NAME = 'one-million';

(async () => {
    try {

        const col = await getCollection();

        console.log('Requesting 1m data....');

        console.time('Requesting 1m data');
        const toBeUpdated = await col.find({}).toArray();
        console.timeEnd('Requesting 1m data');

        console.log('Processing 1m data....');
        console.time('Processing 1m data');
        const operations = toBeUpdated.map((record) => {
            return {
                updateOne: {
                    filter: {_id: record._id},
                    update: {
                        $set: {
                            'newVal': record.name + record.number
                        }
                    }
                }
            }
        })
        console.timeEnd('Processing 1m data');

        console.log('Updating 1m data....');
        console.time('Updating 1m data');
        // https://www.mongodb.com/docs/manual/reference/method/db.collection.bulkWrite/#updateone-and-updatemany
        const res = await col.bulkWrite(operations);
        console.timeEnd('Updating 1m data');

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
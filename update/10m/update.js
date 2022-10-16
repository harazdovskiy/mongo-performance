const dotenv = require('dotenv');
dotenv.config()
const {MongoClient, ServerApiVersion} = require('mongodb');

const DB_NAME = 'performance63m';
const COLLECTION_NAME = '63mil-collection';
const CHUNK = 5;

(async () => {
    try {

        const col = await getCollection();
        const query = {language: 'schinese', popularity: {$exists: false}};
        console.time('Cursor');
        const cursor = col
            .find(
                query,
                {votes_up: 1, votes_funny: 1, comment_count: 1}
            )
        console.timeEnd('Cursor');

        const totalDocs = await col.countDocuments(query);

        console.log({totalDocs})
        let objectCounter = 0;
        let operations = [];
        const cursorStream = cursor.stream();
        cursorStream.on("data", async doc => {
            objectCounter++;
            operations.push(getBulkOperations(doc))
            if (operations.length % CHUNK === 0) {
                cursorStream.pause();
                console.log(operations.length)
                await col.bulkWrite(operations);
                operations = []
                console.log('objectCounter: ', objectCounter)
                cursorStream.resume()
            }
        }).on('end', () => {
            process.exit();
        }).on('error', (e) => {
            console.error(e)
            process.exit(1);
        });
    } catch (e) {
        console.error(e);
    }
})();

function getBulkOperations(record) {
    return {
        updateOne: {
            filter: {_id: record._id},
            update: {
                $set: {
                    'popularity': record.votes_up + record.votes_funny + record.comment_count
                }
            }
        }
    }
}

async function getCollection() {
    const client = new MongoClient(process.env.MONGO_CLUSTER_SHARED, {
        // const client = new MongoClient(process.env.MONGO_CLUSTER_M30, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        serverApi: ServerApiVersion.v1
    });
    await client.connect();

    console.log('Connected successfully to server');
    const db = client.db(DB_NAME);
    return db.collection(COLLECTION_NAME)
}
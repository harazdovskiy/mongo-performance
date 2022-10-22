const dotenv = require('dotenv');
dotenv.config()
const {MongoClient, ServerApiVersion} = require('mongodb');

const DB_NAME = 'performance63m';
const COLLECTION_NAME = '63mil-collection';
const CHUNK = 20_000;

(async () => {
    try {
        console.time('Script took');

        const col = await getCollection();
        const query = {language: 'schinese'};
        console.time('Cursor');
        const cursor = col
            .find(
                query,
                {votes_up: 1, votes_funny: 1, comment_count: 1}
            )
        console.timeEnd('Cursor');

        console.log('Calculating cursor size');
        console.time('docs count');
        const totalDocs = await col.countDocuments(query);
        console.timeEnd('docs count');

        console.log({totalDocs})
        let objectCounter = 0;
        let operations = [];
        const cursorStream = cursor.stream();
        cursorStream.on("data", async doc => {
            objectCounter++;
            operations.push(getBulkOperations(doc))
            if (objectCounter % CHUNK === 0) {
                cursorStream.pause();
                await col.bulkWrite(operations);
                operations = []
                console.log('objectCounter: ', objectCounter)
                cursorStream.resume()
            }
        }).on('end', () => {
            console.timeEnd('Script took');
            process.exit();
        }).on('error', (e) => {
            console.timeEnd('Script took');
            console.error(e)
            process.exit(1);
        });
    } catch (e) {
        console.timeEnd('Script took');
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
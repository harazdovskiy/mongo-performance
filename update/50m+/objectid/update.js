const dotenv = require('dotenv');
dotenv.config()
const {MongoClient, ServerApiVersion} = require('mongodb');
const {chunk} = require("lodash");
const {from = 0, to = 100_000} = require('minimist')(process.argv.slice(2));

const DB_NAME = 'performance63m';
const COLLECTION_NAME = '63mil-collection';
const radash = require('radash');
const READ_CHUNK = 10_000;
const UPDATE_CHUNK_SIZE = 2000;

(async () => {
    try {
        console.time('Script took');

        const col = await getCollection();
        const projection = {
            votes_up: 1,
            votes_funny: 1,
            comment_count: 1
        };
        console.time('First docs');

        const firstChunk = await col.find({}, projection).skip(from).limit(READ_CHUNK).toArray();
        console.timeEnd('First docs');

        let toProcess = to - from;
        let totalOperations = firstChunk.length;

        console.log('Started reading....')
        const operations = firstChunk.map(getBulkOperations);
        await write(col, operations);

        const lastElementsId = getNextId(firstChunk);
        await processBatch(lastElementsId)

        async function processBatch(nextId) {
            console.log('------------------------------------')
            console.log({totalOperations}, nextId.toString())
            console.time("Get took")
            const docs = await col.find({_id: {$gt: nextId}}, projection).limit(READ_CHUNK).toArray();
            console.timeEnd("Get took")
            const newNextId = getNextId(docs);
            totalOperations += docs.length;
            console.log(`Done ${(totalOperations * 100) / toProcess} %`)

            if (!newNextId || (totalOperations + from) >= to) {
                console.log('last Object Id', newNextId, totalOperations >= to)
                await write(col, docs.map(getBulkOperations));
                return Promise.resolve()
            }
            await write(col, docs.map(getBulkOperations));

            return processBatch(newNextId)
        }

        console.timeEnd('Script took');
        process.exit()
    } catch (e) {
        console.timeEnd('Script took');
        console.error(e);
        process.exit(1)
    }
})();

function getNextId(docs) {
    if (!docs.length) {
        return null
    }
    const nextId = docs[docs.length - 1]._id;
    return nextId || null;
}

async function write(col, newOperations) {
    console.log('writing: ', newOperations.length)
    const bulkChunks = chunk(newOperations, UPDATE_CHUNK_SIZE)
    console.time('Parallel update took');
    await radash.parallel(5, bulkChunks, (operations) => col.bulkWrite(operations))
    console.timeEnd('Parallel update took');
    return Promise.resolve();
}

function getBulkOperations(record) {
    return {
        updateOne: {
            filter: {_id: record._id},
            update: {
                $set: {
                    'calc_popularity_id': record.votes_up + record.votes_funny + record.comment_count
                }
            }
        }
    }
}

async function getCollection() {
    const client = new MongoClient(process.env.MONGO_CLUSTER_M30, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        serverApi: ServerApiVersion.v1,
        readPreference: 'secondary'
    });
    await client.connect();

    console.log('Connected successfully to server');
    const db = client.db(DB_NAME);
    return db.collection(COLLECTION_NAME)
}
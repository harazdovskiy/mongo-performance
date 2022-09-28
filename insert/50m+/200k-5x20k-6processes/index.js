const dotenv = require('dotenv');
dotenv.config()

const fs = require('fs');
const {from = 1, to = 20} = require('minimist')(process.argv.slice(2));
const radash = require('radash');
const {MongoClient, ServerApiVersion} = require("mongodb");
const lodash = require("lodash");
const objectSize = require("object-sizeof");

const COLLECTION_NAME = '63mil-collection';
const DB_NAME = 'performance63m';

const BYTE_IN_MB = 0.00000095367432;
const BASE_PATH = '../dataset/reviews';
const READING_CHUNK = 200_000;
const PARALLEL_INSERTIONS = 5;
const PARALLEL_FILES_READ = 5;
const PARALLEL_EXECUTION_CHUNK = 20_000;
const LOG_PATH_EVERY = 1_000_000;

(async () => {
    const col = await getCollection()
    let started = Date.now()
    const dirsPaths = await fs.promises.readdir(BASE_PATH, {});

    const workingPaths = dirsPaths
        .slice(+from, +to)
        .filter(path => !path.startsWith('.'))

    let doneReadingReviews = 0;
    let reviews = [];
    const workingPathsLength = workingPaths.length;
    console.log('\n All paths: ', workingPathsLength)
    let donePaths = 0;
    let moreThan = LOG_PATH_EVERY;
    for (const path of workingPaths) {

        const paginationDirPath = `${BASE_PATH}/${path}`;
        const innerPaths = await fs.promises.readdir(paginationDirPath);

        await radash.parallel(PARALLEL_FILES_READ, innerPaths, async (path) => {
            const filePath = `${paginationDirPath}/${path}`;
            const reviewsBuffer = await fs.promises.readFile(filePath);
            const reviewsRead = JSON.parse(Buffer.from(reviewsBuffer).toString()).reviews || [];
            reviews.push(...reviewsRead);
            doneReadingReviews += reviewsRead.length;

            if (doneReadingReviews > moreThan) {
                console.log(`done paths - ${donePaths} done reviews ${doneReadingReviews}`)
                moreThan += LOG_PATH_EVERY
            }
        })

        if (reviews.length >= READING_CHUNK) {
            await insertReviews(col, reviews);
            reviews = []
        }

        donePaths++

        if (donePaths === workingPathsLength) {
            console.log('Last insert!')
            await insertReviews(col, reviews);
            reviews = []
        }
    }

    console.log('Done reading reviews: ', doneReadingReviews)
    console.log('Script: took - ', (Date.now() - started) * 0.001, ' seconds\n');
    process.exit()
})()

async function getCollection() {
    const client = new MongoClient(process.env.MONGO_CLUSTER_SHARED, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        serverApi: ServerApiVersion.v1
    });
    await client.connect();

    console.log('Connected successfully to server');
    const db = client.db(DB_NAME);
    return db.collection(COLLECTION_NAME)
}

async function insertReviews(col, reviews) {
    console.log(`Started insertReview() count: ${reviews.length}`)
    console.time('Insert took: ')
    const chunks = lodash.chunk(reviews, PARALLEL_EXECUTION_CHUNK);
    await radash.parallel(PARALLEL_INSERTIONS, chunks, async (chunk) => {
        const now = Date.now()
        const stats = `size ${(objectSize(chunk) * BYTE_IN_MB).toFixed(3)} mb, records: ${chunk.length}`
        console.time(`id: ${now} - stats: ${stats} - took: `);
        await col.insertMany(chunk);
        console.timeEnd(`id: ${now} - stats: ${stats} - took: `);
    })
    console.timeEnd('Insert took: ')
    console.log('\n');
}
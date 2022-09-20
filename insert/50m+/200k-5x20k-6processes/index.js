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

(async () => {
    const col = await getCollection()
    let started = Date.now()
    const dirsPaths = await fs.promises.readdir(BASE_PATH, {});

    const workingPaths = dirsPaths
        .sort((a, b) => a - b)
        .slice(+from, +to)
        .filter(path => !path.startsWith('.'))

    let doneReadingReviews = 0;
    let doneInsertingReviews = 0;
    let reviews = [];
    console.log({workingPaths})
    let donePaths = workingPaths.length;
    for (const path of workingPaths) {


        const paginationDirPath = `${BASE_PATH}/${path}`;
        console.log(`reading ${paginationDirPath}  donePaths - ${donePaths} done reviews ${doneReadingReviews}`)

        console.log('reviews.length', reviews.length, '\n')
        if (reviews.length >= 200_000) {
            await insertReview(col, reviews);
            doneInsertingReviews += reviews.length;
            reviews = []
        }

        const innerPaths = await fs.promises.readdir(paginationDirPath);
        // console.log('innerPaths ', innerPaths)
        await radash.parallel(5, innerPaths, async (path) => {
            const filePath = `${paginationDirPath}/${path}`;
            // console.log('inp', filePath)
            const chunk = await fs.promises.readFile(filePath);
            const reviewsRead = JSON.parse(Buffer.from(chunk).toString()).reviews || [];
            reviews.push(...reviewsRead);
            doneReadingReviews += reviewsRead.length;
        })

        if(!donePaths) {
            console.log('Last insert!')
            await insertReview(col, reviews);
            doneInsertingReviews += reviews.length;
            reviews = []
        }
    }


    console.log('Reviews', doneReadingReviews)

    console.log('Operation took - ', (Date.now() - started) * 0.001, ' seconds\n');
    process.exit()
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

async function insertReview(col, reviews) {
    const chunks = lodash.chunk(reviews, 20_000);
    await radash.parallel(5, chunks, async (chunk) => {
        const now = Date.now()
        console.log(`id: ${now}:  chunks size -`, objectSize(chunk) * BYTE_IN_MB, 'mb');
        console.time(`id: ${now}:  Inserting time`);
        await col.insertMany(chunk);
        console.timeEnd(`id: ${now}:  Inserting time`);
    })
}
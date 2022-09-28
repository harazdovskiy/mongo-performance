const fs = require('fs');
const {from = 0, to = 1} = require('minimist')(process.argv.slice(2));
const radash = require('radash');

const BASE_PATH = '../dataset/reviews';
const PARALLEL_EXECUTIONS = 5;
const LOG_PATH_EVERY = 1_000_000;

(async () => {
    let started = Date.now()
    const dirsPaths = await fs.promises.readdir(BASE_PATH, {});

    const workingPaths = dirsPaths.slice(+from, +to).filter(path => !path.startsWith('.'))
    let doneReadingReviews = 0;
    console.log('\n All paths', workingPaths.length)
    let donePaths = 0;
    let moreThan = LOG_PATH_EVERY;
    for (const path of workingPaths) {

        const paginationDirPath = `${BASE_PATH}/${path}`;
        const innerPaths = await fs.promises.readdir(paginationDirPath);

        await radash.parallel(PARALLEL_EXECUTIONS, innerPaths, async (path) => {
            const filePath = `${paginationDirPath}/${path}`;

            const chunk = await fs.promises.readFile(filePath);
            const reviews = JSON.parse(Buffer.from(chunk).toString()).reviews || [];
            doneReadingReviews += reviews.length;

            if (doneReadingReviews > moreThan) {
                console.log(`done paths - ${donePaths} done reviews ${doneReadingReviews}`)
                moreThan += LOG_PATH_EVERY
            }
        })

        donePaths++;
    }
    console.log('Reviews', doneReadingReviews)
    console.log('Operation took - ', (Date.now() - started) * 0.001, ' seconds\n');
})()
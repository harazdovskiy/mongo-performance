const fs = require('fs');
const {from = 0, to = 1} = require('minimist')(process.argv.slice(2));
const radash = require('radash');

const BASE_PATH = '../dataset/reviews';

(async () => {
    let started = Date.now()
    const dirsPaths = await fs.promises.readdir(BASE_PATH, {});

    const workingPaths = dirsPaths.slice(+from, +to).filter(path => !path.startsWith('.'))
    let doneReadingReviews = 0;
    console.log({workingPaths})
    let donePaths = workingPaths.length;
    for (const path of workingPaths) {


        const paginationDirPath = `${BASE_PATH}/${path}`;
        console.log(`reading ${paginationDirPath}  donePaths - ${donePaths} done reviews ${doneReadingReviews}`)


        const innerPaths = await fs.promises.readdir(paginationDirPath);
        // console.log('innerPaths ', innerPaths)
        await radash.parallel(5, innerPaths, async (path) => {
            const filePath = `${paginationDirPath}/${path}`;
            // console.log('inp', filePath)
            const chunk = await fs.promises.readFile(filePath);
            const reviews = JSON.parse(new Buffer(chunk).toString()).reviews || [];
            doneReadingReviews += reviews.length;
        })

        donePaths--;
    }


    console.log('Reviews', doneReadingReviews)

    console.log('Operation took - ', (Date.now() - started) * 0.001, ' seconds\n');
})()
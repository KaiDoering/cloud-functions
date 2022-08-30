const functions = require('@google-cloud/functions-framework');
const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');

const storage = new Storage();
const bigqueryClient = new BigQuery();

functions.cloudEvent('upload', cloudEvent => {
    const file = cloudEvent.data;
    if (file.name.match(/^.+\/mutation\.json$/)) {
        async function downloadIntoMemory() {
            const contents = await storage.bucket(file.bucket).file(file.name).download();

            const content = JSON.parse(contents.toString());
            const mapped = { files: Object.keys(content.files).map(fileName => ({ file_name: fileName, ...content.files[fileName] })) };
            console.log(mapped);
            return mapped;
        }

        async function insertRowsAsStream(mappedReport) {
            await bigqueryClient
                .dataset("mutation_tests")
                .table("stryker")
                .insert(mappedReport);
        }
        
        downloadIntoMemory().then(insertRowsAsStream).catch(console.error);
    }
});

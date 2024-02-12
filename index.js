const { BigQuery } = require('@google-cloud/bigquery');
const bq = new BigQuery();
const datasetId = 'weather_etl';
const tableId = 'Weather_Table';

const { Storage } = require('@google-cloud/storage');
const csv = require('csv-parser');

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
        .on('error', (error) => {
            console.error(error);
        })
        .pipe(csv())
        .on('data', (row) => {
            printDict(row);
            writeToBq(row);
        })
        .on('end', () => {
            console.log('End!');
        });
};

// HELPER FUNCTIONS

function printDict(row) {
    for (let key in row) {
        console.log(`${key}: ${row[key]}`);
    }
}

async function writeToBq(rows) {
    if (!Array.isArray(rows)) {
        rows = [rows];
    }

    // Transform numeric fields and replace missing values
    rows.forEach((row) => {
        // Replace "-9999" with null for all numeric fields
        for (let key in row) {
            if (!isNaN(row[key])) {
                let value = parseFloat(row[key]);
                if (value === -9999) {
                    row[key] = null;
                }
            }
        }

        // Set station identifier code for all rows
        row['station'] = '724380-93819';

        // Convert numeric fields
        for (let key in row) {
            if (!isNaN(row[key])) {
                if (key === 'airtemp' || key === 'dewpoint' || key === 'pressure' || key === 'windspeed' || key === 'precip1hour' || key === 'precip6hour') {
                    // Convert numeric fields to decimal by dividing by 10
                    row[key] = parseFloat(row[key]) / 10;
                    if (isNaN(row[key])) {
                        row[key] = null; // If conversion fails, set to null
                    }
                } else {
                    // Convert other numeric fields to integers
                    row[key] = parseInt(row[key], 10);
                }
            }
        }
    });

    rows.forEach((element) => console.log(element));
    try {
        await bq.dataset(datasetId).table(tableId).insert(rows);
        rows.forEach((row) => {
            console.log(`${row}`);
        });
    } catch (err) {
        console.error(`ERROR: ${err}`);
    }
}




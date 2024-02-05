# Weather Data ETL Cloud Function

This Cloud Function will respond to a new document in a GCP Storage bucket and will extract the CSV data, transform it, and load it into a BigQuery table.

## v1: Trigger the function when a file is loaded to GCS

1. Create a Cloud Storage bucket (in the example below, my bucket is `sp24_elliott_41200_weather_dev`)
2. Create a new directory in Cloud Shell
3. Initialize a new npm module
4. `touch index.js .gitignore README.md`
5. Add `node_modules` to .gitignore
6. Add code to log the basic data about the file on function execution
7. Deploy the function:

```
gcloud functions deploy weather_etl \
--runtime nodejs18 \
--trigger-event google.storage.object.finalize \
--entry-point readObservation \
--trigger-resource sp24_danish_41200_weather_dev
```

### Copying a File to the Cloud Storage Bucket

This repository includes a CSV file that you can use to test your Cloud Function. To copy the file to the Cloud Storage bucket, run the following command **and substitute your bucket name!**

```
gsutil cp 724380-93819_sample.csv gs://sp24_elliott_41200_weather_dev
```

## v2: Read the contents of the file when it is loaded to GCS

1. Install `@google-cloud/storage` and `csv-parser`
2. Use `file.createReadStream()` to open and log the data in the file

## v3: Print each element of the data to the console separately

1. Add a helper function that accepts the dictionary of CSV data and loops through it, logging out each row of data as a separate log entry

## v4: Transform the data

1. Add a helper function that converts the dictionary values to numeric
2. Modify helper function to transform values (fix the scales, set null values to `null`, etc.)

## v5: Write the transformed data to BigQuery

1. Write an insert query to load the transformed data into a table
2. Use the file name to identify the weather station providing the data
# Spotify Data Transformation and Analysis

## Project Overview

This project involves the ingestion, transformation, and analysis of Spotify sample datasets from Kaggle. The task was completed using a Node.js script to clean up and organize these datasets and SQL for data analysis.

## Requirements

### Data Source

- Spotify sample datasets:
  - [Artists Dataset](https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=artists.csv)
  - [Tracks Dataset](https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=tracks.csv)

### Data Transformation

- A Node.js script (using Typescript) was implemented to ingest the data from the source and transform it as follows:
  - Filter out records that meet specific criteria:
    1. Ignore the tracks that have no name.
    2. Ignore the tracks shorter than 1 minute.
    3. Load only these artists that have tracks after the filtering above.
  - Format and structure the data as needed for analysis:
    1. Explode track release date into separate columns: year, month, day.
    2. Transform track danceability into string values based on these intervals:
       - [0; 0.5) assign ‘Low’
       - [0.5; 0.6] assign ‘Medium’
       - (0.6; 1] assign ‘High’

### Data Storage

- The cleaned and transformed data was stored into AWS S3.
- Data was loaded from S3 into a locally hosted PostgreSQL.

### Data Processing

- 3 SQL views were created to perform the following tasks on the data stored:
  1. Return track id, name, popularity, energy, danceability (Low, Medium, High) and number of artist followers.
  2. Return artist id, name, track id, name. Take only these tracks, which artists have followers.
  3. Pick the most energizing track of each release year. Return release year, track id, name, and its energy.

## Deliverables

1. Node.js script(s) for data ingestion and transformation, using Typescript.
2. Unit tests for the Node.js based data transformation solution using Jest.
3. SQL script(s) for data storage and processing.
4. Instructions on how to run the solution, including any configuration settings.
5. All source code is available in the [GitHub repository](https://github.com/PovilasU/spotify-data-transformation).

## Instructions to Run the Solution

1. Clone the repository from GitHub:
   ```sh
   git clone https://github.com/PovilasU/spotify-data-transformation.git
   ```
2. Navigate to the project directory:
   ```sh
   cd spotify-data-transformation
   ```
3. Install the necessary dependencies:
   ```sh
   npm install
   ```
4. Rename `.env.txt` to `.env` and add your AWS and PostgreSQL keys:
   ```sh
   mv .env.txt .env
   ```
5. Configure AWS S3 and PostgreSQL settings in the `.env` file.
6. Set `LOCAL_TEST=false` in the `.env` file if you want to download files from S3. Set it to `true` to use local files from the `data` folder (useful for testing).
7. In `src/config.ts`, ensure `localTest` is set to `false` to download files from S3:
   ```typescript
   localTest: process.env.LOCAL_TEST === "false", // change to true if you don't want to load .csv files from AWS S3 but test with local files
   ```
8. Run the Node.js script to ingest and transform the data from the `src` folder:
   ```sh
   cd src
   npx ts-node .\index.ts
   ```
9. Upload the transformed files to Amazon S3:
   ```sh
   npx ts-node .\uploadToS3.ts
   ```
10. Load the data from S3 to the local PostgreSQL database:
    ```sh
    npx ts-node .\loadFromS3ToPostgres.ts
    ```
11. Inside PostgreSQL, run the `views.sql` script located in `sql/views.sql` to create the views.
12. Run the unit tests:
    ```sh
    npm run test
    ```

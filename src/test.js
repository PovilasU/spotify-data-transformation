const fs = require('fs');
const csv = require('csv-parser');
const minDuration = 60000; // 1 minute in milliseconds
let totalCount = 0;
const filePath = '../data/transformedTracks.csv';
//const filePath = '../data/tracks.csv';
let count = 0;

fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (row) => {
        const duration = parseFloat(row.duration_ms);
        if (duration < minDuration) {
            count++;
        }
    })
    .on('end', () => {
        console.log(`Number of tracks with less than one minute: ${count}`);
    });

    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', () => {
            totalCount++;
        })
        .on('end', () => {
            console.log(`Total number of tracks: ${totalCount}`);
        });
const fs = require('fs');
const JSONStream = require('JSONStream');
const zlib = require('zlib');
const util = require('util');
const execFile = util.promisify(require('child_process').execFile);
const outputFile = 'cloudtrail.csv';
const { RecordTransformStream, columns } = require('./record-transform');
const transformer = new RecordTransformStream();

initOutputFile(outputFile, columns);
generateCSV('data', outputFile);

function generateCSV(dir='data', outputFile) {
  execFile('find', [`${dir}/`], { maxBuffer: 1024*5000 })
    .then((response) => {
      const gzippedFilePaths = response.stdout
        .split('\n')
        .filter(path => path.includes('.json.gz'));
      
      gzippedFilePaths.forEach(gzippedFilePath => {
        const fileContents = fs.createReadStream(gzippedFilePath);

        fileContents
          .pipe(zlib.createGunzip())
          .pipe(JSONStream.parse('Records'))
          .pipe(transformer)
          .pipe(fs.createWriteStream(outputFile))
          .on('finish', () => console.log('Done'))
          .on('error', (e) => console.log('error'));
      });
    })
    .catch((e) => {
      console.error(e);
    });
}
/**
 * Clean out the output file and create it if it doesn't exist
 * @param {string} outputFile 
 */
function initOutputFile(outputFile, columns) {
  fs.truncate(outputFile, () => {});
  fs.writeFile(outputFile, columns.join(','), (err) => {
    if (err) throw err;
  });
}
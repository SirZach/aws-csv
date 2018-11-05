const { Transform } = require('stream');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
const columns = [
  'userIdentityType',
  'userAgent',
  'userIdentityARN',
  'userIdentityAccountId',
  'userName',
  'awsRegion',
  'eventSource',
  'eventName'
]

class RecordTransformStream extends Transform {
  constructor() {
      super({ objectMode: true });

      this._firstRecord = true;
      this._numberOfFiles = 0;
      this._csvStringifier = createCsvStringifier({
        header: columns.map(c => {
          return { id: c, title: c };
        })
      });
  }

  format(record) {
    return {
      userIdentityType: record.userIdentity.type,
      userAgent: record.userAgent,
      userIdentityARN: record.userIdentity.arn,
      userIdentityAccountId: record.userIdentity.accountId,
      userName: this.getUserName(record),
      awsRegion: record.awsRegion,
      eventSource: record.eventSource,
      eventName: record.eventName
    };
  }

  getUserName(record) {
    return record.userIdentity.sessionContext && record.userIdentity.sessionContext.sessionIssuer ? 
      record.userIdentity.sessionContext.sessionIssuer.userName : 
      '';
  }

  _transform(records, encoding, callback) {
    this._numberOfFiles++;
    console.log(this._numberOfFiles);
    const recordLine = this._csvStringifier.stringifyRecords(records.map(this.format.bind(this)));
    if (this._firstRecord) {
        this._firstRecord = false;
        callback(null, this._csvStringifier.getHeaderString() + recordLine);
    } else {
        callback(null, recordLine);
    }
  }
}

module.exports = {
  RecordTransformStream,
  columns
};

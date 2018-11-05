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
      this._csvStringifier = createCsvStringifier({
        header: columns.map(c => {
          return { id: c, title: c };
        })
      });
  }

  /**
   * Create the CSV line object from the record in the log file
   * @param {dict} record 
   */
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

  /**
   * User name seems to be flaky...
   * @param {dict} record 
   */
  getUserName(record) {
    return record.userIdentity.sessionContext && record.userIdentity.sessionContext.sessionIssuer ? 
      record.userIdentity.sessionContext.sessionIssuer.userName : 
      '';
  }

  /**
   * Required function to implement for a Transform stream
   * @param {[dict]} records 
   * @param {*} encoding 
   * @param {fn} callback 
   */
  _transform(records, encoding, callback) {
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

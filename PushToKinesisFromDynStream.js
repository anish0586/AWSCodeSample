const AWS = require('aws-sdk');
const kinesis = new AWS.Kinesis({region: 'us-west-2'});

exports.handler = async (event) => {
    const records = event.Records.map((record) => {
            const eventId = record.eventID;
            const eventName = record.eventName;
            const updateDate = (new Date(record.dynamodb.ApproximateCreationDateTime * 1000)).toISOString();
            const oldImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);
            const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
            const studentId = newImage.StudentId;
            const subjectId = newImage.SubjectId;
            const studentName = newImage.Name;
            const studentState = newImage.State;
            
            //create the object required to be added to kinesis.
            let returnData = {
                eventId,
                eventName,
                studentId,
                subjectId,
                studentName,
                studentState,
                updateDate
            }
            return returnData;
    });
    
    const kinesisPutRecordsParams = {
            StreamName: 'kinesisStudentStream',
            Records: records.map(record => {
                return {
                    PartitionKey: (record.studentId || record.event_id).toString(),
                    Data: JSON.stringify(record)
                };
            })
        };

        return kinesis.putRecords(kinesisPutRecordsParams).promise().then((data) => {
            console.log('Data pushed to kinesis');
            return 'SUCCESS';
        }).catch(function(error) {
            console.log('Error while pushing data to kinesis');
            throw error;
        });
};

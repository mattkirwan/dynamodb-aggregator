# DynamoDB Aggregator

A simple utility module for aggregating dynamodb fields. Currently only aggregates per hour.

## Install

`npm i --save @mattkirwan/dynamodb-aggregator`

## Usage

`const aggregate = require("@mattkirwan/dynamodb-aggregator")`

There is currently only one exported function:

## Functions

### hourly()
---
### Params

**documentClient** - AWS.DynamoDB.DocumentClient - An instance of the DocumentClient from `aws-sdk`

**metaData** - Object - An object defining the DynamoDB table/keys you would like to operate against. Example below.

**data** - Object - An object containing the timestamped events with which you would like to aggregate. Example below.

##### Examples

**metaData**

```
let metaData = {
    TableName: "aggregate_dev",
    PartitionKeyName: "campaign_id",
    SortKeyName: "timestamp_3600",
}
```

**data**

```
// { PartitionKeyX: { AggregateFieldNameX: [ EventTimestamp, EventTimestamp ] } }
let data = {
    // Partition Key
    "1234campaign5678": {
        // Table Field Name
        "total_show": [
            // Events (timestamps in ms)
            1562157215000,
            1562158775000,
            1562162375000,
        ]
    },
    "5678campaign1234": {
        "total_show": [
            1562157215000,
            1562158775000,
            1562162375000,
        ],
        "total_action": [
            1562157215000,
            1562158775000,
            1562162375000,
            1562162375000,
        ]            
    }
}
```

The above inputs would store the following in DynamoDB:

![A screenshot of how dynamodb-aggregator would store the example input](https://raw.githubusercontent.com/mattkirwan/dynamodb-aggregator/master/DynamoDB-ExampleOutput.png)

### Use

Returns a Promise.all containing all of the update query responses from the DynamoDB request.

```
return await aggregate.hourly(documentClient, metaData, data)
    .then(r => {
        console.log(r)
        return
    })
    .catch(e => {
        console.log(e)
        return
    })
```

Successful response:
```
{  
    Attributes: {
        total_show: 2,
        campaign_id: '5678campaign1234',
        timestamp_3600: 1562155200,
        total_action: 1
    },
    ConsumedCapacity: {
        TableName: 'aggregate_dev',
        CapacityUnits: 1
    }
}
```

### Tests

It's got a decent amount of unit tests. Run using `npm run test`.
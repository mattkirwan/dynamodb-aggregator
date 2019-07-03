const rewire = require("rewire")
const aggregate = rewire("../aggregate")

const chai = require("chai")
const expect = chai.expect
const chaiAsPromised = require("chai-as-promised")
chai.use(chaiAsPromised)
chai.should()

const sinon = require("sinon")

describe("Module", () => {
    it("should be a function", done => {
        aggregate.hourly.should.be.a("function")
        done()
    })
})

let update
let AWS

let metaData = {
    TableName: "SomeTable",
    PartitionKeyName: "ddb_partition_key_name",
    SortKeyName: "ddb_sort_key_name",
}

let data = {
    "someUniquePartitionKey": {
        "anAggregateFieldName": [
            1562052275000, // 1562050800
            1562052272000, // 1562050800
        ],
        "anotherAggregateFieldName": [
            1562052095000, // 1562050800
            1562048555000, // 1562047200
        ],
    },
    "anotherUniquePartitionKey": {
        "yetAnotherAggregateFieldName": [
            1562060443000, // 1562058000
            1562053243000, // 1562050800
        ],
    },
    "yetAnotherUniquePartitionKey": {
        "yetAnotherAggregateFieldName": [
            1562072557000, // 1562072400
        ],
    },        
}

describe("#aggregateHourly()", () => {

    beforeEach(() => {
        update = () => {
            return {
                promise: () => {}
            }
        }
        
        AWS = {
            DynamoDB: {
                DocumentClient: {
                    update: sinon.spy(update)
                }
            }
        }
    })

    afterEach(() => {
        AWS = {}
    });

    it("should reject if passed an empty meta object", () => {
        return aggregate.hourly(AWS.DynamoDB.DocumentClient, {}, {}).should.be.rejectedWith("No query metadata passed")
    })

    it("should reject if passed a metadata object with a missing required key", () => {
        return aggregate.hourly(AWS.DynamoDB.DocumentClient, {MissingTableNameKey: "SomeValue"}, {}).should.be.rejectedWith("Invalid metadata. Required keys missing")
    })

    it("should resolve if passed an empty data object", () => {
        return aggregate.hourly(AWS.DynamoDB.DocumentClient, metaData, {}).should.be.fulfilled
    })

    it("should call #formatDataHourly() once", () => {
        let spy = sinon.spy(aggregate.__get__("formatDataHourly"))
        aggregate.__set__("formatDataHourly", spy)

        return aggregate.hourly(AWS.DynamoDB.DocumentClient, metaData, {not: {real: []}}).then(p => {
            expect(spy.callCount).to.equal(1)
        })
    })

    it("should call #createUpdateQuery() for every partition in every key in the formatted data", () => {
        let formattedData = {
            "1562047200": {
                someUniquePartitionKey: {
                    anotherAggregateFieldName: 1
                }
            },
            "1562050800": {
                someUniquePartitionKey: { 
                    anAggregateFieldName: 1,
                    anotherAggregateFieldName: 1
                },
                anotherUniquePartitionKey: {
                    yetAnotherAggregateFieldName: 1
                }
            },
            "1562058000": {
                anotherUniquePartitionKey: {
                    yetAnotherAggregateFieldName: 1
                }
            },
            "1562072400": {
                yetAnotherUniquePartitionKey: {
                    yetAnotherAggregateFieldName: 1
                }
            }
        }

        let spy = sinon.spy(aggregate.__get__("createUpdateQuery"))
        aggregate.__set__("createUpdateQuery", spy)

        return aggregate.hourly(AWS.DynamoDB.DocumentClient, metaData, data).then(p => {
            expect(spy.callCount).to.equal(5) // Number of unique time groups
        })
    })

    it("should call #promiseUpdateQuery() for every update query", () => {
        let spy = sinon.spy(aggregate.__get__("promiseUpdateQuery"))
        aggregate.__set__("promiseUpdateQuery", spy)

        return aggregate.hourly(AWS.DynamoDB.DocumentClient, metaData, data).then(p => {
            expect(spy.callCount).to.equal(5) // Number of unique queries
        })
    })

    it("should call update for every update query", () => {

        update = () => {
            return {
                promise: () => {}
            }
        }
        
        AWS = {
            DynamoDB: {
                DocumentClient: {
                    update: sinon.spy(update)
                }
            }
        }

        return aggregate.hourly(AWS.DynamoDB.DocumentClient, metaData, data).then(m => {
            expect(AWS.DynamoDB.DocumentClient.update.callCount).to.equal(5)
        })
    })

})

describe("#formatDataHourly()", () => {

    it("should correctly format the data with start of the hour keys", () => {
        const uut = aggregate.__get__("formatDataHourly")(data)
        expect(uut).to.have.property("1562050800")
        expect(uut).to.have.property("1562047200")
        expect(uut).to.have.property("1562058000")
        expect(uut).to.have.property("1562072400")
    })

    it("should correctly set all partition keys for all hour keys", () => {
        const uut = aggregate.__get__("formatDataHourly")(data)

        expect(uut["1562050800"]).to.have.property("someUniquePartitionKey")
        expect(uut["1562050800"]).to.have.property("anotherUniquePartitionKey")
        expect(uut["1562050800"]).to.not.have.property("yetAnotherUniquePartitionKey")

        expect(uut["1562047200"]).to.have.property("someUniquePartitionKey")
        expect(uut["1562047200"]).to.have.not.property("anotherUniquePartitionKey")
        expect(uut["1562047200"]).to.have.not.property("yetAnotherUniquePartitionKey")

        expect(uut["1562072400"]).to.have.property("yetAnotherUniquePartitionKey")
        expect(uut["1562072400"]).to.have.not.property("anotherUniquePartitionKey")
        expect(uut["1562072400"]).to.have.not.property("someUniquePartitionKey")

        expect(uut["1562058000"]).to.have.property("anotherUniquePartitionKey")
        expect(uut["1562072400"]).to.have.not.property("anotherUniquePartitionKey")
        expect(uut["1562072400"]).to.have.not.property("someUniquePartitionKey")        
    })

    it("should correctly aggregate the field total events", () => {
        const uut = aggregate.__get__("formatDataHourly")(data)

        expect(uut["1562050800"]["someUniquePartitionKey"]["anAggregateFieldName"]).to.equal(1)
        expect(uut["1562050800"]["someUniquePartitionKey"]["anotherAggregateFieldName"]).to.equal(1)
        expect(uut["1562050800"]["anotherUniquePartitionKey"]["yetAnotherAggregateFieldName"]).to.equal(1)

        expect(uut["1562047200"]["someUniquePartitionKey"]["anotherAggregateFieldName"]).to.equal(1)

        expect(uut["1562058000"]["anotherUniquePartitionKey"]["yetAnotherAggregateFieldName"]).to.equal(1)
        expect(uut["1562072400"]["yetAnotherUniquePartitionKey"]["yetAnotherAggregateFieldName"]).to.equal(1)
    })

})

describe("#createUpdateQuery()", () => {

    it("should create the correct query object", () => {

        metaData["PartitionKeyValue"] = "SomePartitionKey"
        metaData["SortKeyValue"] = "1562047200"

        let partitionedAggregateData = { 
            anAggregateFieldName: 1,
            anotherAggregateFieldName: 1
        }

        let uut = aggregate.__get__("createUpdateQuery")(metaData, partitionedAggregateData)

        expect(uut).to.be.an("object")
        expect(uut).to.have.property("TableName")
        expect(uut["TableName"]).to.be.equal("SomeTable")

        expect(uut).to.have.property("Key")
        expect(uut["Key"]).to.have.property("ddb_partition_key_name")
        expect(uut["Key"]).to.have.property("ddb_sort_key_name")
        expect(uut["Key"]["ddb_partition_key_name"]).to.be.equal("SomePartitionKey")
        expect(uut["Key"]["ddb_sort_key_name"]).to.be.equal("1562047200")

        expect(uut).to.have.property("UpdateExpression")
        expect(uut["UpdateExpression"]).to.be.equal("ADD #anAggregateFieldName :anAggregateFieldName, #anotherAggregateFieldName :anotherAggregateFieldName")        

        expect(uut).to.have.property("ExpressionAttributeNames")
        expect(uut["ExpressionAttributeNames"]).to.have.property("#anAggregateFieldName")
        expect(uut["ExpressionAttributeNames"]).to.have.property("#anotherAggregateFieldName")
        expect(uut["ExpressionAttributeNames"]["#anAggregateFieldName"]).to.be.equal("anAggregateFieldName")
        expect(uut["ExpressionAttributeNames"]["#anotherAggregateFieldName"]).to.be.equal("anotherAggregateFieldName")

        expect(uut).to.have.property("ExpressionAttributeValues")
        expect(uut["ExpressionAttributeValues"]).to.have.property(":anAggregateFieldName")
        expect(uut["ExpressionAttributeValues"]).to.have.property(":anotherAggregateFieldName")
        expect(uut["ExpressionAttributeValues"][":anAggregateFieldName"]).to.be.equal(1)
        expect(uut["ExpressionAttributeValues"][":anotherAggregateFieldName"]).to.be.equal(1)
    })

})

describe("#promiseUpdateQuery()", () => {
    
    let query = {
        TableName: 'SomeTable',
        Key: {
            ddb_partition_key_name: 'anotherUniquePartitionKey',
            ddb_sort_key_name: '1562058000' },
        UpdateExpression: 'ADD #yetAnotherAggregateFieldName :yetAnotherAggregateFieldName',
        ExpressionAttributeNames: {
            '#yetAnotherAggregateFieldName': 'yetAnotherAggregateFieldName'
        },
        ExpressionAttributeValues: {
            ':yetAnotherAggregateFieldName': 1
        },
        ReturnValues: 'ALL_NEW',
        ReturnConsumedCapacity: 'TOTAL'
    }

    beforeEach(() => {
        update = () => {
            return {
                promise: () => {
                    return Promise.resolve({ADynamoDB: "Response"})
                }
            }
        }
        
        AWS = {
            DynamoDB: {
                DocumentClient: {
                    update: sinon.spy(update)
                }
            }
        }
    })

    afterEach(() => {
        AWS = {}
    });

    it("should return a Promise", () => {
        let uut = aggregate.__get__("promiseUpdateQuery")(AWS.DynamoDB.DocumentClient, query)

        expect(uut).to.be.a('promise')
    })
})
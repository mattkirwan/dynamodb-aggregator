"use strict"

const moment = require("moment")

module.exports.hourly = (AWSDocumentClient, metaData = {}, data = {}) => {
    if (!Object.keys(metaData).length)
        return Promise.reject("No query metadata passed")

    if (!("TableName" in metaData) || !("SortKeyName" in metaData) || !("PartitionKeyName" in metaData))
            return Promise.reject("Invalid metadata. Required keys missing")

    if (!Object.keys(data).length)
        return Promise.resolve()

    let formattedData = formatDataHourly(data)

    let partitionedUpdateQueries = Object.keys(formattedData).map(timeGroup => {
        return Object.keys(formattedData[timeGroup]).map(partitionKey => {
            metaData["PartitionKeyValue"] = partitionKey
            metaData["SortKeyValue"] = timeGroup

            return createUpdateQuery(metaData, formattedData[timeGroup][partitionKey])
        })
    })

    let updateQueries = [].concat(...partitionedUpdateQueries)

    console.log(updateQueries)

    let promisedUpdateQueries = updateQueries.map(query => promiseUpdateQuery(AWSDocumentClient, query))

    return Promise.all(promisedUpdateQueries)
}

/**
 * @description Format data into hourly groups with tablename, campaignId, aggregateField and aggregatevalues for all fields
 * @param { PartitionKeyX: { AggregateFieldNameX: [ EventTimestamp, EventTimestamp ] } } data
 * @returns
 * {
 *     TimeGroupTimestamp [Object]: {
 *         PartitionKeyX [Object]: {
 *             AggregateFieldNameX [Number]: 1,
 *             AggregateFieldNameY [Number]: 3,
 *         }
 *     }
 * }
 */
const formatDataHourly = data =>  {

    let formattedParams = {}, startOfHour
    
    Object.keys(data).forEach(partitionKey => {
        Object.keys(data[partitionKey]).forEach(aggregateFieldName => {
            data[partitionKey][aggregateFieldName].forEach(t => {

                startOfHour = moment(t).startOf("hour").unix()
                
                if (!(startOfHour in formattedParams))
                    formattedParams[startOfHour] = {}

                if (!(partitionKey in formattedParams[startOfHour]))
                    formattedParams[startOfHour][partitionKey] = {}
                
                formattedParams[startOfHour][partitionKey][aggregateFieldName] = (formattedParams[startOfHour][aggregateFieldName] || 0) + 1
            })
        })
    })

    return formattedParams
}

/**
 * @description Generate a complete DynamoDB update object
 * @param {*} metaData 
 * @param {*} partitionedAggregateData 
 */
const createUpdateQuery = (metaData, partitionedAggregateData) => {
    let updateExpression = "ADD"
    let expressionAttributeNames = {}
    let expressionAttributeValues = {}
    Object.keys(partitionedAggregateData).forEach(fieldName => {
        updateExpression = `${updateExpression} #${fieldName} :${fieldName},`
        expressionAttributeNames[`#${fieldName}`] = fieldName
        expressionAttributeValues[`:${fieldName}`] = partitionedAggregateData[fieldName]
    })
    updateExpression = updateExpression.replace(/,\s*$/, "")

    return {
        TableName: metaData.TableName,
        Key: {
            [metaData.PartitionKeyName]: metaData.PartitionKeyValue,
            [metaData.SortKeyName]: Number(metaData.SortKeyValue)
        },
        UpdateExpression: updateExpression,
        ExpressionAttributeNames: expressionAttributeNames,
        ExpressionAttributeValues: expressionAttributeValues,
        ReturnValues: "ALL_NEW",
        ReturnConsumedCapacity: "TOTAL"
    }
}

const promiseUpdateQuery = (documentClient, query) => {
    return documentClient.update(query).promise()
}
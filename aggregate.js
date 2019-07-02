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


    // Create update() query

    // update.promise


    // console.log(AWSDocumentClient)

    // AWSDocumentClient()
    AWSDocumentClient.update()

    return Promise.resolve()
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
const formatDataHourly = (data) =>  {

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

const createUpdateQuery = (metaData, partitionedAggregateData) => {
    return {some:"query"}
}










// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

const AWS = require('aws-sdk');

// const ddb = new AWS.DynamoDB.DocumentClient({ apiVersion: '2012-08-10' });
var ddb = new AWS.DynamoDB.DocumentClient();
const { TABLE_NAME } = process.env;

exports.handler = async (event, context) => {
  const payload  = JSON.parse(event.Records[0].body).NewImage;
  let connectionData;
  try {
    connectionData = await ddb.query({
      TableName: TABLE_NAME,
      IndexName: 'userIdIndex',
      KeyConditionExpression: 'userId = :userId',
      ExpressionAttributeValues: {
        ':userId':  payload.userId.S
      },
    }).promise();
  } catch (e) {
    return { statusCode: 500, body: e.stack };
  }
  console.log('connectionData', connectionData.Items)
  const domainHard = 'hgph8uyn64.execute-api.eu-west-1.amazonaws.com';
  const stage = 'Prod';
  const apigwManagementApi = new AWS.ApiGatewayManagementApi({
    apiVersion: '2018-11-29',
    endpoint: domainHard + '/' + stage
  });

  const postData = JSON.stringify(payload);
  const postCalls = connectionData.Items.map(async ({ connectionId }) => {
    try {
      await apigwManagementApi.postToConnection({ ConnectionId: connectionId, Data: postData }).promise();
    } catch (e) {
      if (e.statusCode === 410) {
        console.log(`Found stale connection, deleting ${connectionId}`);
        await ddb.delete({ TableName: TABLE_NAME, Key: { connectionId } }).promise();
      } else {
        throw e;
      }
    }
  });

  try {
    await Promise.all(postCalls);
  } catch (e) {
    return { statusCode: 500, body: e.stack };
  }

  return { statusCode: 200, body: 'Data sent.' };
};

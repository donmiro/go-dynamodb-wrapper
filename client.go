package go_dynamodb_wrapper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DDBTable struct {
	region           string
	name             string
	partitionKeyName string
}

func NewTable(region, name, partitionKeyName string) (*DDBTable, error) {
	if region == "" || name == "" || partitionKeyName == "" {
		return nil, errors.New("you must specify all values: region, name & partition_key name")
	}

	return &DDBTable{
		region:           region,
		name:             name,
		partitionKeyName: partitionKeyName,
	}, nil
}

func (ddb *DDBTable) ReadPartitionKeysList() ([]string, error) {
	svc, err := svcCreator(ddb.region)
	if err != nil {
		return []string{}, err
	}

	var partitionKeys []string
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		input := &dynamodb.ScanInput{
			TableName:            aws.String(ddb.name),
			ProjectionExpression: aws.String(ddb.partitionKeyName),
			ExclusiveStartKey:    lastEvaluatedKey,
		}

		result, err := svc.Scan(context.TODO(), input)
		if err != nil {
			return []string{}, err
		}

		for _, item := range result.Items {
			if pk, ok := item[ddb.partitionKeyName]; ok {
				if s, ok := pk.(*types.AttributeValueMemberS); ok {
					partitionKeys = append(partitionKeys, s.Value)
				}
			}
		}

		if result.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = result.LastEvaluatedKey
	}

	return partitionKeys, nil
}

func (ddb *DDBTable) ReadItem(partitionKeyValue string) (map[string]interface{}, error) {
	svc, err := svcCreator(ddb.region)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.GetItemInput{
		TableName: aws.String(ddb.name),
		Key: map[string]types.AttributeValue{
			ddb.partitionKeyName: &types.AttributeValueMemberS{
				Value: partitionKeyValue,
			},
		},
	}

	result, err := svc.GetItem(context.TODO(), input)
	if err != nil {
		return nil, errors.New("failed to get item")
	}
	if result.Item == nil {
		return nil, errors.New("item not found")
	}

	return convertDynamoDBJSONToMap(result.Item), nil
}

func (ddb *DDBTable) WriteItem(item map[string]interface{}) error {
	svc, err := svcCreator(ddb.region)
	if err != nil {
		return err
	}

	dynamodbItem := convertToDynamoDBJSON(item)
	input := &dynamodb.PutItemInput{
		TableName: aws.String(ddb.name),
		Item:      dynamodbItem,
	}

	_, err = svc.PutItem(context.TODO(), input)
	if err != nil {
		return err
	}

	return nil
}

func (ddb *DDBTable) UpdateItem(partitionKeyValue string, updatedValue map[string]interface{}) error {
	svc, err := svcCreator(ddb.region)
	if err != nil {
		return err
	}

	dynamoDBUpdateValues := convertToDynamoDBJSON(updatedValue)
	updateExpression := "SET "
	expressionAttributeValues := make(map[string]types.AttributeValue)
	expressionAttributeNames := make(map[string]string)
	i := 1
	for k := range dynamoDBUpdateValues {
		updateExpression += fmt.Sprintf("#k%d = :v%d, ", i, i)
		expressionAttributeValues[fmt.Sprintf(":v%d", i)] = dynamoDBUpdateValues[k]
		expressionAttributeNames[fmt.Sprintf("#k%d", i)] = k
		i++
	}

	updateExpression = updateExpression[:len(updateExpression)-2]

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.name),
		Key:                       map[string]types.AttributeValue{ddb.partitionKeyName: &types.AttributeValueMemberS{Value: partitionKeyValue}},
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ExpressionAttributeNames:  expressionAttributeNames,
	}

	_, err = svc.UpdateItem(context.TODO(), input)
	if err != nil {
		return err
	}

	return nil
}

func (ddb *DDBTable) DeleteItem(partitionKeyValue string) error {
	svc, err := svcCreator(ddb.region)
	if err != nil {
		return err
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.name),
		Key: map[string]types.AttributeValue{
			ddb.partitionKeyName: &types.AttributeValueMemberS{
				Value: partitionKeyValue,
			},
		},
	}

	_, err = svc.DeleteItem(context.TODO(), input)
	if err != nil {
		return err
	}

	return nil
}

func DDBTablesList(awsRegion string) ([]string, error) {
	svc, err := svcCreator(awsRegion)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.ListTablesInput{}
	result, err := svc.ListTables(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	return result.TableNames, nil
}

////////////////////////
// Internal functions //
////////////////////////

func svcCreator(awsRegion string) (*dynamodb.Client, error) {
	// Create AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion))
	if err != nil {
		return nil, err
	}

	svc := dynamodb.NewFromConfig(cfg)

	return svc, nil
}

func convertToDynamoDBJSON(regularJSON map[string]interface{}) map[string]types.AttributeValue {
	dynamodbJSON := make(map[string]types.AttributeValue)
	for k, v := range regularJSON {
		dynamodbJSON[k] = convertValue(v)
	}

	return dynamodbJSON
}

func convertDynamoDBJSONToMap(attributes map[string]types.AttributeValue) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range attributes {
		switch val := v.(type) {
		case *types.AttributeValueMemberS:
			result[k] = val.Value
		case *types.AttributeValueMemberN:
			result[k] = val.Value
		case *types.AttributeValueMemberBOOL:
			result[k] = fmt.Sprintf("%v", val.Value)
		case *types.AttributeValueMemberM:
			jsonStr, _ := json.Marshal(convertDynamoDBJSONToMap(val.Value))
			result[k] = string(jsonStr)
		case *types.AttributeValueMemberL:
			result[k] = convertDynamoDBListToSlice(val.Value)
		default:
			result[k] = fmt.Sprintf("%v", v)
		}
	}
	return result
}

func convertDynamoDBListToSlice(list []types.AttributeValue) []interface{} {
	var result []interface{}
	for _, item := range list {
		switch val := item.(type) {
		case *types.AttributeValueMemberS:
			result = append(result, val.Value)
		case *types.AttributeValueMemberN:
			result = append(result, val.Value)
		case *types.AttributeValueMemberBOOL:
			result = append(result, fmt.Sprintf("%v", val.Value))
		case *types.AttributeValueMemberM:
			result = append(result, convertDynamoDBJSONToMap(val.Value))
		case *types.AttributeValueMemberL:
			result = append(result, convertDynamoDBListToSlice(val.Value))
		default:
			result = append(result, fmt.Sprintf("%v", item))
		}
	}
	return result
}

func convertValue(value interface{}) types.AttributeValue {
	switch v := value.(type) {
	case string:
		return &types.AttributeValueMemberS{Value: v}
	case bool:
		return &types.AttributeValueMemberBOOL{Value: v}
	case int, int8, int16, int32, int64, float32, float64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", v)}
	case map[string]interface{}:
		return &types.AttributeValueMemberM{Value: convertToDynamoDBJSON(v)}
	case []interface{}:
		var listValues []types.AttributeValue
		for _, item := range v {
			listValues = append(listValues, convertValue(item))
		}
		return &types.AttributeValueMemberL{Value: listValues}
	default:
		log.Fatalf("Unsupported type: %v", reflect.TypeOf(v))
		return nil
	}
}

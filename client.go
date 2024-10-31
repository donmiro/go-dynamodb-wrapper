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
	client           *dynamodb.Client
}

func NewTable(region, name, partitionKeyName string) (*DDBTable, error) {
	if region == "" || name == "" || partitionKeyName == "" {
		return nil, errors.New("you must specify all values: region, name & partition_key name")
	}

	// Create DynamoDB client
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	return &DDBTable{
		region:           region,
		name:             name,
		partitionKeyName: partitionKeyName,
		client:           client,
	}, nil
}

func (ddb *DDBTable) ReadPartitionKeysList() ([]string, error) {
	var partitionKeys []string
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		input := &dynamodb.ScanInput{
			TableName:            aws.String(ddb.name),
			ProjectionExpression: aws.String(ddb.partitionKeyName),
			ExclusiveStartKey:    lastEvaluatedKey,
		}

		result, err := ddb.client.Scan(context.Background(), input)
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

func (ddb *DDBTable) ScanTable() ([]map[string]interface{}, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(ddb.name),
	}

	result, err := ddb.client.Scan(context.Background(), input)
	if err != nil {
		return make([]map[string]interface{}, 0), err
	}

	var returnedList []map[string]interface{}

	for _, item := range result.Items {
		returnedList = append(returnedList, convertDynamoDBJSONToMap(item))
	}

	return returnedList, nil
}

func (ddb *DDBTable) ReadItem(partitionKeyValue string) (map[string]interface{}, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(ddb.name),
		Key: map[string]types.AttributeValue{
			ddb.partitionKeyName: &types.AttributeValueMemberS{
				Value: partitionKeyValue,
			},
		},
	}

	result, err := ddb.client.GetItem(context.Background(), input)
	if err != nil {
		return nil, errors.New("failed to get item")
	}
	if result.Item == nil {
		return nil, errors.New("item not found")
	}

	return convertDynamoDBJSONToMap(result.Item), nil
}

func (ddb *DDBTable) WriteItem(item map[string]interface{}) error {
	dynamodbItem := convertToDynamoDBJSON(item)
	input := &dynamodb.PutItemInput{
		TableName: aws.String(ddb.name),
		Item:      dynamodbItem,
	}

	_, err := ddb.client.PutItem(context.Background(), input)
	if err != nil {
		return err
	}

	return nil
}

func (ddb *DDBTable) UpdateItem(partitionKeyValue string, updatedValue map[string]interface{}) error {
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

	_, err := ddb.client.UpdateItem(context.Background(), input)
	if err != nil {
		return err
	}

	return nil
}

func (ddb *DDBTable) DeleteItem(partitionKeyValue string) error {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.name),
		Key: map[string]types.AttributeValue{
			ddb.partitionKeyName: &types.AttributeValueMemberS{
				Value: partitionKeyValue,
			},
		},
	}

	_, err := ddb.client.DeleteItem(context.Background(), input)
	if err != nil {
		return err
	}

	return nil
}

func DDBTablesList(awsRegion string) ([]string, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(awsRegion))
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	input := &dynamodb.ListTablesInput{}

	var tables []string
	paginator := dynamodb.NewListTablesPaginator(client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %v", err)
		}

		tables = append(tables, page.TableNames...)
	}

	return tables, nil
}

////////////////////////
// Internal functions //
////////////////////////

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

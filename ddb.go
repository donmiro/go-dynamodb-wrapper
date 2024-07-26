package go_dynamodb_wrapper

import (
	"context"
	"errors"

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

func (ddb *DDBTable) GetPartitionKeysList() ([]string, error) {
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

func (ddb *DDBTable) GetItem(partitionKeyValue string) (map[string]types.AttributeValue, error) {
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

	return result.Item, nil
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

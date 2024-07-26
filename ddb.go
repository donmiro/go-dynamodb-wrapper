package go_dynamodb_wrapper

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DDBTable struct {
	Region           string
	Name             string
	PartitionKeyName string
}

func (ddb *DDBTable) GetPartitionKeysList() ([]string, error) {
	svc, err := svcCreator(ddb.Region)
	if err != nil {
		return []string{}, err
	}

	var partitionKeys []string
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		input := &dynamodb.ScanInput{
			TableName:            aws.String(ddb.Name),
			ProjectionExpression: aws.String(ddb.PartitionKeyName),
			ExclusiveStartKey:    lastEvaluatedKey,
		}

		result, err := svc.Scan(context.TODO(), input)
		if err != nil {
			return []string{}, err
		}

		for _, item := range result.Items {
			if pk, ok := item[ddb.PartitionKeyName]; ok {
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

package go_dynamodb_wrapper

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func cfgCreator(awsRegion string) (aws.Config, error) {
	// Create AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion))
	if err != nil {
		return aws.Config{}, err
	}

	return cfg, nil
}

func DDBTablesList(awsRegion string) ([]string, error) {
	cfg, err := cfgCreator(awsRegion)
	if err != nil {
		return nil, err
	}

	// Create service client
	svc := dynamodb.NewFromConfig(cfg)
	input := &dynamodb.ListTablesInput{}
	result, err := svc.ListTables(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	return result.TableNames, nil
}

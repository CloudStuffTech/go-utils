package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	AuthSource string
	Username   string
	Password   string
	Opts       string
	Database   string
	Hosts      []string
}

type Client struct {
	Mclient *mongo.Client
	Db      *mongo.Database
}

// NewClient method takes a config map argument
func NewClient(conf Config) (*Client, error) {
	var client = &Client{}
	var auth = &options.Credential{
		AuthSource: conf.AuthSource,
		Username:   conf.Username,
		Password:   conf.Password,
	}
	var rs = conf.Opts
	var opts = &options.ClientOptions{
		Hosts:      conf.Hosts,
		ReplicaSet: &rs,
	}
	if len(conf.Username) > 0 && len(conf.Password) > 0 {
		opts.Auth = auth
	}
	mongoClient, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		return nil, err
	}
	client.Mclient = mongoClient
	client.Db = mongoClient.Database(conf.Database)
	return client, nil
}

func (c *Client) GetDb() *mongo.Database {
	return c.Db
}

func (c *Client) Ping() error {
	return c.Mclient.Ping(context.TODO(), nil)
}

func (c *Client) GenerateID() primitive.ObjectID {
	return primitive.NewObjectID()
}

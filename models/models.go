package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/CloudStuffTech/go-utils/cache"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Model interface is a generic type for all the models
type Model interface {
	New() Model
	Table() string
	IsEmpty() bool
	FindOne(db *mongo.Database, id string) Model
	ClearCacheData(cacheClient *cache.Client)
}

// FindOptions struct will contain the additional find information
type FindOptions struct {
	Sort, Hint  interface{}
	Limit, Skip *int64
}

// AggregateOpts method is used for holding options while doing aggregation
type AggregateOpts struct {
	SumField    string
	Sort, Limit interface{}
}

// CountDocs method will count the documents in a table based on query supplied
func CountDocs(db *mongo.Database, model Model, query bson.M) int64 {
	var duration = time.Second
	var opts = &options.CountOptions{MaxTime: &duration}
	var result, _ = db.Collection(model.Table()).CountDocuments(context.Background(), query, opts)
	return result
}

// NewMongoID method will return a new BSON Objec ID
func NewMongoID() primitive.ObjectID {
	return primitive.NewObjectID()
}

// EmptyMongoID method will return an objectID which is empty
func EmptyMongoID() primitive.ObjectID {
	return primitive.NilObjectID
}

// ConvertID method will try to convert given string to BSON Object ID
func ConvertID(id string) primitive.ObjectID {
	bsonID, _ := primitive.ObjectIDFromHex(id)
	return bsonID
}

// ConvertIDs method will try to convert given slice to BSON Object ID slice
func ConvertIDs(ids []string) []primitive.ObjectID {
	var result []primitive.ObjectID
	for _, id := range ids {
		result = append(result, ConvertID(id))
	}
	return result
}

// Aggregate method will aggregate the collection and return the results accordingly
func Aggregate(model Model, query bson.M, groupBy []string, extra *AggregateOpts, db *mongo.Database) {

}

// ToJSON method will convert the model to json string
func ToJSON(m Model) string {
	encoded, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// FindOne method will try to find the object with given query
func FindOne(db *mongo.Database, model Model, query bson.M) Model {
	var duration = time.Second
	var opts = &options.FindOneOptions{MaxTime: &duration}
	var result = db.Collection(model.Table()).FindOne(context.Background(), query, opts)
	if result.Err() == nil {
		result.Decode(model)
	}
	return model
}

// DeleteOne method will delete a single document based on the query
func DeleteOne(db *mongo.Database, model Model, query bson.M) bool {
	var opts = &options.DeleteOptions{}
	var _, err = db.Collection(model.Table()).DeleteOne(context.Background(), query, opts)
	if err == nil {
		return true
	}
	return false
}

// DeleteMany method will delete multiple documents based on the filter
func DeleteMany(db *mongo.Database, model Model, query bson.M) bool {
	var opts = &options.DeleteOptions{}
	var _, err = db.Collection(model.Table()).DeleteMany(context.Background(), query, opts)
	if err == nil {
		return true
	}
	return false
}

// FindAll will try to find the documents based on the query
func FindAll(db *mongo.Database, model Model, query bson.M, queryOpts *FindOptions) []interface{} {
	var duration = time.Second
	var opts = &options.FindOptions{MaxTime: &duration}
	if queryOpts != nil {
		opts.Sort = queryOpts.Sort
		opts.Hint = queryOpts.Hint
		opts.Limit = queryOpts.Limit
		opts.Skip = queryOpts.Skip
	}
	var cur, err = db.Collection(model.Table()).Find(context.Background(), query, opts)
	var dataArr []interface{}
	if err != nil {
		return dataArr
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var dummyObj = model.New()
		err := cur.Decode(dummyObj)
		if err == nil {
			dataArr = append(dataArr, dummyObj)
		}
	}
	return dataArr
}

// FindOneWithOpts method will try to find the object based on the query and options
func FindOneWithOpts(db *mongo.Database, model Model, query bson.M, queryOpts *FindOptions) Model {
	var duration = time.Second
	var opts = &options.FindOneOptions{MaxTime: &duration}
	if queryOpts != nil {
		opts.Sort = queryOpts.Sort
		opts.Hint = queryOpts.Hint
		opts.Skip = queryOpts.Skip
	}
	var result = db.Collection(model.Table()).FindOne(context.Background(), query, opts)
	if result.Err() == nil {
		result.Decode(model)
	}
	return model
}

func clearCache(cacheClient *cache.Client, model Model, id string) {
	cacheKey := getCacheKey(model, id)
	cacheClient.Delete(cacheKey)
}

// Save method will save the document in db and update the cache
func Save(db *mongo.Database, cacheClient *cache.Client, model Model, id string) error {
	var err error
	if model.IsEmpty() {
		_, err = db.Collection(model.Table()).InsertOne(context.Background(), model)
	} else {
		var upsert = true
		var updateOpts = &options.UpdateOptions{Upsert: &upsert}
		if len(id) == 24 {
			oid := ConvertID(id)
			_, err = db.Collection(model.Table()).UpdateOne(context.Background(), bson.M{"_id": oid}, bson.M{"$set": model}, updateOpts)
		} else {
			_, err = db.Collection(model.Table()).UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": model}, updateOpts)
		}
	}
	clearCache(cacheClient, model, id)
	model.ClearCacheData(cacheClient)
	return err
}

// UpdateMany will update the rows of the table based on the query supplied
func UpdateMany(db *mongo.Database, model Model, query, updateObj bson.M) error {
	var updateOpts = &options.UpdateOptions{}
	_, err := db.Collection(model.Table()).UpdateMany(context.Background(), query, updateObj, updateOpts)
	return err
}

// CacheFirst method will try to find the object with given id in cache else it
// will query the db and save the result in cache
func CacheFirst(cacheClient *cache.Client, db *mongo.Database, model Model, id string) Model {
	var cacheKey = getCacheKey(model, id)
	var result, found = cacheClient.Get(cacheKey)
	if found {
		return result.(Model)
	}
	r := model.FindOne(db, id)
	cacheClient.Set(cacheKey, r)
	return r
}

// DateQuery will return the bson representation to query daterange between
// 2 time intervals
func DateQuery(start, end time.Time) bson.M {
	var query = bson.M{"$gte": start, "$lte": end}
	return query
}

func getCacheKey(model Model, id string) string {
	return model.Table() + "::" + id
}

func findByID(coll *mongo.Collection, id string) *mongo.SingleResult {
	var duration = time.Second
	var opts = &options.FindOneOptions{MaxTime: &duration}

	var query = bson.M{"_id": id}
	if len(id) == 24 {
		query["_id"], _ = primitive.ObjectIDFromHex(id)
	}
	var result = coll.FindOne(context.Background(), query, opts)
	return result
}

package modelsv2

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/CloudStuffTech/go-utils/cache"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const OBJECT_ID_LEN = 24

// Model interface is a generic type for all the models
type Model interface {
	New() Model
	Table() string
	IsEmpty() bool
	FindByID(db *mongo.Database, id string) Model
	ClearCacheData(cacheClient *cache.MultiClient)
}

// FindOptions struct will contain the additional find information
type FindOptions struct {
	Sort, Hint  interface{}
	Projection  interface{}
	Limit, Skip *int64
	BatchSize   *int32
	Timeout     time.Duration
}

// AggregateOpts method is used for holding options while doing aggregation
type AggregateOpts struct {
	Match, Group bson.M
	Project      []string
	MaxTime      time.Duration
	Limit        int
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
func Aggregate(db *mongo.Database, model Model, extra *AggregateOpts) ([]interface{}, error) {
	var opts = &options.AggregateOptions{MaxTime: &extra.MaxTime}
	var pipeline []bson.M

	project := bson.M{}
	for _, p := range extra.Project {
		project[p] = 1
	}
	pipeline = append(pipeline, bson.M{"$match": extra.Match})
	pipeline = append(pipeline, bson.M{"$project": project})
	pipeline = append(pipeline, bson.M{"$group": extra.Group})
	if extra.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": extra.Limit})
	}
	var cursor, err = db.Collection(model.Table()).Aggregate(context.Background(), pipeline, opts)

	var results []interface{}
	if err != nil {
		return nil, err
	}
	err = cursor.All(context.Background(), &results)
	return results, err
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
		opts.Projection = queryOpts.Projection
		if queryOpts.Timeout > 0 {
			opts.MaxTime = &queryOpts.Timeout
		}
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

func FindAllv2(db *mongo.Database, model Model, query bson.M, queryOpts *FindOptions) ([]interface{}, error) {
	var duration = time.Second
	var opts = &options.FindOptions{MaxTime: &duration}
	if queryOpts != nil {
		opts.Sort = queryOpts.Sort
		opts.Hint = queryOpts.Hint
		opts.Limit = queryOpts.Limit
		opts.Skip = queryOpts.Skip
		opts.Projection = queryOpts.Projection
		if queryOpts.Timeout > 0 {
			opts.MaxTime = &queryOpts.Timeout
		}
	}
	var cur, err = db.Collection(model.Table()).Find(context.Background(), query, opts)
	var dataArr []interface{}
	if err != nil {
		return dataArr, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		var dummyObj = model.New()
		err := cur.Decode(dummyObj)
		if err == nil {
			dataArr = append(dataArr, dummyObj)
		}
	}
	return dataArr, nil
}

// FindOneWithOpts method will try to find the object based on the query and options
func FindOneWithOpts(db *mongo.Database, model Model, query bson.M, queryOpts *FindOptions) Model {
	var duration = time.Second
	var opts = &options.FindOneOptions{MaxTime: &duration}
	if queryOpts != nil {
		opts.Sort = queryOpts.Sort
		opts.Hint = queryOpts.Hint
		opts.Skip = queryOpts.Skip
		if queryOpts.Timeout > 0 {
			opts.MaxTime = &queryOpts.Timeout
		}
	}
	var result = db.Collection(model.Table()).FindOne(context.Background(), query, opts)
	if result.Err() == nil {
		result.Decode(model)
	}
	return model
}

func clearCache(cacheClient *cache.MultiClient, model Model, id string) {
	cacheKey := GetCacheKey(model, id)
	cacheClient.Delete(cacheKey)
}

// Save method will save the document in db and update the cache
func Save(db *mongo.Database, cacheClient *cache.MultiClient, model Model, id string) error {
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

// Query method will return cursor to the database
func Query(db *mongo.Database, model Model, query bson.M, queryOpts *FindOptions) (*mongo.Cursor, error) {
	var duration = time.Second
	var opts = &options.FindOptions{MaxTime: &duration}
	if queryOpts != nil {
		opts.Sort = queryOpts.Sort
		opts.Hint = queryOpts.Hint
		opts.Limit = queryOpts.Limit
		opts.Skip = queryOpts.Skip
		opts.BatchSize = queryOpts.BatchSize
		if queryOpts.Timeout > 0 {
			opts.MaxTime = &queryOpts.Timeout
		}
	}
	return db.Collection(model.Table()).Find(context.Background(), query, opts)
}

// InsertMany method will insert documents in bulk inside the collection
func InsertMany(db *mongo.Database, model Model, docs []interface{}) ([]interface{}, error) {
	var ordered = false
	opts := &options.InsertManyOptions{
		Ordered: &ordered,
	}
	r, err := db.Collection(model.Table()).InsertMany(context.Background(), docs, opts)
	if r != nil {
		return r.InsertedIDs, err
	}
	return nil, err
}

// UpdateMany will update the rows of the table based on the query supplied
func UpdateMany(db *mongo.Database, model Model, query, updateObj bson.M) error {
	var updateOpts = &options.UpdateOptions{}
	_, err := db.Collection(model.Table()).UpdateMany(context.Background(), query, updateObj, updateOpts)
	return err
}

// CacheFirst method will try to find the object with given id in cache else it
// will query the db and save the result in cache
func CacheFirst(cacheClient *cache.MultiClient, db *mongo.Database, model Model, id string) Model {
	var cacheKey = GetCacheKey(model, id)
	var result, found = cacheClient.Get(cacheKey)
	if found {
		return result.(Model)
	}
	r := model.FindByID(db, id)
	cacheClient.Set(cacheKey, r)
	return r
}

// CacheFirst method will try to find the object with given id in cache else it
// will query the db and save the result in cache
func CacheFirstWithOpts(cacheClient *cache.MultiClient, db *mongo.Database, model Model, query bson.M, queryOpts *FindOptions) Model {
	var cacheKey = GetCacheKeyWithOpts(model, query, queryOpts)
	var result, found = cacheClient.Get(cacheKey)
	if found {
		return result.(Model)
	}
	r := FindOneWithOpts(db, model, query, queryOpts)
	cacheClient.Set(cacheKey, r)
	return r
}

// DateQuery will return the bson representation to query daterange between
// 2 time intervals
func DateQuery(start, end time.Time) bson.M {
	var query = bson.M{"$gte": start, "$lte": end}
	return query
}

// GetCacheKey method returns the cache key for model with id
func GetCacheKey(model Model, id string) string {
	return model.Table() + "::" + id
}

func GetCacheKeyWithOpts(model Model, query bson.M, queryOpts *FindOptions) string {
	keyParts := []string{model.Table() + "::"}

	// Serialize the query
	if queryBytes, err := json.Marshal(query); err == nil {
		keyParts = append(keyParts, "query="+string(queryBytes))
	}

	// Add each optional part
	if queryOpts != nil {
		if queryOpts.Sort != nil {
			if b, err := json.Marshal(queryOpts.Sort); err == nil {
				keyParts = append(keyParts, "sort="+string(b))
			}
		}
		if queryOpts.Hint != nil {
			if b, err := json.Marshal(queryOpts.Hint); err == nil {
				keyParts = append(keyParts, "hint="+string(b))
			}
		}
		if queryOpts.Projection != nil {
			if b, err := json.Marshal(queryOpts.Projection); err == nil {
				keyParts = append(keyParts, "projection="+string(b))
			}
		}
		if queryOpts.Limit != nil {
			keyParts = append(keyParts, fmt.Sprintf("limit=%d", *queryOpts.Limit))
		}
		if queryOpts.Skip != nil {
			keyParts = append(keyParts, fmt.Sprintf("skip=%d", *queryOpts.Skip))
		}
		if queryOpts.BatchSize != nil {
			keyParts = append(keyParts, fmt.Sprintf("batchSize=%d", *queryOpts.BatchSize))
		}
		if queryOpts.Timeout > 0 {
			keyParts = append(keyParts, fmt.Sprintf("timeout=%s", queryOpts.Timeout.String()))
		}
	}

	return fmt.Sprintf("%s", joinKeyParts(keyParts))
}

// FindByID method will try to find the document in collection with given id
func FindByID(coll *mongo.Collection, id string) *mongo.SingleResult {
	var duration = time.Second
	var opts = &options.FindOneOptions{MaxTime: &duration}

	var query = bson.M{"_id": id}
	if len(id) == 24 {
		query["_id"], _ = primitive.ObjectIDFromHex(id)
	}
	var result = coll.FindOne(context.Background(), query, opts)
	return result
}

// FindByID method will try to find the document in collection with given id
func FindByIDWithOpts(coll *mongo.Collection, id string, opts *options.FindOneOptions) *mongo.SingleResult {
	var query = bson.M{"_id": id}
	if len(id) == 24 {
		query["_id"], _ = primitive.ObjectIDFromHex(id)
	}
	var result = coll.FindOne(context.Background(), query, opts)
	return result
}

func joinKeyParts(parts []string) string {
	return stringJoin(parts, "|")
}

func stringJoin(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for i := 1; i < len(parts); i++ {
		out += sep + parts[i]
	}
	return out
}

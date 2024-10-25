package commoncrud

import (
	"context"
	"log/slog"
	"reflect"
	"strconv"

	"github.com/lefalya/commoncrud/interfaces"
	"github.com/lefalya/commoncrud/types"
	"github.com/redis/go-redis/v9"
)

const (
	ascending  = "ascending"
	descending = "descending"
	randomized = iota

	ascendingTrailing  = ":ascby:"
	descendingTrailing = ":descby:"
)

type PaginationType[T interfaces.Item] struct {
	logger                  *slog.Logger
	redisClient             redis.UniversalClient
	filter                  []string
	mongo                   interfaces.Mongo[T]
	itemCache               interfaces.ItemCache[T]
	itemKeyFormat           string
	itemPerPage             int64
	attribute               string
	direction               string
	paginationRedisFormat   string
	paginationFilter        []string
	index                   int
	settledKeyTrailing      string
	cardinalityKeyTrailing  string
	highestScoreKeyTrailing string
	lowestScoreKeyTrailing  string
	sortedSetKeyTrailing    string
}

func Pagination[T interfaces.Item](
	entityName string,
	attribute string,
	order string,
	filterBy []string,
	itemPerPage int64,
	suffix string,
	logger *slog.Logger,
	redisClient redis.UniversalClient,
) *PaginationType[T] {
	itemCache := &ItemCacheType[T]{
		logger:        logger,
		redisClient:   redisClient,
		itemKeyFormat: attribute + ":%s",
	}

	var middleKey string
	var keyFormat string
	for _, filter := range filterBy {
		middleKey += ":" + filter + ":%s"
	}

	var formattedSuffix string
	if suffix != "" {
		formattedSuffix = ":" + suffix
	}

	keyFormat = entityName + middleKey

	pagination := &PaginationType[T]{
		attribute:             attribute,
		direction:             order,
		paginationRedisFormat: keyFormat,
		logger:                logger,
		redisClient:           redisClient,
		itemCache:             itemCache,
		filter:                filterBy,
	}

	t := reflect.TypeOf((*T)(nil)).Elem()

	if pagination.attribute == "createdat" {
		pagination.index = 0
		if pagination.direction == ascending {
			pagination.sortedSetKeyTrailing = ascendingTrailing + "createdat" + formattedSuffix
			pagination.cardinalityKeyTrailing = pagination.sortedSetKeyTrailing + ":cardinality"
		} else {
			pagination.sortedSetKeyTrailing = descendingTrailing + "createdat" + formattedSuffix
		}

	} else {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Tag.Get("bson") == pagination.attribute || f.Tag.Get("db") == pagination.attribute {
				pagination.index = i

				if pagination.direction == ascending {
					pagination.sortedSetKeyTrailing = ascendingTrailing + pagination.attribute + formattedSuffix
					pagination.highestScoreKeyTrailing = pagination.sortedSetKeyTrailing + ":highestscore"
				} else {
					pagination.sortedSetKeyTrailing = descendingTrailing + pagination.attribute + formattedSuffix
					pagination.lowestScoreKeyTrailing = pagination.sortedSetKeyTrailing + ":lowestscore"
				}

				break
			}
		}
	}

	pagination.settledKeyTrailing = pagination.sortedSetKeyTrailing + ":settled"

	return pagination
}

func (pg *PaginationType[T]) WithMongo(mongo interfaces.Mongo[T]) {
	pg.mongo = mongo
}

func (pg *PaginationType[T]) AddItem(item T, paginationParameters ...string) *types.PaginationError {
	if pg.mongo != nil {
		err := pg.mongo.Create(item)
		if err != nil {
			return &types.PaginationError{
				Err:     err.Err,
				Details: err.Details,
				Message: "Failed to create item to MongoDB",
			}
		}
	}

	errorSet := pg.itemCache.Set(item)
	if errorSet != nil {
		return &types.PaginationError{
			Err:     errorSet.Err,
			Details: errorSet.Details,
			Message: "Failed to set item to Redis",
		}
	}

	key := concatKey(pg.paginationRedisFormat, paginationParameters)

	totalItem := pg.redisClient.ZCard(
		context.TODO(),
		key+pg.sortedSetKeyTrailing,
	)
	if totalItem.Err() != nil {
		return &types.PaginationError{
			Err:     REDIS_FATAL_ERROR,
			Details: totalItem.Err().Error(),
			Message: "Failed to count total items on Redis",
		}
	}
	// only add item to sorted set, if the sorted set exists
	if totalItem.Val() > 0 {
		var score float64
		addToSortedSet := false
		// sort createdAt ascending
		if pg.attribute == "createdat" && pg.direction == ascending {
			cardinalityFromRedis := pg.redisClient.Get(context.TODO(), key+pg.cardinalityKeyTrailing)
			if cardinalityFromRedis.Err() != nil {
				if cardinalityFromRedis.Err() == redis.Nil {
					// TODO: reingest cardinality but if reingestion failed then return redis fatal error
				}
				return &types.PaginationError{
					Err:     REDIS_FATAL_ERROR,
					Details: cardinalityFromRedis.Err().Error(),
					Message: "Failed to get cardinality on Redis",
				}
			}

			cardinality, errorParseInt := strconv.ParseInt(cardinalityFromRedis.Val(), 10, 64)
			if errorParseInt != nil {
				// TODO: reingest cardinality but if reingestion failed then return redis fatal error
				return &types.PaginationError{
					Err:     REDIS_FATAL_ERROR,
					Details: cardinalityFromRedis.Err().Error(),
					Message: "Failed to parse cardinality on Redis",
				}
			}

			if totalItem.Val() == cardinality {
				addToSortedSet = true
				score = float64(item.GetCreatedAt().UnixMilli())
			}
		} else if pg.attribute == "createdat" && pg.direction == descending {
			// createdat & descending
			addToSortedSet = true

			// if zcard >= itemPerPage && zcard mod itemPerPage != 0 then :

			if totalItem.Val() >= pg.itemPerPage && totalItem.Val()%pg.itemPerPage != 0 {
				deleteSettledKey := pg.redisClient.Del(context.TODO(), key+pg.settledKeyTrailing)
				if deleteSettledKey.Err() != nil {
					// TODO: remove sorted set
					return &types.PaginationError{
						Err:     REDIS_FATAL_ERROR,
						Details: deleteSettledKey.Err().Error(),
						Message: "Failed to delete settled key on Redis",
					}
				}
			}

			// end if

			score = float64(item.GetCreatedAt().UnixMilli())
		} else {
			value := reflect.ValueOf(&item).Elem().Field(pg.index).Interface()
			if value != nil {
				switch v := value.(type) {
				case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32:
					score = float64(v.(int64))
				case float64:
					score = v
				default:
					score = float64(0)
				}
			} else {
				return &types.PaginationError{
					Err: FOUND_SORTING_BUT_NO_VALUE,
				}
			}

			//var highestScore float64
			//var lowestScore float64
			var thresholdScore float64
			var scoreKey string
			var errorMessage string

			if pg.direction == ascending {
				scoreKey = key + pg.highestScoreKeyTrailing
				errorMessage = "Failed to get highest score of custom attribute sorted set"
			} else if pg.direction == descending {
				scoreKey = key + pg.lowestScoreKeyTrailing
				errorMessage = "Failed to get lowest score of custom attribute sorted set"
			}

			errorGetThreshold := pg.redisClient.Get(context.TODO(), scoreKey)
			if errorGetThreshold.Err() != nil {
				if errorGetThreshold.Err() == redis.Nil {
					// TODO remove sorted set
				}
				return &types.PaginationError{
					Err:     REDIS_FATAL_ERROR,
					Details: errorGetThreshold.Err().Error(),
					Message: errorMessage,
				}
			}

			thresholdScore, errorParseFloat := strconv.ParseFloat(errorGetThreshold.Val(), 64)
			if errorParseFloat != nil {
				// TODO: remove sorted set
				return &types.PaginationError{
					Err:     REDIS_FATAL_ERROR,
					Details: errorParseFloat.Error(),
					Message: "Failed to parse threshold score on Redis",
				}
			}

			if pg.direction == ascending && score <= thresholdScore {
				addToSortedSet = true
			} else if pg.direction == descending && score >= thresholdScore {
				addToSortedSet = true
			}
		}

		if addToSortedSet {
			sortedSetMember := redis.Z{
				Score:  score,
				Member: item.GetRandId(),
			}
			setSortedSet := pg.redisClient.ZAdd(
				context.TODO(),
				key+pg.sortedSetKeyTrailing,
				sortedSetMember,
			)
			if setSortedSet.Err() != nil {
				return &types.PaginationError{
					Err:     REDIS_FATAL_ERROR,
					Details: setSortedSet.Err().Error(),
					Message: "Failed to add item to pagination set on Redis",
				}
			}

			setExpire := pg.redisClient.Expire(
				context.TODO(),
				key+pg.sortedSetKeyTrailing,
				SORTED_SET_TTL,
			)
			if setExpire.Err() != nil {
				return &types.PaginationError{
					Err:     REDIS_FATAL_ERROR,
					Details: setExpire.Err().Error(),
					Message: "Failed to extend pagination set expiration on Redis",
				}
			}
		}
	}

	return nil
}

func (pg *PaginationType[T]) UpdateItem(item T, paginationParameters ...string) *types.PaginationError {
	if pg.mongo != nil {
		err := pg.mongo.Update(item)
		if err != nil {
			return &types.PaginationError{
				Err:     err.Err,
				Details: err.Details,
				Message: "Failed to update item on MongoDB",
			}
		}
	}

	key := concatKey(pg.paginationRedisFormat, paginationParameters)

	errorSet := pg.itemCache.Set(item)
	if errorSet != nil {
		return errorSet
	}

	if pg.attribute != "createdat" {
		var score float64
		// zrank if sorted set exists...
		rank := pg.redisClient.ZRank(context.TODO(), key+pg.sortedSetKeyTrailing, item.GetRandId())
		if rank.Err() != nil {
			if rank.Err() == redis.Nil {
				return nil
			}
			return &types.PaginationError{
				Err:     REDIS_FATAL_ERROR,
				Details: rank.Err().Error(),
				Message: "Fatal error while getting member's rank from sorted set",
			}
		}

		value := reflect.ValueOf(&item).Elem().Field(pg.index).Interface()
		if value != nil {
			switch v := value.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32:
				score = float64(v.(int64))
			case float64:
				score = v
			default:
				score = float64(0)
			}
		} else {
			return &types.PaginationError{
				Err: FOUND_SORTING_BUT_NO_VALUE,
			}
		}

		member := redis.Z{
			Score:  score,
			Member: item.GetRandId(),
		}
		updateSortedSet := pg.redisClient.ZAdd(context.TODO(), key+pg.sortedSetKeyTrailing, member)
		if updateSortedSet.Err() != nil {
			return &types.PaginationError{
				Err:     REDIS_FATAL_ERROR,
				Details: updateSortedSet.Err().Error(),
				Message: "Failed to update score on sorted set!",
			}
		}

		updateSortedSetExpiration := pg.redisClient.Expire(
			context.TODO(),
			key+pg.sortedSetKeyTrailing,
			SORTED_SET_TTL,
		)
		if updateSortedSetExpiration.Err() != nil {
			return &types.PaginationError{
				Err:     REDIS_FATAL_ERROR,
				Details: updateSortedSetExpiration.Err().Error(),
				Message: "Failed to extend sorted set expiration!",
			}
		}
	}

	return nil
}

func (pg *PaginationType[T]) RemoveItem(pagKeyParams []string, item T) *types.PaginationError {
	key := concatKey(pg.paginationRedisFormat, pagKeyParams)

	if pg.mongo != nil {
		err := pg.mongo.Delete(item)
		if err != nil {
			return &types.PaginationError{
				Err:     err.Err,
				Details: err.Details,
				Message: "Failed to delete item from MongoDB",
			}
		}
	}

	errorDelete := pg.itemCache.Del(item)
	if errorDelete != nil {
		return errorDelete
	}

	itemRank := pg.redisClient.ZRank(
		context.TODO(),
		key,
		item.GetRandId(),
	)
	if itemRank.Err() != nil {
		if itemRank.Err() == redis.Nil {
			// will skip member removal from sorted set
			return nil
		}
		return &types.PaginationError{
			Err:     REDIS_FATAL_ERROR,
			Details: itemRank.Err().Error(),
			Message: "Failed to count total items on Redis",
		}
	}

	// only remove item from sorted set, if the sorted set exists
	removeItemFromSortedSet := pg.redisClient.ZRem(context.TODO(), key+pg.sortedSetKeyTrailing, item.GetRandId())
	if removeItemFromSortedSet.Err() != nil {
		return &types.PaginationError{
			Err:     REDIS_FATAL_ERROR,
			Details: removeItemFromSortedSet.Err().Error(),
			Message: "Failed to remove item from pagination set on Redis",
		}
	}

	// if attribute is not createdat then re-set the highest & lowest key
	if pg.attribute == "createdat" {

	} else {
		var score float64
		value := reflect.ValueOf(&item).Elem().Field(pg.index).Interface()
		if value != nil {
			switch v := value.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32:
				score = float64(v.(int64))
			case float64:
				score = v
			default:
				score = float64(0)
			}
		} else {
			return &types.PaginationError{
				Err: FOUND_SORTING_BUT_NO_VALUE,
			}
		}

		var thersholdKey string
		if pg.direction == ascending {
			thersholdKey = key + pg.highestScoreKeyTrailing
		} else if pg.direction == descending {
			thersholdKey = key + pg.lowestScoreKeyTrailing
		}

		thresholdFromCache := pg.redisClient.Get(context.TODO(), thersholdKey)
		if thresholdFromCache.Err() != nil {
			// TODO redis error, will decide what to do in future
		}

		threshold, errorParseFloat := strconv.ParseFloat(thresholdFromCache.Val(), 64)
		if errorParseFloat != nil {
			// TODO will decide what to do in future
		}

		if threshold == score {
			// TODO ambil item te
		}
	}

	return nil
}

/*
func (pg *PaginationType[T]) TotalItemOnCache(pagKeyParams []string) *types.PaginationError {
	key := concatKey(pg.pagKeyFormat, pagKeyParams)

	var sortedSetKey string
	if pg.sorting != nil && pg.sorting.direction == ascending {
		sortedSetKey = key + ascendingTrailing + pg.sorting.attribute
	} else if pg.sorting != nil && pg.sorting.direction == descending {
		sortedSetKey = key + descendingTrailing + pg.sorting.attribute
	} else {
		sortedSetKey = key + descendingTrailing + "createdat"
	}

	totalItem := pg.redisClient.ZCard(
		context.TODO(),
		sortedSetKey,
	)

	if totalItem.Err() != nil {
		return &types.PaginationError{
			Err:     REDIS_FATAL_ERROR,
			Details: totalItem.Err().Error(),
			Message: "Failed to count total items on Redis",
		}
	}

	return nil
}

func (pg *PaginationType[T]) FetchOne(randId string) (*T, *types.PaginationError) {
	item, errorGet := pg.itemCache.Get(randId)

	if errorGet != nil {
		return nil, errorGet
	}

	return &item, nil
}

func (pg *PaginationType[T]) FetchLinked(
	pagKeyParams []string,
	references []string,
	itemPerPage int64,
	processor interfaces.PaginationProcessor[T]) ([]T, *types.PaginationError) {
	var items []T
	var start int64
	var stop int64

	key := concatKey(pg.pagKeyFormat, pagKeyParams)
	totalReferences := len(references)

	if totalReferences > 0 {
		if totalReferences > MAXIMUM_AMOUNT_REFERENCES {
			return nil, &types.PaginationError{
				Err:     TOO_MUCH_REFERENCES,
				Message: "Too much references!",
			}
		}

		for i := len(references) - 1; i >= 0; i-- {
			var rank *redis.IntCmd
			if pg.sorting != nil && pg.sorting.direction == ascending {
				sortedSetKey := key + ascendingTrailing + pg.sorting.attribute
				rank = pg.redisClient.ZRank(context.TODO(), sortedSetKey, references[i])
			} else if pg.sorting != nil && pg.sorting.direction == descending {
				sortedSetKey := key + descendingTrailing + pg.sorting.attribute
				rank = pg.redisClient.ZRevRank(context.TODO(), sortedSetKey, references[i])
			} else {
				sortedSetKey := key + descendingTrailing + "createdat"
				rank = pg.redisClient.ZRevRank(context.TODO(), sortedSetKey, references[i])
			}

			if rank.Err() != nil {
				if rank.Err() == redis.Nil {
					continue
				}

				return nil, &types.PaginationError{
					Err:     REDIS_FATAL_ERROR,
					Details: rank.Err().Error(),
					Message: "Failed to get reference's index from pagination set on Redis",
				}
			}

			start = rank.Val() + 1
			break
		}

		if start == 0 {
			return nil, &types.PaginationError{
				Err:     NO_VALID_REFERENCES,
				Message: "No references found from pagination set on Redis",
			}
		}
	}

	stop = start + itemPerPage - 1

	var members *redis.StringSliceCmd
	if pg.sorting != nil && pg.sorting.direction == ascending {
		sortedSetKey := key + ascendingTrailing + pg.sorting.attribute
		members = pg.redisClient.ZRange(context.TODO(), sortedSetKey, start, stop)
	} else if pg.sorting != nil && pg.sorting.direction == descending {
		sortedSetKey := key + descendingTrailing + pg.sorting.attribute
		members = pg.redisClient.ZRevRange(context.TODO(), sortedSetKey, start, stop)
	} else {
		sortedSetKey := key + descendingTrailing + "createdat"
		members = pg.redisClient.ZRevRange(context.TODO(), sortedSetKey, start, stop)
	}

	if members.Err() != nil {
		return nil, &types.PaginationError{
			Err:     REDIS_FATAL_ERROR,
			Details: members.Err().Error(),
			Message: "Failed to get items from pagination set on Redis",
		}
	}

	if len(members.Val()) > 0 {
		pg.redisClient.Expire(context.TODO(), key, SORTED_SET_TTL)

		for _, member := range members.Val() {
			item, errorGetItem := pg.itemCache.Get(member)
			if errorGetItem != nil && errorGetItem.Err == REDIS_FATAL_ERROR {
				return nil, &types.PaginationError{
					Err:     errorGetItem.Err,
					Details: errorGetItem.Details,
					Message: "Failed to get item details from Redis",
				}
			} else if errorGetItem != nil && errorGetItem.Err == KEY_NOT_FOUND {
				continue
			}

			if processor != nil {
				processor(item, &items)
			} else {
				items = append(items, item)
			}
		}
	}

	return items, nil
}

func (pg *PaginationType[T]) FetchAll(pagKeyParams []string, processor interfaces.PaginationProcessor[T]) ([]T, *types.PaginationError) {
	var items []T
	key := concatKey(pg.pagKeyFormat, pagKeyParams)

	var members *redis.StringSliceCmd
	if pg.sorting != nil && pg.sorting.direction == ascending {
		sortedSetKey := key + ascendingTrailing + pg.sorting.attribute
		members = pg.redisClient.ZRange(context.TODO(), sortedSetKey, 0, -1)
	} else if pg.sorting != nil && pg.sorting.direction == descending {
		sortedSetKey := key + descendingTrailing + pg.sorting.attribute
		members = pg.redisClient.ZRevRange(context.TODO(), sortedSetKey, 0, -1)
	} else {
		sortedSetKey := key + descendingTrailing + "createdat"
		members = pg.redisClient.ZRevRange(context.TODO(), sortedSetKey, 0, -1)
	}

	if members.Err() != nil {
		return nil, &types.PaginationError{
			Err:     REDIS_FATAL_ERROR,
			Details: members.Err().Error(),
			Message: "Failed to get items from pagination set on Redis",
		}
	}

	if len(members.Val()) > 0 {
		pg.redisClient.Expire(context.TODO(), key, SORTED_SET_TTL)

		for _, member := range members.Val() {
			item, errorGetItem := pg.itemCache.Get(member)
			if errorGetItem != nil && errorGetItem.Err == REDIS_FATAL_ERROR {
				return nil, &types.PaginationError{
					Err:     errorGetItem.Err,
					Details: errorGetItem.Details,
					Message: "Failed to get item details from Redis",
				}
			} else if errorGetItem != nil && errorGetItem.Err == KEY_NOT_FOUND {
				continue
			}

			if processor != nil {
				processor(item, &items)
			} else {
				items = append(items, item)
			}
		}
	}

	return items, nil
}

func (pg *PaginationType[T]) SeedOne(randId string) (*T, *types.PaginationError) {
	var result T
	if pg.mongo != nil {
		item, errorFind := pg.mongo.FindOne(randId)
		if errorFind != nil {
			if errorFind.Err == MONGO_FATAL_ERROR {
				return nil, &types.PaginationError{
					Err:     errorFind.Err,
					Details: errorFind.Details,
					Message: "Item not found on MongoDB",
				}
			} else {
				// MONGO_FATAL_ERROR
				return nil, &types.PaginationError{
					Err:     errorFind.Err,
					Details: errorFind.Details,
					Message: "Fatal error from MongoDB while finding item",
				}
			}
		}
		result = item
	} else {
		return nil, &types.PaginationError{
			Err:     NO_DATABASE_CONFIGURED,
			Message: "No database configured",
		}
	}

	return &result, nil
}

func (pg *PaginationType[T]) SeedLinked(
	paginationKeyParameters []string,
	reference T,
	itemPerPage int64,
	processor interfaces.SeedProcessor[T],
) ([]T, *types.PaginationError) {
	var result []T
	var filter bson.D

	if pg.mongo != nil {
		if !reflect.ValueOf(reference).IsZero() {
			inMongoItem, ok := any(reference).(interfaces.MongoItem)
			if !ok {
				// return lastItem must be in MongoItem
				return nil, &types.PaginationError{
					Err:     LASTITEM_MUST_MONGOITEM,
					Message: "Using MongoDB as database but the reference is not in MongoItem",
				}
			}

			filter = bson.D{
				{
					Key: "$and",
					Value: append(
						pg.mongo.GetPaginationFilter(),
						bson.A{
							bson.D{
								{
									Key: "_id",
									Value: bson.D{
										{
											Key:   "$lt",
											Value: inMongoItem.GetObjectId(),
										},
									},
								},
							},
						}...,
					),
				},
			}
		} else {
			if pg.mongo != nil {
				filter = bson.D{
					{Key: "$and", Value: pg.mongo.GetPaginationFilter()},
				}
			}
		}

		findOptions := options.Find()
		if pg.sorting != nil && pg.sorting.direction == ascending {
			findOptions.SetSort(bson.D{{"_id", 1}})
		} else {
			findOptions.SetSort(bson.D{{"_id", -1}})
		}

		findOptions.SetLimit(itemPerPage)

		items, errorFindItems := pg.mongo.FindMany(
			filter,
			findOptions,
			pg,
			paginationKeyParameters,
			processor,
		)

		if errorFindItems != nil {
			return nil, &types.PaginationError{
				Err:     errorFindItems.Err,
				Details: errorFindItems.Details,
				Message: "MongoDB fatal error while retrieving items",
			}
		}

		result = items
	} else {
		return nil, &types.PaginationError{
			Err:     NO_DATABASE_CONFIGURED,
			Message: "No database configured",
		}
	}

	return result, nil
}

func (pg *PaginationType[T]) SeedAll(
	paginationKeyParameters []string,
	processor interfaces.SeedProcessor[T],
) ([]T, *types.PaginationError) {
	var results []T
	var filter bson.D

	if pg.mongo != nil {
		filter = bson.D{
			{Key: "$and", Value: pg.mongo.GetPaginationFilter()},
		}

		findOptions := options.Find()
		if pg.sorting != nil && pg.sorting.direction == ascending {
			findOptions.SetSort(bson.D{{"_id", 1}})
		} else {
			findOptions.SetSort(bson.D{{"_id", -1}})
		}

		cursor, errorFindItems := pg.mongo.FindMany(
			filter,
			findOptions,
			pg,
			paginationKeyParameters,
			processor,
		)

		if errorFindItems != nil {
			return nil, &types.PaginationError{
				Err:     errorFindItems.Err,
				Details: errorFindItems.Details,
				Message: "MongoDB fatal error while retrieving items",
			}
		}

		results = cursor
	} else {
		return nil, &types.PaginationError{
			Err:     NO_DATABASE_CONFIGURED,
			Message: "No database configured",
		}
	}

	return results, nil
}
*/

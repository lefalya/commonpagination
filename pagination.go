package commoncrud

import (
	"context"
	"log/slog"
	"reflect"
	"strconv"

	"github.com/lefalya/commoncrud/interfaces"
	"github.com/lefalya/commoncrud/types"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	ascending  = "ascending"
	descending = "descending"
	randomized = iota

	ascendingTrailing  = ":ascby:"
	descendingTrailing = ":descby:"
)

type PaginationType[T interfaces.Item] struct {
	pagKeyFormat  string
	itemKeyFormat string
	logger        *slog.Logger
	redisClient   redis.UniversalClient
	sorting       []types.SortingOption
	mongo         interfaces.Mongo[T]
	itemCache     interfaces.ItemCache[T]
}

func SetIndex[T interfaces.Item](sortOpts *[]types.SortingOption) {
	t := reflect.TypeOf((*T)(nil)).Elem()
	for i := range *sortOpts {
		sortOpt := &(*sortOpts)[i]
		if sortOpt.Attribute == "createdat" {
			sortOpt.Index = 0
			if sortOpt.Direction == ascending {
				sortOpt.SortedSetKeyTrailing = ascendingTrailing + "createdat"
				sortOpt.SettledKeyTrailing = sortOpt.SortedSetKeyTrailing + ":settled"
				sortOpt.CardinalityKeyTrailing = sortOpt.SortedSetKeyTrailing + ":cardinality"
			} else {
				sortOpt.SortedSetKeyTrailing = descendingTrailing + "createdat"
			}

		} else {
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if f.Tag.Get("bson") == sortOpt.Attribute || f.Tag.Get("db") == sortOpt.Attribute {
					sortOpt.Index = i

					if sortOpt.Direction == ascending {
						sortOpt.SortedSetKeyTrailing = ascendingTrailing + sortOpt.Attribute
						sortOpt.HighestScoreKeyTrailing = sortOpt.SortedSetKeyTrailing + ":highestscore"
					} else {
						sortOpt.SortedSetKeyTrailing = descendingTrailing + sortOpt.Attribute
						sortOpt.LowestScoreKeyTrailing = sortOpt.SortedSetKeyTrailing + ":lowestscore"
					}

					break
				}
			}
		}
	}
}

func Pagination[T interfaces.Item](
	pagKeyFormat string,
	itemKeyFormat string,
	logger *slog.Logger,
	redisClient redis.UniversalClient,
	sortOpts []types.SortingOption, // atau mungkin bisa dibuat sortArgs... saja
) *PaginationType[T] {
	itemCache := &ItemCacheType[T]{
		logger:        logger,
		redisClient:   redisClient,
		itemKeyFormat: itemKeyFormat,
	}

	pagination := &PaginationType[T]{
		pagKeyFormat:  pagKeyFormat,
		itemKeyFormat: itemKeyFormat,
		logger:        logger,
		redisClient:   redisClient,
		itemCache:     itemCache,
	}

	var validatedSortOpts []types.SortingOption
	if sortOpts != nil {
		SetIndex[T](&sortOpts)
		validatedSortOpts = sortOpts
	} else {
		defaultSortOpt := []types.SortingOption{
			{
				Attribute: "createdat",
				Direction: "descending",
				Index:     0,
			},
		}

		SetIndex[T](&defaultSortOpt)
		validatedSortOpts = append(validatedSortOpts, defaultSortOpt[0])
	}
	pagination.sorting = validatedSortOpts

	return pagination
}

func (pg *PaginationType[T]) WithMongo(mongo interfaces.Mongo[T], paginationFilter bson.A) {
	pg.mongo = mongo
	pg.mongo.SetPaginationFilter(paginationFilter)
}

func (pg *PaginationType[T]) AddItem(pagKeyParams []string, item T) *types.PaginationError {
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

	key := concatKey(pg.pagKeyFormat, pagKeyParams)

	for _, sortOpt := range pg.sorting {
		totalItem := pg.redisClient.ZCard(
			context.TODO(),
			key+sortOpt.SortedSetKeyTrailing,
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
			if sortOpt.Attribute == "createdat" && sortOpt.Direction == ascending {
				cardinalityFromRedis := pg.redisClient.Get(context.TODO(), key+sortOpt.CardinalityKeyTrailing)
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
				} else {
					deleteSettledKey := pg.redisClient.Del(context.TODO(), key+sortOpt.SettledKeyTrailing)
					if deleteSettledKey.Err() != nil {
						// TODO: remove sorted set
						return &types.PaginationError{
							Err:     REDIS_FATAL_ERROR,
							Details: deleteSettledKey.Err().Error(),
							Message: "Failed to delete settled key on Redis",
						}
					}
				}
				// sort custom attribute
			} else if sortOpt.Attribute != "createdat" {
				value := reflect.ValueOf(&item).Elem().Field(sortOpt.Index).Interface()
				if value != nil {
					switch v := value.(type) {
					case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32:
						score = float64(v.(int64))
					case float64:
						score = v
					default:
						return &types.PaginationError{
							Err:     MUST_BE_NUMERICAL_DATATYPE,
							Message: "Cannot use assigned attribute value for sorting due to its invalid datatype.",
						}
					}
				} else {
					return &types.PaginationError{
						Err: FOUND_SORTING_BUT_NO_VALUE,
					}
				}

				var highestScore float64
				var lowestScore float64

				highestScoreKey := key + sortOpt.HighestScoreKeyTrailing
				errorGetHighest := pg.redisClient.Get(context.TODO(), highestScoreKey)
				if errorGetHighest.Err() != nil {
					if errorGetHighest.Err() == redis.Nil {
						// TODO remove sorted set
					}
					return &types.PaginationError{
						Err:     REDIS_FATAL_ERROR,
						Details: errorGetHighest.Err().Error(),
						Message: "Failed to get highest score of custom attribute sorted set",
					}
				}

				lowestScoreKey := key + sortOpt.LowestScoreKeyTrailing
				errorGetLowest := pg.redisClient.Get(context.TODO(), lowestScoreKey)
				if errorGetLowest.Err() != nil {
					if errorGetLowest.Err() == redis.Nil {
						// TODO remove sorted set
					}
					return &types.PaginationError{
						Err:     REDIS_FATAL_ERROR,
						Details: errorGetLowest.Err().Error(),
						Message: "Failed to get lowest score of custom attribute sorted set",
					}
				}

				if sortOpt.Direction == ascending && score <= highestScore {
					addToSortedSet = true
				} else if sortOpt.Direction == descending && score >= lowestScore {
					addToSortedSet = true
				}
			} else {
				// createdat & descending
				addToSortedSet = true
			}

			if addToSortedSet {
				sortedSetMember := redis.Z{
					Score:  score,
					Member: item.GetRandId(),
				}
				setSortedSet := pg.redisClient.ZAdd(
					context.TODO(),
					key+sortOpt.SortedSetKeyTrailing,
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
					key+sortOpt.SortedSetKeyTrailing,
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
	}

	return nil
}

/*
func (pg *PaginationType[T]) UpdateItem(item T) *types.PaginationError {
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

	errorSet := pg.itemCache.Set(item)
	if errorSet != nil {
		return errorSet
	}

	return nil
}

func (pg *PaginationType[T]) RemoveItem(pagKeyParams []string, item T) *types.PaginationError {
	key := concatKey(pg.pagKeyFormat, pagKeyParams)

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

	// only remove item from sorted set, if the sorted set exists
	if totalItem.Val() > 0 {
		removeItemSortedSet := pg.redisClient.ZRem(context.TODO(), sortedSetKey, item.GetRandId())

		if removeItemSortedSet.Err() != nil {
			return &types.PaginationError{
				Err:     REDIS_FATAL_ERROR,
				Details: removeItemSortedSet.Err().Error(),
				Message: "Failed to remove item from pagination set on Redis",
			}
		}
	}

	// will not return an error if the ZRem, Del, or Delete command results in zero deletions.
	return nil
}

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

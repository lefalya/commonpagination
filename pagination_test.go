package commoncrud

import (
	"log/slog"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/golang/mock/gomock"
	"github.com/lefalya/commoncrud/interfaces"
	mock_interfaces "github.com/lefalya/commoncrud/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	brand    = "Volkswagen"
	category = "SUV"
	car      = NewMongoItem(Car{
		Brand:    brand,
		Category: category,
		Seating: []Seater{
			{
				Material:  "Leather",
				Occupancy: 2,
			},
			{
				Material:  "Leather",
				Occupancy: 3,
			},
			{
				Material:  "Leather",
				Occupancy: 2,
			},
		},
	})

	itemKeyFormat        = "car:%s"
	paginationKeyFormat  = "car:brands:%s:type:%s"
	paginationParameters = []string{"Volkswagen", "SUV"}
	paginationFilter     = bson.A{
		bson.D{{"car", brand}},
		bson.D{{"category", category}},
	}
	key = concatKey(paginationKeyFormat, paginationParameters)
)

func initTestPaginationType[T interfaces.Item](
	pagKeyFormat string,
	itemKeyFormat string,
	logger *slog.Logger,
	redisClient redis.UniversalClient,
	itemCache interfaces.ItemCache[T],
) *PaginationType[T] {
	return &PaginationType[T]{
		pagKeyFormat:  pagKeyFormat,
		itemKeyFormat: itemKeyFormat,
		logger:        logger,
		redisClient:   redisClient,
		itemCache:     itemCache,
	}
}

type Seater struct {
	Material  string
	Occupancy int64
}

type Car struct {
	*Item
	*MongoItem
	Brand    string
	Category string
	Seating  []Seater
}

func TestInjectPagination(t *testing.T) {
	type Injected[T interfaces.Item] struct {
		pagination interfaces.Pagination[T]
	}

	pagination := Pagination[Car]("", "", nil, nil)
	injected := Injected[Car]{
		pagination: pagination,
	}

	assert.NotNil(t, injected)
}

func TestConcatKey(t *testing.T) {

}

func TestAddItem(t *testing.T) {
	t.Run("successfully add item without addition to sorted set", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mongoMock := mock_interfaces.NewMockMongo[Car](ctrl)
		mongoMock.EXPECT().Create(car).Return(nil)
		mongoMock.EXPECT().SetPaginationFilter(nil)

		redis, mockedRedis := redismock.NewClientMock()
		mockedRedis.ExpectZCard(key).SetVal(0)

		pagination := initTestPaginationType[Car](
			paginationKeyFormat,
			itemKeyFormat,
			logger,
			redis,
			nil,
		)
		pagination.WithMongo(mongoMock, nil)

		errorAddItem := pagination.AddItem(paginationParameters, car)
		assert.Nil(t, errorAddItem)
	})

	t.Run("successfully add item with addition to sorted set", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
	})

	t.Run("successfully add item with no database specified", func(t *testing.T) {

	})

	t.Run("zcard fatal error", func(t *testing.T) {})

	t.Run("zadd fatal error", func(t *testing.T) {})

	t.Run("set expire fatal error", func(t *testing.T) {})

	t.Run("mongo create error", func(t *testing.T) {})
}

func TestUpdateItem(t *testing.T) {
	t.Run("successfully update item", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mongoMock := mock_interfaces.NewMockMongo[Car](ctrl)
		mongoMock.EXPECT().SetPaginationFilter(nil)
		mongoMock.EXPECT().Update(car).Return(nil)

		itemCache := mock_interfaces.NewMockItemCache[Car](ctrl)
		itemCache.EXPECT().Set(car).Return(nil)

		pagination := initTestPaginationType[Car](
			paginationKeyFormat,
			itemKeyFormat,
			logger,
			nil,
			itemCache,
		)
		pagination.WithMongo(mongoMock, nil)

		errorUpdateItem := pagination.UpdateItem(car)
		assert.Nil(t, errorUpdateItem)
	})

	t.Run("successfull update with no database specified", func(t *testing.T) {})

	t.Run("error mongo update", func(t *testing.T) {})

	t.Run("error set itemcache", func(t *testing.T) {})
}

func TestRemoveItem(t *testing.T) {
	t.Run("successfully remove item with no sorted set exits", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		redis, mockedRedis := redismock.NewClientMock()
		mockedRedis.ExpectZCard(key).SetVal(0)

		mongoMock := mock_interfaces.NewMockMongo[Car](ctrl)
		mongoMock.EXPECT().SetPaginationFilter(nil)
		mongoMock.EXPECT().Delete(car).Return(nil)

		itemCache := mock_interfaces.NewMockItemCache[Car](ctrl)
		itemCache.EXPECT().Del(car).Return(nil)

		pagination := initTestPaginationType[Car](
			paginationKeyFormat,
			itemKeyFormat,
			logger,
			redis,
			itemCache,
		)
		pagination.WithMongo(mongoMock, nil)

		errorRemoveItem := pagination.RemoveItem(paginationParameters, car)
		assert.Nil(t, errorRemoveItem)
	})

	t.Run("remove item success with no database specified", func(t *testing.T) {})

	t.Run("zcard fatal error", func(t *testing.T) {})

	t.Run("zrem fatal error", func(t *testing.T) {})

	t.Run("itemcache delete error", func(t *testing.T) {})

	t.Run("mongo delete error", func(t *testing.T) {})
}

func TestTotalItemOnCache(t *testing.T) {

	t.Run("", func(t *testing.T) {})
}

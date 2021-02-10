package bulk_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"gitlab.eldorado.ru/golang/bulk"
	"testing"
	"time"
)

type City struct {
	CityId     uint32    `json:"city_id,omitempty"`
	Name       string    `json:"name,omitempty"`
	RegionId   int       `json:"region_id,omitempty"`
	Comment    string    `json:"comment,omitempty"`
	DateCreate time.Time `jsom:"date_create"`
}

//
//func TestTest1(t *testing.T) {
//
//	databaseUrl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
//		"user",
//		"user",
//		"localhost",
//		5432,
//		"test",
//		"disable",
//	)
//
//	bulkService, err := bulk.NewBulkService(databaseUrl, []bulk.TableParam{
//		{
//			Name:     "city",
//			PkColumn: "city_id",
//			Fields: map[string]string{
//				"Name":       "name",
//				"RegionId":   "region_id",
//				"Comment":    "comment",
//				"DateCreate": "date_create",
//			},
//			UniqueConstraint: []string{"name"},
//		},
//		{
//			Name:             "region",
//			PkColumn:         "region_id",
//			UniqueConstraint: []string{"region_id"},
//		},
//	})
//	if err != nil {
//		t.Fatal(bulkService, err)
//	}
//
//	version, err := bulkService.CreateDataVersion([]string{"city", "region"})
//	if err != nil {
//		t.Fatal(bulkService, err)
//	}
//
//	cities := []*City{
//		{
//			Name:       "test1",
//			RegionId:   3,
//			Comment:    "test",
//			DateCreate: time.Now(),
//		},
//		{
//			Name:       "test2",
//			RegionId:   3,
//			Comment:    "test",
//			DateCreate: time.Now(),
//		},
//		{
//			Name:       "test3",
//			RegionId:   3,
//			Comment:    "test",
//			DateCreate: time.Now(),
//		},
//		{
//			Name:     "test4",
//			RegionId: 3,
//			Comment:  "test",
//		},
//		{
//			Name:       "test5",
//			RegionId:   3,
//			DateCreate: time.Now(),
//		},
//		{
//			Name:       "test6",
//			RegionId:   3,
//			Comment:    "test",
//			DateCreate: time.Now(),
//		},
//		{
//			Name:       "test7",
//			RegionId:   3,
//			Comment:    "test",
//			DateCreate: time.Now(),
//		},
//		{
//			Name:       "test8",
//			RegionId:   3,
//			Comment:    "test",
//			DateCreate: time.Now(),
//		},
//		{
//			Name:       "test9",
//			RegionId:   3,
//			Comment:    "test",
//			DateCreate: time.Now(),
//		},
//	}
//
//	models := make([]interface{}, 0)
//	for _, v := range cities {
//		models = append(models, v)
//	}
//
//	successfulModels, err := bulkService.InsertDataVersion(uint(version.DataVersionId), models, "city")
//	fmt.Print(successfulModels, err)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	err = bulkService.ApplyDataVersion(uint(version.DataVersionId), []string{"city", "region"})
//	if err != nil {
//		t.Fatal(err)
//	}
//
//}

func TestNewBulkService(t *testing.T) {
	ctx := context.Background()
	src, err := bulk.NewBulkService(ctx, bulk.Config{
		Conn: bulk.DataBase{
			DataBaseName: "stms",
			User:         "postgres",
			Password:     "postgres",
			Host:         "localhost",
			Port:         5432,
			Secure:       "disable",
		},
		TableBulkParams: bulk.TableParam{
			Name:     "city",
			PkColumn: "city_id",
			Fields: map[string]string{
				"Name":       "name",
				"RegionId":   "region_id",
				"Comment":    "comment",
				"DateCreate": "date_create",
			},
			UniqueConstraint: []string{"name"},
		},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, src)

	src.CreateDataVersion(ctx, "region")
}

func TestBulkService_CreateDataVersion(t *testing.T) {
}

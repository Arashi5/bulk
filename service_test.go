package bulk

import (
	"github.com/DATA-DOG/go-sqlmock"
)

type City struct {
	CityId uint32 `json:"city_id,omitempty"`
	Name string  `json:"name,omitempty"`
}


func NewMockBulkService(tableBulkParams []TableParam) (*BulkService, sqlmock.Sqlmock) {
	//db, mock, _ := sqlmock.New()
	//bulkService := &BulkService{
	//	Db:          db,
	//	tableParams: make(map[string]tableBulkParam),
	//}
	//
	//hasher := md5.New()
	//for _, v := range tableBulkParams {
	//	bulkService.tableParams[v.Name] = tableBulkParam{
	//		tableName:        v.Name,
	//		pkColumn:         v.PkColumn,
	//		fields:           v.Fields,
	//		uniqueConstraint: v.UniqueConstraint,
	//		hasher:           hasher,
	//	}
	//}

	return nil, nil
}
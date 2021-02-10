package bulk

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"
)

const (
	bulkVersionField      = "data_version_bulk"
	defaultClearDelayHour = 12
)

type resource interface {
	Query(string, ...interface{}) (*sql.Rows, error)
}

type DataVersion struct {
	DataVersionId uint32 `json:"data_version_id,omitempty"`
	Name          string `json:"name,omitempty"`
}

type Model map[string]interface{}
type Models []Model

type BulkService struct {
	conn        *Connection
	tx          transaction
	tableParams map[string]tableBulkParam
	clearDelay  uint
	ctx         context.Context
}

type Config struct {
	Conn            DataBase
	TableBulkParams []TableParam
	ClearDelayHour  uint
}

type DataBase struct {
	User         string
	Password     string
	Host         string
	Port         int
	DataBaseName string
	Secure       string
}

func NewBulkService(ctx context.Context, config Config) (*BulkService, error) {
	conn, err := Connect(ctx, &config)
	if err != nil {
		return nil, err
	}

	if config.ClearDelayHour == 0 {
		config.ClearDelayHour = defaultClearDelayHour
	}

	bulkService := &BulkService{
		conn:        conn,
		tableParams: make(map[string]tableBulkParam),
		clearDelay:  config.ClearDelayHour,
	}

	hasher := md5.New()
	for _, v := range config.TableBulkParams {
		if len(v.UniqueConstraint) == 0 {
			return nil, NewError(InvalidArgument, fmt.Sprint("empty UniqueConstraint in $s ", v.Name))
		}

		bulkService.tableParams[v.Name] = tableBulkParam{
			tableName:        v.Name,
			pkColumn:         v.PkColumn,
			fields:           v.Fields,
			uniqueConstraint: v.UniqueConstraint,
			hasher:           hasher,
		}
	}

	go bulkService.loop()

	return bulkService, nil
}

func (s BulkService) CreateDataVersion(ctx context.Context, modelCode string) (version *DataVersion, err error) {
	conn, ctx, err := s.tx.Begin(ctx, s.conn)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	version = &DataVersion{}

	if modelCode == "" {
		err = NewError(InvalidArgument, "Model codes are not specified")
		return
	}

	ht, ctx := s.DBHasTable(ctx, "data_version")
	if !ht {
		ct := `
			CREATE TABLE data_version
			(
				data_version_id serial PRIMARY KEY,
				"time"          timestamp without time zone,
				"delete"        boolean DEFAULT false
			);`
		_, err = conn.Exec(ctx, ct)
		if err != nil {
			return
		}
	}

	defer func() {
		if r := recover(); r != nil {
			s.tx.Rollback(ctx)
			err, _ = r.(error)
		}
	}()

	_, err = conn.Exec(ctx, `INSERT INTO data_version (time) VALUES ($1) RETURNING data_version_id`, time.Now())
	if err != nil {
		s.tx.Rollback(ctx)
		return
	}

	err = s.createTableBulk(ctx, modelCode, uint(version.DataVersionId))
	if err != nil {
		s.tx.Rollback(ctx)
		return
	}

	s.tx.Commit(ctx)

	return
}

func (s BulkService) createTableBulk(ctx context.Context, modelCode string, dataVersionId uint) (err error) {
	conn, ctx, err := s.tx.Begin(ctx, s.conn)
	if err != nil {
		return  err
	}
	defer conn.Release()

	tableParams := s.tableParams[modelCode]
	queryMaxRes := 0

	tableNameNew := tableParams.tableNameNew(dataVersionId)
	sequenceNameNew := tableParams.sequenceNameNew(dataVersionId)
	sequenceForAlignmentName := tableParams.sequenceForAlignmentName(dataVersionId)
	sequenceVersionBulkName := tableParams.sequenceVersionBulkName(dataVersionId)
	tn, ctx := s.DBHasTable(ctx, tableNameNew)
	tp, ctx := s.DBHasTable(ctx, tableParams.tableName)

	if !tn && tp{
		// копируем таблицу со всеми индексами и constraint`ами и первичным ключем, исключая данные
		conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %v (LIKE %v INCLUDING ALL);", tableNameNew, tableParams.tableName))

		// создаем сиквенсы, которые продолжатся с последнего ID + 1 из оригинальной таблицы
		conn.QueryRow(ctx, fmt.Sprintf("SELECT MAX(%v) FROM %v;", tableParams.pkColumn, tableParams.tableName)).Scan(&queryMaxRes)
		conn.Exec(ctx, fmt.Sprintf("CREATE SEQUENCE %v START %v;", sequenceNameNew, queryMaxRes+1))
		conn.Exec(ctx, fmt.Sprintf("CREATE SEQUENCE %v START %v;", sequenceForAlignmentName, queryMaxRes+1))
		conn.Exec(ctx, fmt.Sprintf("CREATE SEQUENCE %v;", sequenceVersionBulkName))

		// добавляем колонку с номером пачки батча, пригодится чтобы возвращать добавленные/измененные записи
		conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %v ADD %v integer NULL;", tableNameNew, bulkVersionField))
		conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %v ADD data_version_dupl boolean NOT NULL DEFAULT false;", tableNameNew))

		// удаляем первичный ключ, т.к. во время батчинга он будет мешать. Вернем на место во время применения батча
		if cn := GetConstraintName(ctx, conn, tableNameNew, []string{tableParams.pkColumn}); cn != "" {
			conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %v DROP CONSTRAINT %v;", tableNameNew, cn))
		}

		// устанавливаем у таблицы новую сиквенс
		conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT nextval('%v')", tableNameNew, tableParams.pkColumn, sequenceNameNew))
		conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %v ALTER %v SET NOT NULL;", tableNameNew, tableParams.pkColumn))

		// узнаем название constraint`а, т.к. при вызове CREATE TABLE ... LIKE название constraint`а было создано случайным образом
		// (на самом деле не случайным, но т.к. название constraint`а не должно превышать 64 символа название может быть обрезано
		// самим движком базы данных. Можно конечно попытаться обрезать название также, как это делает движок БД, но нет гарантий,
		// что этот алгоритм не изменится в новой версии БД)
		originalUniqueConstraint := GetConstraintName(ctx, conn, tableNameNew, []string{tableParams.pkColumn});
		if originalUniqueConstraint != "" {
			conn.Exec(ctx, fmt.Sprintf("ALTER INDEX %v RENAME TO %v;", originalUniqueConstraint, tableParams.uniqueConstraintNew(dataVersionId)))
		}
	}

	return err
}

func (s BulkService) DBHasTable(ctx context.Context, tableName string) (bool, context.Context) {
	conn, ctx, err := s.tx.Begin(ctx, s.conn)
	if err != nil {
		return false, ctx
	}

	r := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)`, tableName)

	var result bool
	err = r.Scan(&result)
	if err != nil {
		return false, ctx
	}
	return result, ctx
}

//func (s BulkService) ApplyDataVersion(dataVersionId uint, modelCodes []string) (err error) {
//	err = s.versionExist(dataVersionId)
//	if err != nil {
//		return NewError(InvalidArgument, "Version not found")
//	}
//	if len(modelCodes) == 0 {
//		return NewError(InvalidArgument, "Model codes are not specified")
//	}
//
//	tx, err := s.Db.Begin()
//	if err != nil {
//		tx.Rollback()
//		err = NewError(Unknown, err.Error())
//		return
//	}
//	defer func() {
//		if r := recover(); r != nil {
//			tx.Rollback()
//			err, _ = r.(error)
//		}
//	}()
//	for _, modelCode := range modelCodes {
//		if !s.hasTable(tx, s.tableParams[modelCode].tableName) {
//			continue
//		}
//
//		tableParams := s.tableParams[modelCode]
//		tableNameNew := tableParams.tableNameNew(dataVersionId)
//		sequenceNameNew := tableParams.sequenceNameNew(dataVersionId)
//		sequenceForAlignmentName := tableParams.sequenceForAlignmentName(dataVersionId)
//		sequenceVersionBulkName := tableParams.sequenceVersionBulkName(dataVersionId)
//		if s.hasTable(tx, tableNameNew) && s.hasTable(tx, tableParams.tableName) {
//			// название сиквенса ищем до удаления таблицы, т.к. после удаления таблицы ничего не вернется, но при этому
//			// сам сиквенс еще будет существовать
//			sequenceName := s.getSequenceName(tx, tableParams.tableName, tableParams.pkColumn)
//			originalUniqueConstraint := s.getConstraintName(tx, tableParams.tableName, tableParams.uniqueConstraint)
//
//			// удаляем старую таблицу
//			tx.Exec(fmt.Sprintf("DROP TABLE %v;", tableParams.tableName))
//
//			// удаляем колонку версии, создаем индекс первичного ключа
//			tx.Exec(fmt.Sprintf("ALTER TABLE %v DROP %v;", tableNameNew, bulkVersionField))
//			tx.Exec(fmt.Sprintf("ALTER TABLE %v DROP data_version_dupl;", tableNameNew))
//			tx.Exec(fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v PRIMARY KEY (%v);", tableNameNew, tableParams.pkName(), tableParams.pkColumn))
//
//			// переименовывем новую таблицу (заменяет собой старую таблицу), убираем значение по-умолчанию для первичного ключа
//			tx.Exec(fmt.Sprintf("ALTER TABLE %v RENAME TO %v;", tableNameNew, tableParams.tableName))
//			tx.Exec(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP DEFAULT;", tableParams.tableName, tableParams.pkColumn))
//			tx.Exec(fmt.Sprintf("ALTER TABLE %v ALTER %v SET NOT NULL;", tableParams.tableName, tableParams.pkColumn))
//
//			// удаляем сиквенсы старой таблицы. Если сиквенс окажется serial, то она удалится в момент удаления таблицы
//			// и вместо названия сиквенса вернется пустая строка
//			if sequenceName != "" {
//				tx.Exec(fmt.Sprintf("DROP SEQUENCE IF EXISTS  %v;", sequenceName))
//			}
//
//			// удаляем сиквенсы: новой таблицы, батчинга
//			tx.Exec(fmt.Sprintf("DROP SEQUENCE %v;", sequenceNameNew))
//			tx.Exec(fmt.Sprintf("DROP SEQUENCE %v;", sequenceVersionBulkName))
//
//			// переименовываем индекс и сиквенс
//			tx.Exec(fmt.Sprintf("ALTER INDEX %v RENAME TO %v;", tableParams.uniqueConstraintNew(dataVersionId), originalUniqueConstraint))
//			tx.Exec(fmt.Sprintf("ALTER SEQUENCE %v RENAME TO %v;", sequenceForAlignmentName, tableParams.sequenceName()))
//
//			// устанавливаем таблице новый сиквенс (в котором ID идут по порядку без разрывов)
//			tx.Exec(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT nextval('%v');", tableParams.tableName, tableParams.pkColumn, tableParams.sequenceName()))
//		} else {
//			tx.Rollback()
//			return NewError(InvalidArgument, "Version for entity not found")
//		}
//	}
//	err = tx.Commit()
//
//	return err
//}

func (s BulkService) Revert(ctx context.Context, dataVersionId uint, modelCodes []string) (err error) {
	conn, err := GetMasterConn(ctx, s.conn)
	if err != nil {
		return
	}
	defer conn.Release()

	if len(modelCodes) == 0 {
		return NewError(InvalidArgument, "Model codes are not specified")
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		tx.Rollback(ctx)
		err = NewError(Unknown, err.Error())
		return
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback(ctx)
			err, _ = r.(error)
		}
	}()

	for _, modelCode := range modelCodes {
		tableParams := s.tableParams[modelCode]
		tableNameNew := tableParams.tableNameNew(dataVersionId)
		sequenceNameNew := tableParams.sequenceNameNew(dataVersionId)
		sequenceVersionBulkName := tableParams.sequenceVersionBulkName(dataVersionId)
		sequenceForAlignmentName := tableParams.sequenceForAlignmentName(dataVersionId)
		ht, ctx := s.DBHasTable(ctx, tableNameNew)
		if !ht {
			continue
		}

		attrs := make([]interface{}, 4)
		attrs = append(
			attrs,
			tableNameNew,
			sequenceNameNew,
			sequenceVersionBulkName,
			sequenceForAlignmentName,
		)

		query := `
			DROP TABLE $1;
			DROP SEQUENCE ($2,$3,$4);`

		tx.Exec(ctx, query, attrs)
	}

	err = tx.Commit(ctx)

	return err
}

//func (s BulkService) versionExist(id uint) (err error) {
//	version := new(DataVersion)
//	err = s.Db.QueryRow(fmt.Sprintf(`SELECT data_version_id FROM data_version WHERE data_version_id = %v`, id)).Scan(&version.DataVersionId)
//	return
//}

func (s BulkService) clearRepeat(models []interface{}, paramsName string) (rModels []interface{}) {

	keys := make(map[string]bool)
	tableParams := s.tableParams[paramsName]

	if len(models) > 0 {
		for _, item := range models {
			obj := s.mapFromInterface(item, tableParams)

			tkey := make([]string, 0)
			for _, conctrain := range tableParams.uniqueConstraint {
				if v, ok := obj[conctrain]; ok {
					tkey = append(tkey, fmt.Sprintf("%v", v))
				}
			}
			key := strings.Join(tkey, "/%/")

			if _, ok := keys[key]; !ok {
				keys[key] = true
				rModels = append(rModels, item)
			}
		}
	}

	return
}

func (s BulkService) splitPK(models []interface{}, paramsName string) (idModels []interface{}, Models []interface{}) {
	tableParams := s.tableParams[paramsName]

	if len(models) > 0 {
		for _, item := range models {
			obj := s.mapFromInterface(item, tableParams)

			if v, ok := obj[tableParams.pkColumn]; ok && v != nil {
				idModels = append(idModels, item)
			} else {
				Models = append(Models, item)
			}
		}
	}

	return
}

//func (s BulkService) InsertDataVersion(dataVersionId uint, models []interface{}, paramsName string) (successfulModels Models, err error) {
//	err = s.versionExist(dataVersionId)
//	if err != nil {
//		err = NewError(InvalidArgument, err.Error())
//		return
//	}
//	models = s.clearRepeat(models, paramsName)
//
//	tableParams := s.tableParams[paramsName]
//
//	tx, err := s.Db.Begin()
//	if err != nil {
//		tx.Rollback()
//		err = NewError(Unknown, err.Error())
//		return
//	}
//	defer func() {
//		if r := recover(); r != nil {
//			tx.Rollback()
//			err, _ = r.(error)
//		}
//	}()
//
//	var dataVersionBulk uint
//	if dataVersionBulk, err = s.createBulkModels(tx, models, dataVersionId, tableParams); err != nil {
//		tx.Rollback()
//		err = NewError(Unknown, err.Error())
//		return
//	}
//
//	if err = s.updatePrimaryKey(tx, tableParams, dataVersionId, dataVersionBulk); err != nil {
//		tx.Rollback()
//		err = NewError(Unknown, err.Error())
//		return
//	}
//
//	if err = tx.Commit(); err != nil {
//		err = NewError(Unknown, err.Error())
//		return
//	}
//
//	newTableName := tableParams.tableNameNew(dataVersionId)
//	rows, err := s.Db.Query(fmt.Sprintf("SELECT * FROM %v WHERE %v = %v", newTableName, bulkVersionField, dataVersionBulk))
//	if err != nil {
//		err = NewError(Unknown, err.Error())
//		return
//	}
//
//	err = s.unmarshalModels(rows, &successfulModels)
//	if err != nil {
//		err = NewError(Unknown, err.Error())
//		return
//	}
//
//	return
//}

func (s BulkService) unmarshalModels(rows *sql.Rows, successfulModels *Models) (err error) {
	fields, err := rows.Columns()
	if err != nil {
		err = NewError(Unknown, err.Error())
		return
	}

	for rows.Next() {
		args := make([]interface{}, len(fields))
		argsLink := make([]interface{}, len(fields))
		for i := 0; i < len(fields); i++ {
			argsLink[i] = &args[i]
		}
		err := rows.Scan(argsLink...)
		if err == nil {
			model := Model{}
			for i, n := range fields {
				model[n] = args[i]
			}
			*successfulModels = append(*successfulModels, model)
		}
	}

	return
}

// Обновляет первичный ключ для записей текущей части батча. Для записей, которые существовали ранее - восстановит их
// старые ключи из оригинальной таблицы. Для новых записей - выстроит первичный ключ по порядку, чтобы не было разрывов
// между идентификаторами
//func (s BulkService) updatePrimaryKey(db *sql.Tx, param tableBulkParam, dataVersionId uint, dataVersionBulk uint) error {
//	columns := make([]string, 0)
//	newTableName := param.tableNameNew(dataVersionId)
//	for _, v := range param.uniqueConstraint {
//		columns = append(columns, fmt.Sprintf("%v.%v = %v.%v", param.tableName, v, newTableName, v))
//	}
//	columns = append(columns, fmt.Sprintf("%v.%v = %v", newTableName, bulkVersionField, dataVersionBulk))
//
//	// стартовое значение, с которого начиналась сиквенс. Будет неизменно, даже после вызова nextval у сиквенса
//	sequenceForAlignmentName := param.sequenceForAlignmentName(dataVersionId)
//	sequenceEndVal := s.getSequenceStartValue(sequenceForAlignmentName) - 1
//
//	// чтобы не обновлять записи, которые успели добавить в оригинальную таблицу после начала батчинга
//	columns = append(columns, fmt.Sprintf("%v.%v <= %v", param.tableName, param.pkColumn, sequenceEndVal))
//
//	// обновление запсией, которые существовали раньше (восстановление их старых идентификаторов)
//	columnsStr := strings.Join(columns, " AND ")
//	_, err := db.Exec(
//		`UPDATE ` + newTableName +
//			` SET ` + param.pkColumn + ` = ` + param.tableName + `.` + param.pkColumn +
//			` FROM ` + param.tableName +
//			` WHERE ` + columnsStr + `;`)
//	if err != nil {
//		return err
//	}
//
//	// выравнивание по порядку идентификаторов новых записей, которые раньше не существовали
//	_, err = db.Exec(fmt.Sprintf(
//		`UPDATE `+newTableName+
//			` SET `+param.pkColumn+` = nextval('`+sequenceForAlignmentName+`') `+
//			` WHERE `+bulkVersionField+` = %v `+
//			` AND data_version_dupl = false `+
//			` AND `+param.pkColumn+` > %v;`, dataVersionBulk, sequenceEndVal,
//	))
//	if err != nil {
//		return err
//	}
//
//	return nil
//}

// Возвращает стартовое значение сиквенса. Стартовое значение будет неизменным даже после вызова nextval()
//func (s BulkService) getSequenceStartValue(sequenceName string) (val int) {
//	s.Db.QueryRow("SELECT start_value as val  FROM information_schema.sequences WHERE CAST(sequence_name AS text) = $1;", sequenceName).Scan(&val)
//	return
//}

// Возвращает название сериала
func (s BulkService) getSequenceName(db *sql.Tx, tableName, pkColumnName string) string {
	columnDefault := ""

	// сначала ищем сериал
	db.QueryRow("SELECT column_default as name from information_schema.columns where table_name=$1 AND column_name=$2;", tableName, pkColumnName).Scan(&columnDefault)
	name := strings.TrimPrefix(columnDefault, "nextval('")
	name = strings.TrimSuffix(name, "'::regclass)")

	return name
}

//  Добавляет или изменяет записи
//func (s BulkService) createBulkModels(db *sql.Tx, items []interface{}, dataVersionId uint, tableParam tableBulkParam) (dataVersionBulk uint, err error) {
//	err = s.versionExist(dataVersionId)
//	if err != nil {
//		return 0, NewError(InvalidArgument, "Version not found")
//	}
//
//	tableNameNew := tableParam.tableNameNew(dataVersionId)
//	if !s.hasTable(db, tableNameNew) {
//		return 0, fmt.Errorf("error bulk insert: table %v does not exist", tableParam.tableName)
//	}
//
//	sequenceVersionBulkName := tableParam.sequenceVersionBulkName(dataVersionId)
//	db.QueryRow(fmt.Sprintf(`SELECT nextval('%v') as max;`, sequenceVersionBulkName)).Scan(&dataVersionBulk)
//
//	uniqueConstraint := s.getConstraintName(db, tableNameNew, tableParam.uniqueConstraint)
//	err = s.batchInsert(db, items, tableNameNew, uniqueConstraint, dataVersionBulk, tableParam)
//
//	return dataVersionBulk, err
//}

func (s BulkService) fieldsFromInterface(item interface{}, tableParam tableBulkParam, fields *[]string) {
	ts := reflect.TypeOf(item)
	el := reflect.ValueOf(item)

	if k := el.Kind(); k == reflect.Ptr {
		ts = ts.Elem()
		el = el.Elem()
	}

	if k := el.Kind(); k != reflect.Struct && k != reflect.Interface {
		return
	}

	for i := 0; i < ts.NumField(); i++ {
		if columnName, ok := tableParam.fields[ts.Field(i).Name]; ok {
			*fields = append(*fields, columnName)
		}
	}

	return
}

func (s BulkService) mapFromInterface(item interface{}, tableParam tableBulkParam) (obj map[string]interface{}) {
	itemType := reflect.TypeOf(item)
	itemVal := reflect.ValueOf(item)

	if k := itemVal.Kind(); k == reflect.Ptr {
		itemType = itemType.Elem()
		itemVal = itemVal.Elem()
	}

	if k := itemVal.Kind(); k != reflect.Struct && k != reflect.Interface {
		return
	}

	obj = make(map[string]interface{})
	for i := 0; i < itemType.NumField(); i++ {
		if columnName, ok := tableParam.fields[itemType.Field(i).Name]; ok {
			v := itemVal.Field(i)
			if isEmptyValue(v) {
				continue
			}
			obj[columnName] = v.Interface()
		}
	}

	return
}

func (s BulkService) batchInsert(db *sql.Tx, items []interface{}, tableName, uniqueConstraintName string, dataVersionBulk uint, tableParam tableBulkParam) error {
	// If there is no data, nothing to do.
	if len(items) == 0 {
		return nil
	}

	qfields, values, replace, attrs := s.getInsertQueryParams(items, tableParam, dataVersionBulk)

	sql := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s ON CONFLICT ON CONSTRAINT %v DO UPDATE SET %v;",
		tableName,
		strings.Join(qfields, ", "),
		strings.Join(values, ", "),
		uniqueConstraintName,
		strings.Join(replace, ", "),
	)
	_, err := db.Exec(sql, attrs...)

	if err != nil {
		return err
	}

	return nil
}

func (s BulkService) getInsertQueryParams(items []interface{}, tableParam tableBulkParam, dataVersionBulk uint) (qfields []string, values []string, replace []string, attrs []interface{}) {
	fields := make([]string, 0)
	if dataVersionBulk != 0 {
		fields = append(fields, bulkVersionField)
	}
	s.fieldsFromInterface(items[0], tableParam, &fields)

	qfields = make([]string, len(fields))
	for i, f := range fields {
		qfields[i] = "\"" + f + "\""
	}

	values = make([]string, len(items))
	attrs = make([]interface{}, 0)
	n := 1
	for num, item := range items {
		obj := s.mapFromInterface(item, tableParam)
		vals := make([]string, 0)

		if dataVersionBulk != 0 {
			vals = append(vals, fmt.Sprintf("$%v", n))
			attrs = append(attrs, dataVersionBulk)
			n++
		}

		for _, f := range fields {
			if dataVersionBulk != 0 && f == bulkVersionField {
				continue
			}

			if value, ok := obj[f]; ok {
				attrs = append(attrs, value)
				vals = append(vals, fmt.Sprintf("$%v", n))
				n++
			} else {
				vals = append(vals, "default")
			}
		}

		values[num] = "(" + strings.Join(vals, ", ") + ")"
	}

	replace = make([]string, 0)
	for _, v := range fields {
		if v == tableParam.pkColumn {
			continue
		}
		replace = append(replace, fmt.Sprintf("%v = excluded.%v", v, v))
	}
	if dataVersionBulk != 0 {
		replace = append(replace, `"data_version_dupl" = true`)
	}

	return
}

// Insert - fast and unsafe insert, insert items in exist table, return inserting items
//func (s BulkService) Insert(ctx context.Context, items []interface{}, paramsName string) (successfulModels Models, err error) {
//	// If there is no data, nothing to do.
//	if len(items) == 0 {
//		return
//	}
//
//	tableParam, ok := s.tableParams[paramsName]
//	if !ok {
//		return
//	}
//
//	items = s.clearRepeat(items, paramsName)
//	idModels, items := s.splitPK(items, paramsName)
//
//	tx, err := s.Db.Begin()
//	if err != nil {
//		tx.Rollback()
//		err = NewError(Unknown, err.Error())
//		return
//	}
//
//	if len(idModels) > 0 {
//		if err = s.insert(ctx, tx, idModels, tableParam, &successfulModels, []string{tableParam.pkColumn}); err != nil {
//			tx.Rollback()
//			return
//		}
//	}
//
//	if len(items) > 0 {
//		if err = s.insert(ctx, tx, items, tableParam, &successfulModels, tableParam.uniqueConstraint); err != nil {
//			tx.Rollback()
//			return
//		}
//	}
//
//	tx.Commit()
//	return
//}

//func (s BulkService) insert(ctx context.Context, tx *sql.Tx, items []interface{}, tableParam tableBulkParam, successfulModels *Models, uniqueConstraint []string) (err error) {
//	f, v, r, attrs := s.getInsertQueryParams(items, tableParam, 0)
//
//	uniqueConstraintName := s.getConstraintName(s.Db, tableParam.tableName, uniqueConstraint)
//	sql := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s ON CONFLICT ON CONSTRAINT %v DO UPDATE SET %v RETURNING *;",
//		tableParam.tableName,
//		strings.Join(f, ", "),
//		strings.Join(v, ", "),
//		uniqueConstraintName,
//		strings.Join(r, ", "),
//	)
//
//	rows, err := tx.QueryContext(ctx, sql, attrs...)
//	if err != nil {
//		return NewError(Unknown, err.Error())
//	}
//
//	err = s.unmarshalModels(rows, successfulModels)
//	if err != nil {
//		err = NewError(Unknown, err.Error())
//		return NewError(Unknown, err.Error())
//	}
//
//	return
//}

// Возвращает название constraint по его колонкам.
// Огранчение - если создано 2 и более constraint с одинаковыми колонками, то вернет один из constraint
//func (s BulkService) getConstraintName(ctx context.Context, tableName string, columnNames []string) string {
//	empty := ""
//	if len(columnNames) == 0 {
//		return empty
//	}
//
//	columnNamesQuoted := make([]string, len(columnNames))
//	for i, v := range columnNames {
//		columnNamesQuoted[i] = fmt.Sprintf("'%v'", v)
//	}
//
//	q := `SELECT kcu1.constraint_name
//		  FROM information_schema.key_column_usage as kcu1
//		  WHERE kcu1.table_name = '%v' AND kcu1.constraint_name IN (
//				SELECT DISTINCT kcu.constraint_name
//				FROM information_schema.key_column_usage as kcu
//				WHERE kcu.table_name = '%v' AND kcu.column_name IN (%v)
//		  )
//		  GROUP BY kcu1.constraint_name
//		  HAVING COUNT(kcu1.constraint_name) = %v`
//
//	query := fmt.Sprintf(q, tableName, tableName, strings.Join(columnNamesQuoted, ", "), len(columnNamesQuoted))
//
//	rows, err := db.Query(query)
//	defer rows.Close()
//	if err != nil {
//		return empty
//	}
//
//	for rows.Next() {
//		constraintName := ""
//
//		err := rows.Scan(&constraintName)
//		if err != nil {
//			return empty
//		}
//
//		return constraintName
//	}
//
//	return empty
//}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func (s BulkService) loop() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	clearTick := time.Tick(time.Hour * time.Duration(s.clearDelay))

	for {
		select {
		case <-clearTick:
			if err := s.cleaner(); err != nil {
				fmt.Println(err)
			}
		case <-quit:
			break
		}
	}
}

// garbage collector
func (s BulkService) cleaner() (err error) {
	conn, err := GetMasterConn(s.ctx, s.conn)
	if err != nil {
		return
	}
	defer conn.Release()

	// Находим все записи старше определённого срока
	query := `
		UPDATE public.data_version
		SET delete = true
		WHERE time < $1
		  AND delete = false
		RETURNING data_version_id`
	rows, err := conn.Query(s.ctx, query, time.Now().Add(-(time.Hour * time.Duration(s.clearDelay))))
	if err != nil {
		return
	}

	defer func() {
		rows.Close()
	}()

	for rows.Next() {
		dataVersionID := uint(0)

		err := rows.Scan(&dataVersionID)
		if err != nil {
			continue
		}

		err = s.clean(dataVersionID)
		if err != nil {
			_, _ = conn.Exec(s.ctx, `UPDATE public.data_version SET delete = false WHERE data_version_id = $1`, dataVersionID)
		}
	}

	return
}

// Удаляет все доступные таблици и запись о операции
func (s BulkService) clean(dataVersionId uint) (err error) {
	conn, err := GetMasterConn(s.ctx, s.conn)
	if err != nil {
		return
	}
	defer conn.Release()

	// список доступных таблиц
	mc := make([]string, 0)
	for s := range s.tableParams {
		mc = append(mc, s)
	}

	// Удаляем все временные таблицы
	if err := s.Revert(s.ctx, dataVersionId, mc); err != nil {
		return err
	}

	// удаляем версию
	_, _ = conn.Exec(s.ctx, `DELETE FROM data_version WHERE data_version_id = $1`, dataVersionId)

	return
}

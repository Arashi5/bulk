# Bulk

Сервис для массового обновления записей в базе данных. Работает только с postrgres&

Пример использования

##### Создание сервиса
```
bulkService := bulk.NewBulkService(dbConnection, []bulk.TableParam{
    {
        Name:             "city",
        PkColumn:         "city_id",
        UniqueConstraint: []string{"name", "region_id"},
    },
    {
        Name:             "region",
        PkColumn:         "region_id",
        UniqueConstraint: []string{"name"},
    },
})
```

##### Создали новую версию набора данных
```
version, err := bulkService.CreateDataVersion([]string{"city", "region"})

```


##### Добавление данных
```
cities := make([]City, 0)
models := make([]interface{}, 0)
successfulModels := make([]*City, 0)
for _, v := range cities {
    models = append(models, v)
}
err := bulkService.InsertDataVersion(version.DataVersionId, models, &successfulModels, "city")
```

##### Применить изменения
```
err := bulkService.ApplyDataVersion(version.DataVersionId, []string{"city", "region"})
```

package bulk

import (
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
)

type TableParam struct {
	Name             string
	PkColumn         string
	Fields           map[string]string
	UniqueConstraint []string
}

type tableBulkParam struct {
	tableName        string
	fields           map[string]string
	pkColumn         string
	uniqueConstraint []string
	hasher           hash.Hash
}

func (s tableBulkParam) tableNameNew(dataVersionId uint) string {
	return fmt.Sprintf("%v_%v", s.tableName, dataVersionId)
}

func (s tableBulkParam) sequenceName() string {
	return fmt.Sprintf("%v_pk_seq", s.tableName)
}

func (s tableBulkParam) sequenceNameNew(dataVersionId uint) string {
	hashRunes := []rune(s.getMD5Hash(s.pkColumn))

	return fmt.Sprintf("%v_%v_seq", s.tableNameNew(dataVersionId), string(hashRunes[0:8]))
}

func (s tableBulkParam) sequenceVersionBulkName(dataVersionId uint) string {
	return fmt.Sprintf("%v_bulk_seq", s.tableNameNew(dataVersionId))
}

func (s tableBulkParam) sequenceForAlignmentName(dataVersionId uint) string {
	hashRunes := []rune(s.getMD5Hash(fmt.Sprintf("%v_aligment", s.pkColumn)))

	return fmt.Sprintf("%v_%v_seq", s.tableNameNew(dataVersionId), string(hashRunes[0:8]))
}

func (s tableBulkParam) pkName() string {
	return fmt.Sprintf("%v_%v_pkey", s.tableName, s.pkColumn)
}

func (s tableBulkParam) pkNameNew(dataVersionId uint) string {
	return fmt.Sprintf("%v_%v_pkey", s.tableName, dataVersionId)
}

func (s tableBulkParam) uniqueConstraintName() string {
	hashRunes := []rune(s.getMD5Hash(strings.Join(s.uniqueConstraint, ",")))

	return fmt.Sprintf("%v_%v_uniq", s.tableName, string(hashRunes[0:8]))
}

func (s tableBulkParam) uniqueConstraintNew(dataVersionId uint) string {
	hashRunes := []rune(s.getMD5Hash(strings.Join(s.uniqueConstraint, ",")))

	return fmt.Sprintf("%v_%v_%v_uniq", s.tableName, string(hashRunes[0:8]), dataVersionId)
}

func (s tableBulkParam) getMD5Hash(text string) string {
	s.hasher.Reset()
	s.hasher.Write([]byte(text))
	return hex.EncodeToString(s.hasher.Sum(nil))
}

package sharding

import (
	"fmt"
	"hash/fnv"
	"reflect"
)

// HashShardingStrategy 基于 Hash 的分表策略
type HashShardingStrategy struct {
	baseTableName string
	shardingKey   string // 分表键字段名
	tableCount    int    // 分表数量
}

// NewHashShardingStrategy 创建 Hash 分表策略
// baseTableName: 基础表名（如 "users"）
// shardingKey: 分表键字段名（如 "user_id"）
// tableCount: 分表数量（如 4，将创建 users_0, users_1, users_2, users_3）
func NewHashShardingStrategy(baseTableName, shardingKey string, tableCount int) *HashShardingStrategy {
	if tableCount <= 0 {
		tableCount = 1
	}
	return &HashShardingStrategy{
		baseTableName: baseTableName,
		shardingKey:   shardingKey,
		tableCount:    tableCount,
	}
}

// GetTableName 根据分表键值获取实际表名
func (s *HashShardingStrategy) GetTableName(baseTableName string, shardingValue interface{}) string {
	hashValue := s.hashValue(shardingValue)
	tableIndex := hashValue % uint64(s.tableCount)
	return fmt.Sprintf("%s_%d", baseTableName, tableIndex)
}

// GetAllTableNames 获取所有分表名称
func (s *HashShardingStrategy) GetAllTableNames(baseTableName string) []string {
	tableNames := make([]string, s.tableCount)
	for i := 0; i < s.tableCount; i++ {
		tableNames[i] = fmt.Sprintf("%s_%d", baseTableName, i)
	}
	return tableNames
}

// GetShardingValue 从模型对象中提取分表键值
func (s *HashShardingStrategy) GetShardingValue(value interface{}) (interface{}, error) {
	return ExtractValue(value, s.shardingKey)
}

// GetBaseTableName 获取基础表名
func (s *HashShardingStrategy) GetBaseTableName() string {
	return s.baseTableName
}

// hashValue 计算值的 Hash
func (s *HashShardingStrategy) hashValue(value interface{}) uint64 {
	hash := fnv.New64a()
	
	// 根据不同类型计算 Hash
	switch v := value.(type) {
	case string:
		hash.Write([]byte(v))
	case int:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case int8:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case int16:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case int32:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case int64:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case uint:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case uint8:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case uint16:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case uint32:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	case uint64:
		hash.Write([]byte(fmt.Sprintf("%d", v)))
	default:
		// 尝试转换为字符串
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		hash.Write([]byte(fmt.Sprintf("%v", rv.Interface())))
	}
	
	return hash.Sum64()
}


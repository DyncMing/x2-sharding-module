package sharding

import (
	"fmt"
)

// CustomShardingFunc 自定义分表函数类型
// 参数：baseTableName - 基础表名，shardingValue - 分表键值
// 返回：实际表名
type CustomShardingFunc func(baseTableName string, shardingValue interface{}) string

// CustomShardingValueFunc 自定义分表值提取函数类型
// 参数：value - 模型对象
// 返回：分表键值，错误
type CustomShardingValueFunc func(value interface{}) (interface{}, error)

// CustomGetAllTablesFunc 自定义获取所有表名函数类型
// 参数：baseTableName - 基础表名
// 返回：所有分表名称列表
type CustomGetAllTablesFunc func(baseTableName string) []string

// CustomShardingStrategy 自定义分表策略
// 允许用户通过函数来自定义分表逻辑
type CustomShardingStrategy struct {
	baseTableName      string
	shardingKey        string
	getTableNameFunc   CustomShardingFunc
	getValueFunc       CustomShardingValueFunc
	getAllTablesFunc   CustomGetAllTablesFunc
}

// NewCustomShardingStrategy 创建自定义分表策略
// baseTableName: 基础表名
// shardingKey: 分表键字段名
// getTableNameFunc: 自定义表名生成函数
// getValueFunc: 自定义值提取函数（可选，如果为 nil 则使用默认实现）
// getAllTablesFunc: 自定义获取所有表名函数（可选，如果为 nil 则返回空列表）
func NewCustomShardingStrategy(
	baseTableName string,
	shardingKey string,
	getTableNameFunc CustomShardingFunc,
	getValueFunc CustomShardingValueFunc,
	getAllTablesFunc CustomGetAllTablesFunc,
) *CustomShardingStrategy {
	// 如果没有提供值提取函数，使用默认实现
	if getValueFunc == nil {
		getValueFunc = func(value interface{}) (interface{}, error) {
			return ExtractValue(value, shardingKey)
		}
	}

	// 如果没有提供获取所有表名函数，使用默认实现（返回空列表）
	if getAllTablesFunc == nil {
		getAllTablesFunc = func(baseTableName string) []string {
			return []string{baseTableName}
		}
	}

	return &CustomShardingStrategy{
		baseTableName:    baseTableName,
		shardingKey:      shardingKey,
		getTableNameFunc: getTableNameFunc,
		getValueFunc:     getValueFunc,
		getAllTablesFunc: getAllTablesFunc,
	}
}

// GetTableName 根据分表键值获取实际表名
func (s *CustomShardingStrategy) GetTableName(baseTableName string, shardingValue interface{}) string {
	return s.getTableNameFunc(baseTableName, shardingValue)
}

// GetAllTableNames 获取所有分表名称
func (s *CustomShardingStrategy) GetAllTableNames(baseTableName string) []string {
	return s.getAllTablesFunc(baseTableName)
}

// GetShardingValue 从模型对象中提取分表键值
func (s *CustomShardingStrategy) GetShardingValue(value interface{}) (interface{}, error) {
	return s.getValueFunc(value)
}

// GetBaseTableName 获取基础表名
func (s *CustomShardingStrategy) GetBaseTableName() string {
	return s.baseTableName
}

// RangeShardingStrategy 范围分表策略（示例：按 ID 范围分表）
// 例如：0-9999 在 table_0，10000-19999 在 table_1
type RangeShardingStrategy struct {
	baseTableName string
	shardingKey   string
	rangeSize     int64 // 每个分表的数据范围大小
	tableCount    int   // 分表数量
}

// NewRangeShardingStrategy 创建范围分表策略
func NewRangeShardingStrategy(baseTableName, shardingKey string, rangeSize int64, tableCount int) *RangeShardingStrategy {
	if rangeSize <= 0 {
		rangeSize = 10000 // 默认每个分表 10000 条数据
	}
	if tableCount <= 0 {
		tableCount = 1
	}
	return &RangeShardingStrategy{
		baseTableName: baseTableName,
		shardingKey:   shardingKey,
		rangeSize:     rangeSize,
		tableCount:    tableCount,
	}
}

// GetTableName 根据分表键值获取实际表名
func (s *RangeShardingStrategy) GetTableName(baseTableName string, shardingValue interface{}) string {
	// 将分表值转换为 int64
	var intValue int64
	switch v := shardingValue.(type) {
	case int:
		intValue = int64(v)
	case int32:
		intValue = int64(v)
	case int64:
		intValue = v
	case uint:
		intValue = int64(v)
	case uint32:
		intValue = int64(v)
	case uint64:
		intValue = int64(v)
	default:
		// 如果不是数字类型，使用 Hash 分表作为后备方案
		hashStrategy := NewHashShardingStrategy(baseTableName, s.shardingKey, s.tableCount)
		return hashStrategy.GetTableName(baseTableName, shardingValue)
	}

	// 计算分表索引
	tableIndex := int(intValue / s.rangeSize)
	
	// 限制在有效范围内
	if tableIndex >= s.tableCount {
		tableIndex = s.tableCount - 1
	}
	if tableIndex < 0 {
		tableIndex = 0
	}

	return fmt.Sprintf("%s_%d", baseTableName, tableIndex)
}

// GetAllTableNames 获取所有分表名称
func (s *RangeShardingStrategy) GetAllTableNames(baseTableName string) []string {
	tableNames := make([]string, s.tableCount)
	for i := 0; i < s.tableCount; i++ {
		tableNames[i] = fmt.Sprintf("%s_%d", baseTableName, i)
	}
	return tableNames
}

// GetShardingValue 从模型对象中提取分表键值
func (s *RangeShardingStrategy) GetShardingValue(value interface{}) (interface{}, error) {
	return ExtractValue(value, s.shardingKey)
}

// GetBaseTableName 获取基础表名
func (s *RangeShardingStrategy) GetBaseTableName() string {
	return s.baseTableName
}

// ModuloShardingStrategy 取模分表策略（另一种常见的分表方式）
// 例如：ID % 4 = 0 的在 table_0，ID % 4 = 1 的在 table_1
type ModuloShardingStrategy struct {
	baseTableName string
	shardingKey   string
	modulo        int // 取模数
}

// NewModuloShardingStrategy 创建取模分表策略
func NewModuloShardingStrategy(baseTableName, shardingKey string, modulo int) *ModuloShardingStrategy {
	if modulo <= 0 {
		modulo = 1
	}
	return &ModuloShardingStrategy{
		baseTableName: baseTableName,
		shardingKey:   shardingKey,
		modulo:        modulo,
	}
}

// GetTableName 根据分表键值获取实际表名
func (s *ModuloShardingStrategy) GetTableName(baseTableName string, shardingValue interface{}) string {
	// 将分表值转换为 int64
	var intValue int64
	switch v := shardingValue.(type) {
	case int:
		intValue = int64(v)
	case int32:
		intValue = int64(v)
	case int64:
		intValue = v
	case uint:
		intValue = int64(v)
	case uint32:
		intValue = int64(v)
	case uint64:
		intValue = int64(v)
	default:
		// 如果不是数字类型，使用 Hash 分表作为后备方案
		hashStrategy := NewHashShardingStrategy(baseTableName, s.shardingKey, s.modulo)
		return hashStrategy.GetTableName(baseTableName, shardingValue)
	}

	// 取模计算分表索引
	tableIndex := int(intValue) % s.modulo
	if tableIndex < 0 {
		tableIndex = -tableIndex % s.modulo
	}

	return fmt.Sprintf("%s_%d", baseTableName, tableIndex)
}

// GetAllTableNames 获取所有分表名称
func (s *ModuloShardingStrategy) GetAllTableNames(baseTableName string) []string {
	tableNames := make([]string, s.modulo)
	for i := 0; i < s.modulo; i++ {
		tableNames[i] = fmt.Sprintf("%s_%d", baseTableName, i)
	}
	return tableNames
}

// GetShardingValue 从模型对象中提取分表键值
func (s *ModuloShardingStrategy) GetShardingValue(value interface{}) (interface{}, error) {
	return ExtractValue(value, s.shardingKey)
}

// GetBaseTableName 获取基础表名
func (s *ModuloShardingStrategy) GetBaseTableName() string {
	return s.baseTableName
}


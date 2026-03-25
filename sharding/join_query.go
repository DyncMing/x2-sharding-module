package sharding

import (
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// JoinType JOIN 类型
type JoinType string

const (
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
)

// CrossTableJoin 跨表连接查询
// 支持两个分表的连接查询
func CrossTableJoin(
	db *gorm.DB,
	strategy1, strategy2 ShardingStrategy,
	joinType JoinType,
	onCondition string, // 例如: "users.id = orders.user_id"
	dest interface{},
	queryBuilder QueryBuilder,
) error {
	config := buildTwoTableJoinConfig(strategy1, strategy2, joinType, onCondition)
	return CrossTableMultiJoin(db, config, dest, queryBuilder)
}

func buildTwoTableJoinConfig(
	strategy1, strategy2 ShardingStrategy,
	joinType JoinType,
	onCondition string,
) MultiJoinConfig {
	return MultiJoinConfig{
		MainTable: JoinInfo{
			Strategy: strategy1,
			Alias:    strategy1.GetBaseTableName(),
		},
		JoinTables: []JoinInfo{
			{
				Strategy:    strategy2,
				JoinType:    joinType,
				OnCondition: onCondition,
				Alias:       strategy2.GetBaseTableName(),
			},
		},
	}
}

// CrossTableJoinCount 两个分表的连接计数
func CrossTableJoinCount(
	db *gorm.DB,
	strategy1, strategy2 ShardingStrategy,
	joinType JoinType,
	onCondition string,
	queryBuilder QueryBuilder,
) (int64, error) {
	config := buildTwoTableJoinConfig(strategy1, strategy2, joinType, onCondition)
	return CrossTableMultiJoinCount(db, config, queryBuilder)
}

// CrossTableJoinPaginate 两个分表的连接分页
func CrossTableJoinPaginate(
	db *gorm.DB,
	strategy1, strategy2 ShardingStrategy,
	joinType JoinType,
	onCondition string,
	dest interface{},
	page, pageSize int,
	queryBuilder QueryBuilder,
) (*Paginator, error) {
	config := buildTwoTableJoinConfig(strategy1, strategy2, joinType, onCondition)
	return CrossTableMultiJoinPaginate(db, config, dest, page, pageSize, queryBuilder)
}

// CrossTableJoinWithTimeRange 两个时间分表的连接查询（指定时间范围）
func CrossTableJoinWithTimeRange(
	db *gorm.DB,
	strategy1, strategy2 ShardingStrategy,
	joinType JoinType,
	onCondition string,
	dest interface{},
	queryBuilder QueryBuilder,
	startValue, endValue interface{},
) error {
	config := buildTwoTableJoinConfig(strategy1, strategy2, joinType, onCondition)
	applyTimeRangeToMultiJoinConfig(&config, startValue, endValue)
	return CrossTableMultiJoin(db, config, dest, queryBuilder)
}

// CrossTableJoinCountWithTimeRange 两个时间分表的连接计数（指定时间范围）
func CrossTableJoinCountWithTimeRange(
	db *gorm.DB,
	strategy1, strategy2 ShardingStrategy,
	joinType JoinType,
	onCondition string,
	queryBuilder QueryBuilder,
	startValue, endValue interface{},
) (int64, error) {
	config := buildTwoTableJoinConfig(strategy1, strategy2, joinType, onCondition)
	return CrossTableMultiJoinCountWithTimeRange(db, config, queryBuilder, startValue, endValue)
}

// CrossTableJoinPaginateWithTimeRange 两个时间分表的连接分页（指定时间范围）
func CrossTableJoinPaginateWithTimeRange(
	db *gorm.DB,
	strategy1, strategy2 ShardingStrategy,
	joinType JoinType,
	onCondition string,
	dest interface{},
	page, pageSize int,
	queryBuilder QueryBuilder,
	startValue, endValue interface{},
) (*Paginator, error) {
	config := buildTwoTableJoinConfig(strategy1, strategy2, joinType, onCondition)
	return CrossTableMultiJoinPaginateWithTimeRange(db, config, dest, page, pageSize, queryBuilder, startValue, endValue)
}

// CrossTableJoinOptimized 优化的跨表连接查询
// 根据 JOIN 条件，只连接相关的表对，而不是所有表的笛卡尔积
func CrossTableJoinOptimized(
	db *gorm.DB,
	strategy1, strategy2 ShardingStrategy,
	joinType JoinType,
	joinKey string, // JOIN 的键字段，用于确定哪些表需要连接
	dest interface{},
	queryBuilder QueryBuilder,
) error {
	// 这种方法需要根据实际的业务逻辑来确定哪些表需要连接
	// 例如：如果两个表都是基于 user_id 分表的，则 users_0 应该只连接 orders_0

	// 简化为对所有可能的表对进行连接
	return CrossTableJoin(db, strategy1, strategy2, joinType, joinKey, dest, queryBuilder)
}

// convertResults 将 map 结果转换为目标类型
func convertResults(results []map[string]interface{}, dest interface{}) error {
	if len(results) == 0 {
		return nil
	}

	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	destElem := destValue.Elem()
	if destElem.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	elemType := destElem.Type().Elem()

	for _, result := range results {
		elem := reflect.New(elemType).Elem()

		// 将 map 的字段映射到结构体
		if err := mapToStruct(result, elem); err != nil {
			continue // 跳过转换失败的行
		}

		destElem.Set(reflect.Append(destElem, elem))
	}

	return nil
}

// mapToStruct 将 map 转换为结构体
func mapToStruct(m map[string]interface{}, structValue reflect.Value) error {
	structType := structValue.Type()

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldValue := structValue.Field(i)

		// 获取字段名（考虑 gorm 和 json tag）
		fieldName := getFieldName(field)

		if value, ok := m[fieldName]; ok {
			if fieldValue.CanSet() {
				if value == nil {
					continue
				}
				val := reflect.ValueOf(value)
				if val.Type().AssignableTo(fieldValue.Type()) {
					fieldValue.Set(val)
				} else if val.Type().ConvertibleTo(fieldValue.Type()) {
					fieldValue.Set(val.Convert(fieldValue.Type()))
				}
			}
		}
	}

	return nil
}

// getFieldName 获取字段在数据库中的名称
func getFieldName(field reflect.StructField) string {
	// 优先使用 gorm tag
	if gormTag := field.Tag.Get("gorm"); gormTag != "" {
		// 解析 gorm tag，提取 column 名称
		if strings.Contains(gormTag, "column:") {
			parts := strings.Split(gormTag, "column:")
			if len(parts) > 1 {
				columnName := strings.Split(parts[1], ";")[0]
				columnName = strings.TrimSpace(columnName)
				if columnName != "" {
					return columnName
				}
			}
		}
	}

	// 其次使用 json tag
	if jsonTag := field.Tag.Get("json"); jsonTag != "" {
		return strings.Split(jsonTag, ",")[0]
	}

	// 最后使用字段名（转换为下划线命名）
	return toSnakeCase(field.Name)
}

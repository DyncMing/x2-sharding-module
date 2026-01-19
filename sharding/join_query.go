package sharding

import (
	"fmt"
	"reflect"
	"strings"
	"time"

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
	// 获取两个策略的所有表名
	tableNames1 := strategy1.GetAllTableNames(strategy1.GetBaseTableName())
	tableNames2 := strategy2.GetAllTableNames(strategy2.GetBaseTableName())

	// 如果是时间分表
	if timeStrategy1, ok := strategy1.(*TimeShardingStrategy); ok {
		endTime := time.Now()
		startTime := endTime.AddDate(-1, 0, 0)
		tableNames1 = timeStrategy1.GetAllTableNamesInRange(strategy1.GetBaseTableName(), startTime, endTime)
	}

	if timeStrategy2, ok := strategy2.(*TimeShardingStrategy); ok {
		endTime := time.Now()
		startTime := endTime.AddDate(-1, 0, 0)
		tableNames2 = timeStrategy2.GetAllTableNamesInRange(strategy2.GetBaseTableName(), startTime, endTime)
	}

	// 对于 Hash 分表，通常每个表之间都需要连接
	// 这里采用笛卡尔积的方式，但实际上应该根据 join 条件进行优化
	var allResults []map[string]interface{}

	for _, table1 := range tableNames1 {
		for _, table2 := range tableNames2 {
			query := db.Table(table1)
			
			// 构建 JOIN 语句
			joinSQL := fmt.Sprintf("%s JOIN %s ON %s", joinType, table2, onCondition)
			query = query.Joins(joinSQL)

			if queryBuilder != nil {
				query = queryBuilder(query)
			}

			var results []map[string]interface{}
			if err := query.Find(&results).Error; err != nil {
				if !strings.Contains(err.Error(), "doesn't exist") {
					return err
				}
				continue
			}

			allResults = append(allResults, results...)
		}
	}

	// 将结果转换为目标类型
	return convertResults(allResults, dest)
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
				val := reflect.ValueOf(value)
				if val.Type().AssignableTo(fieldValue.Type()) {
					fieldValue.Set(val)
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


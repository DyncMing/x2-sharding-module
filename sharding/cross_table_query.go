package sharding

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"gorm.io/gorm"
)

// QueryBuilder 查询构建器函数类型
type QueryBuilder func(*gorm.DB) *gorm.DB

// CrossTableQuery 跨表查询，在所有分表中执行查询并合并结果
func CrossTableQuery(db *gorm.DB, strategy ShardingStrategy, dest interface{}, queryBuilder QueryBuilder) error {
	return CrossTableQueryWithTimeRange(db, strategy, dest, queryBuilder, nil, nil)
}

// CrossTableQueryWithTimeRange 跨表查询（支持指定时间范围）
// startValue 和 endValue 可以是 time.Time, int64(时间戳), string(日期/时间戳字符串) 等
func CrossTableQueryWithTimeRange(
	db *gorm.DB,
	strategy ShardingStrategy,
	dest interface{},
	queryBuilder QueryBuilder,
	startValue, endValue interface{},
) error {
	tableNames := strategy.GetAllTableNames(strategy.GetBaseTableName())

	// 如果是时间分表，需要获取时间范围
	if timeStrategy, ok := strategy.(*TimeShardingStrategy); ok {
		if startValue != nil && endValue != nil {
			// 使用指定的时间范围
			tableNames = timeStrategy.GetAllTableNamesInRangeWithValues(
				strategy.GetBaseTableName(),
				startValue,
				endValue,
			)
		} else {
			// 对于时间分表，默认查询最近一年的数据
			endTime := time.Now()
			startTime := endTime.AddDate(-1, 0, 0)
			tableNames = timeStrategy.GetAllTableNamesInRange(strategy.GetBaseTableName(), startTime, endTime)
		}
	}

	if len(tableNames) == 0 {
		return fmt.Errorf("no tables found")
	}

	// 使用反射获取 dest 的类型
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	destElem := destValue.Elem()
	if destElem.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	elemType := destElem.Type().Elem()

	// 对每个分表执行查询并合并结果
	for _, tableName := range tableNames {
		query := db.Table(tableName)
		if queryBuilder != nil {
			query = queryBuilder(query)
		}

		// 创建临时切片来存储当前表的查询结果
		tableResults := reflect.New(reflect.SliceOf(elemType)).Interface()

		if err := query.Find(tableResults).Error; err != nil {
			// 如果表不存在，跳过（某些分表可能尚未创建）
			errMsg := strings.ToLower(err.Error())
			if strings.Contains(errMsg, "doesn't exist") ||
				strings.Contains(errMsg, "unknown table") ||
				strings.Contains(errMsg, "table") && strings.Contains(errMsg, "not found") {
				continue
			}
			return err
		}

		// 将当前表的结果追加到总结果中
		tableResultsValue := reflect.ValueOf(tableResults).Elem()
		destElem.Set(reflect.AppendSlice(destElem, tableResultsValue))
	}

	return nil
}

// CrossTableQueryUnion 使用 UNION ALL 进行跨表查询（更高效）
func CrossTableQueryUnion(db *gorm.DB, strategy ShardingStrategy, dest interface{}, queryBuilder QueryBuilder) error {
	tableNames := strategy.GetAllTableNames(strategy.GetBaseTableName())

	// 如果是时间分表，需要获取时间范围
	if timeStrategy, ok := strategy.(*TimeShardingStrategy); ok {
		endTime := time.Now()
		startTime := endTime.AddDate(-1, 0, 0)
		tableNames = timeStrategy.GetAllTableNamesInRange(strategy.GetBaseTableName(), startTime, endTime)
	}

	if len(tableNames) == 0 {
		return fmt.Errorf("no tables found")
	}

	// 构建 UNION ALL 查询
	queries := make([]string, 0, len(tableNames))
	args := make([]interface{}, 0)

	for _, tableName := range tableNames {
		query := db.Table(tableName)
		if queryBuilder != nil {
			query = queryBuilder(query)
		}

		// 获取 SQL 和参数
		sql := query.ToSQL(func(query *gorm.DB) *gorm.DB {
			return query
		})
		queries = append(queries, fmt.Sprintf("(%s)", sql))
	}

	// 组合 UNION ALL 查询
	unionSQL := strings.Join(queries, " UNION ALL ")

	return db.Raw(unionSQL, args...).Find(dest).Error
}

// CrossTableCount 跨表计数
func CrossTableCount(db *gorm.DB, strategy ShardingStrategy, queryBuilder QueryBuilder) (int64, error) {
	var totalCount int64
	tableNames := strategy.GetAllTableNames(strategy.GetBaseTableName())

	// 如果是时间分表
	if timeStrategy, ok := strategy.(*TimeShardingStrategy); ok {
		endTime := time.Now()
		startTime := endTime.AddDate(-1, 0, 0)
		tableNames = timeStrategy.GetAllTableNamesInRange(strategy.GetBaseTableName(), startTime, endTime)
	}

	for _, tableName := range tableNames {
		query := db.Table(tableName)
		if queryBuilder != nil {
			query = queryBuilder(query)
		}

		var count int64
		if err := query.Count(&count).Error; err != nil {
			errMsg := strings.ToLower(err.Error())
			if strings.Contains(errMsg, "doesn't exist") ||
				strings.Contains(errMsg, "unknown table") ||
				strings.Contains(errMsg, "table") && strings.Contains(errMsg, "not found") {
				continue
			}
			return 0, err
		}
		totalCount += count
	}

	return totalCount, nil
}

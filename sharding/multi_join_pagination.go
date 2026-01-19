package sharding

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

// CrossTableMultiJoinCount 多表连接查询的计数（自动去重）
// 为了确保计数准确，先查询所有结果，然后去重计数
func CrossTableMultiJoinCount(
	db *gorm.DB,
	config MultiJoinConfig,
	queryBuilder QueryBuilder,
) (int64, error) {
	// 为了准确计数并去重，先查询所有结果，然后去重计数
	// 这样可以确保计数和查询结果一致
	var tempResults []map[string]interface{}

	// 获取主表的所有分表名称
	mainTableNames := getTableNamesWithTimeRange(config.MainTable.Strategy, config.MainTable.Strategy.GetBaseTableName(), config.TimeRanges)

	// 获取所有连接表的分表名称
	joinTableNamesList := make([][]string, len(config.JoinTables))
	for i, joinInfo := range config.JoinTables {
		joinTableNamesList[i] = getTableNamesWithTimeRange(joinInfo.Strategy, joinInfo.Strategy.GetBaseTableName(), config.TimeRanges)
	}

	// 构建表名到别名的映射
	mainBaseName := config.MainTable.Strategy.GetBaseTableName()
	mainAlias := config.MainTable.Alias
	if mainAlias == "" {
		mainAlias = mainBaseName
	}

	joinAliases := make([]string, len(config.JoinTables))
	for i, joinInfo := range config.JoinTables {
		if joinInfo.Alias != "" {
			joinAliases[i] = joinInfo.Alias
		} else {
			joinAliases[i] = joinInfo.Strategy.GetBaseTableName()
		}
	}

	// 对所有可能的表组合进行连接查询
	tableCombinations := generateTableCombinations(mainTableNames, joinTableNamesList)

	for _, combination := range tableCombinations {
		mainTableName := combination[0]
		
		// 为主表设置别名
		query := db.Table(fmt.Sprintf("%s AS %s", mainTableName, mainAlias))

		// 依次添加 JOIN
		for i := 0; i < len(config.JoinTables); i++ {
			joinInfo := config.JoinTables[i]
			joinTableName := combination[i+1]
			joinAlias := joinAliases[i]

			// 替换 ON 条件中的基础表名为别名
			onCondition := replaceTableNamesInCondition(joinInfo.OnCondition, mainBaseName, mainAlias, joinInfo.Strategy.GetBaseTableName(), joinAlias)

			joinSQL := fmt.Sprintf("%s JOIN %s AS %s ON %s", joinInfo.JoinType, joinTableName, joinAlias, onCondition)
			query = query.Joins(joinSQL)
		}

		// 应用查询构建器
		if queryBuilder != nil {
			query = queryBuilder(query)
		}

		// 执行查询（获取数据用于去重计数）
		var results []map[string]interface{}
		if err := query.Find(&results).Error; err != nil {
			errMsg := strings.ToLower(err.Error())
			if strings.Contains(errMsg, "doesn't exist") ||
				strings.Contains(errMsg, "unknown table") ||
				strings.Contains(errMsg, "table") && strings.Contains(errMsg, "not found") ||
				strings.Contains(errMsg, "unknown column") {
				continue // 表不存在或列不存在，跳过
			}
			return 0, fmt.Errorf("count error on tables %v: %w", combination, err)
		}

		tempResults = append(tempResults, results...)
	}

	// 对结果进行去重，然后返回去重后的数量
	deduplicateFields := config.DeduplicateFields
	if len(deduplicateFields) == 0 {
		deduplicateFields = GetDefaultDeduplicateFields()
	}
	deduplicatedResults := deduplicateResults(tempResults, deduplicateFields)

	return int64(len(deduplicatedResults)), nil
}

// CrossTableMultiJoinPaginate 多表连接查询的分页（自动去重）
func CrossTableMultiJoinPaginate(
	db *gorm.DB,
	config MultiJoinConfig,
	dest interface{},
	page, pageSize int,
	queryBuilder QueryBuilder,
) (*Paginator, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 先获取总数（已自动去重）
	total, err := CrossTableMultiJoinCount(db, config, queryBuilder)
	if err != nil {
		return nil, err
	}

	// 计算总页数
	totalPages := int(total) / pageSize
	if int(total)%pageSize > 0 {
		totalPages++
	}

	// 执行多表连接查询（获取所有数据，已自动去重）
	err = CrossTableMultiJoin(db, config, dest, queryBuilder)
	if err != nil {
		return nil, err
	}

	// 手动分页（因为数据已经在内存中）
	// 注意：这种方式对于大数据量可能不够高效
	paginatedData := paginateSlice(dest, page, pageSize)

	return &Paginator{
		Page:       page,
		PageSize:   pageSize,
		Total:      total,
		TotalPages: totalPages,
		Data:       paginatedData,
	}, nil
}

// CrossTableMultiJoinPaginateOptimized 优化的多表连接查询分页（使用优化的连接）
// 只连接相关的表组合，而不是所有可能的组合
func CrossTableMultiJoinPaginateOptimized(
	db *gorm.DB,
	config MultiJoinConfig,
	joinKeys map[string]interface{}, // 连接键值映射，用于确定需要连接的表
	dest interface{},
	page, pageSize int,
	queryBuilder QueryBuilder,
) (*Paginator, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 构建表名到别名的映射
	mainBaseName := config.MainTable.Strategy.GetBaseTableName()
	mainAlias := config.MainTable.Alias
	if mainAlias == "" {
		mainAlias = mainBaseName
	}

	// 获取主表的表名（分表名）
	mainTableName := getTableNameByKey(config.MainTable.Strategy, mainBaseName, joinKeys)
	
	// 为主表设置别名（使用基础表名作为别名，这样在 WHERE 条件中可以使用 users.user_id）
	query := db.Table(fmt.Sprintf("%s AS %s", mainTableName, mainAlias))

	// 获取所有连接表的表名和别名
	joinTableNames := make([]string, len(config.JoinTables))
	joinAliases := make([]string, len(config.JoinTables))
	for i, joinInfo := range config.JoinTables {
		joinTableNames[i] = getTableNameByKey(joinInfo.Strategy, joinInfo.Strategy.GetBaseTableName(), joinKeys)
		if joinInfo.Alias != "" {
			joinAliases[i] = joinInfo.Alias
		} else {
			joinAliases[i] = joinInfo.Strategy.GetBaseTableName()
		}
	}

	// 添加 JOIN
	for i, joinInfo := range config.JoinTables {
		joinAlias := joinAliases[i]
		
		// 替换 ON 条件中的基础表名为别名
		onCondition := replaceTableNamesInCondition(
			joinInfo.OnCondition,
			mainBaseName, mainAlias,
			joinInfo.Strategy.GetBaseTableName(), joinAlias,
		)

		joinSQL := fmt.Sprintf("%s JOIN %s AS %s ON %s", joinInfo.JoinType, joinTableNames[i], joinAlias, onCondition)
		query = query.Joins(joinSQL)
	}

	// 应用查询构建器
	// 注意：在 queryBuilder 中应该使用别名（基础表名），如 users.user_id
	if queryBuilder != nil {
		query = queryBuilder(query)
	}

	// 先获取总数
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, fmt.Errorf("count error: %w", err)
	}

	// 计算总页数
	totalPages := int(total) / pageSize
	if int(total)%pageSize > 0 {
		totalPages++
	}

	// 应用分页
	offset := (page - 1) * pageSize
	query = query.Offset(offset).Limit(pageSize)

	// 执行查询
	if err := query.Find(dest).Error; err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	return &Paginator{
		Page:       page,
		PageSize:   pageSize,
		Total:      total,
		TotalPages: totalPages,
		Data:       dest,
	}, nil
}

// CrossTableMultiJoinCountWithTimeRange 多表连接查询的计数（支持时间范围）
func CrossTableMultiJoinCountWithTimeRange(
	db *gorm.DB,
	config MultiJoinConfig,
	queryBuilder QueryBuilder,
	startValue, endValue interface{}, // 时间范围值（支持多种类型）
) (int64, error) {
	// 如果是时间分表，需要更新 TimeRanges
	if startValue != nil && endValue != nil {
		// 更新配置中的时间范围
		if config.TimeRanges == nil {
			config.TimeRanges = make(map[string]TimeRange)
		}

		// 转换时间值
		var startTime, endTime time.Time
		
		// 转换开始时间
		if st, ok := startValue.(time.Time); ok {
			startTime = st
		} else {
			// 尝试通过策略获取表名来推断时间
			// 使用 GetTableName 方法间接获取时间
			startTime = convertValueToTime(startValue)
		}

		// 转换结束时间
		if et, ok := endValue.(time.Time); ok {
			endTime = et
		} else {
			endTime = convertValueToTime(endValue)
		}

		// 为所有时间分表设置时间范围
		baseTableName := config.MainTable.Strategy.GetBaseTableName()
		config.TimeRanges[baseTableName] = TimeRange{
			StartTime: startTime,
			EndTime:   endTime,
		}

		for _, joinInfo := range config.JoinTables {
			if _, ok := joinInfo.Strategy.(*TimeShardingStrategy); ok {
				joinBaseName := joinInfo.Strategy.GetBaseTableName()
				config.TimeRanges[joinBaseName] = TimeRange{
					StartTime: startTime,
					EndTime:   endTime,
				}
			}
		}
	}

	return CrossTableMultiJoinCount(db, config, queryBuilder)
}

// CrossTableMultiJoinPaginateWithTimeRange 多表连接查询的分页（支持时间范围）
func CrossTableMultiJoinPaginateWithTimeRange(
	db *gorm.DB,
	config MultiJoinConfig,
	dest interface{},
	page, pageSize int,
	queryBuilder QueryBuilder,
	startValue, endValue interface{}, // 时间范围值（支持多种类型）
) (*Paginator, error) {
	// 如果是时间分表，需要更新 TimeRanges
	if startValue != nil && endValue != nil {
		if config.TimeRanges == nil {
			config.TimeRanges = make(map[string]TimeRange)
		}

		// 转换时间值
		var startTime, endTime time.Time
		
		// 转换开始时间
		if st, ok := startValue.(time.Time); ok {
			startTime = st
		} else {
			startTime = convertValueToTime(startValue)
		}

		// 转换结束时间
		if et, ok := endValue.(time.Time); ok {
			endTime = et
		} else {
			endTime = convertValueToTime(endValue)
		}

		// 为所有时间分表设置时间范围
		baseTableName := config.MainTable.Strategy.GetBaseTableName()
		config.TimeRanges[baseTableName] = TimeRange{
			StartTime: startTime,
			EndTime:   endTime,
		}

		for _, joinInfo := range config.JoinTables {
			if _, ok := joinInfo.Strategy.(*TimeShardingStrategy); ok {
				joinBaseName := joinInfo.Strategy.GetBaseTableName()
				config.TimeRanges[joinBaseName] = TimeRange{
					StartTime: startTime,
					EndTime:   endTime,
				}
			}
		}
	}

	return CrossTableMultiJoinPaginate(db, config, dest, page, pageSize, queryBuilder)
}

// convertValueToTime 将各种类型的时间值转换为 time.Time（辅助函数）
func convertValueToTime(value interface{}) time.Time {
	if value == nil {
		return time.Now()
	}

	switch v := value.(type) {
	case time.Time:
		return v
	case *time.Time:
		if v != nil {
			return *v
		}
		return time.Now()
	case int:
		return time.Unix(int64(v), 0)
	case int32:
		return time.Unix(int64(v), 0)
	case int64:
		// 判断是秒还是毫秒
		if v > 1e10 {
			return time.Unix(v/1000, (v%1000)*1e6)
		}
		return time.Unix(v, 0)
	case uint:
		return time.Unix(int64(v), 0)
	case uint32:
		return time.Unix(int64(v), 0)
	case uint64:
		if v > 1e10 {
			return time.Unix(int64(v/1000), int64((v%1000)*1e6))
		}
		return time.Unix(int64(v), 0)
	case string:
		return parseStringTimeInMultiJoin(v)
	default:
		// 尝试通过反射获取值
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				return time.Now()
			}
			rv = rv.Elem()
		}
		if rv.CanInt() {
			timestamp := rv.Int()
			if timestamp > 1e10 {
				return time.Unix(timestamp/1000, (timestamp%1000)*1e6)
			}
			return time.Unix(timestamp, 0)
		}
		if rv.CanUint() {
			timestamp := rv.Uint()
			if timestamp > 1e10 {
				return time.Unix(int64(timestamp/1000), int64((timestamp%1000)*1e6))
			}
			return time.Unix(int64(timestamp), 0)
		}
		return time.Now()
	}
}

// parseStringTimeInMultiJoin 解析字符串时间（辅助函数）
func parseStringTimeInMultiJoin(str string) time.Time {
	if str == "" {
		return time.Now()
	}

	// 尝试多种时间格式
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02",
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, str); err == nil {
			return t
		}
	}

	// 尝试作为 Unix 时间戳字符串解析
	if timestamp, err := strconv.ParseInt(str, 10, 64); err == nil {
		if timestamp > 1e10 {
			return time.Unix(timestamp/1000, (timestamp%1000)*1e6)
		}
		return time.Unix(timestamp, 0)
	}

	return time.Now()
}


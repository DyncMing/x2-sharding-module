package sharding

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// JoinInfo 连接信息
type JoinInfo struct {
	Strategy    ShardingStrategy // 分表策略
	JoinType    JoinType         // JOIN 类型
	OnCondition string           // ON 条件，例如: "users.id = orders.user_id"
	Alias       string           // 表别名（可选）
}

// TimeRange 时间范围（用于时间分表）
type TimeRange struct {
	StartTime time.Time
	EndTime   time.Time
}

// MultiJoinConfig 多表连接配置
type MultiJoinConfig struct {
	MainTable  JoinInfo              // 主表
	JoinTables []JoinInfo            // 需要连接的表列表
	TimeRanges map[string]TimeRange  // 时间分表的时间范围（可选）
	// DeduplicateFields 去重字段配置（可选）
	// 如果不设置，将使用默认的去重字段配置
	// 字段组合按优先级顺序，从最精确到最通用
	// 例如：[][]string{{"id"}, {"user_id", "order_id"}, {"user_id"}}
	DeduplicateFields [][]string
}

// GetDefaultDeduplicateFields 获取默认的去重字段配置
func GetDefaultDeduplicateFields() [][]string {
	return [][]string{
		{"id"},                                    // 单一主键
		{"user_id", "order_id", "payment_id"},     // 用户、订单、支付组合（最完整）
		{"user_id", "order_id"},                   // 用户和订单组合
		{"order_id", "payment_id"},                // 订单和支付组合
		{"user_id"},                               // 单一用户ID
		{"order_id"},                              // 单一订单ID
		{"payment_id"},                            // 单一支付ID
		{"log_id"},                                // 日志ID
		{"product_id"},                            // 商品ID
	}
}

// CrossTableMultiJoin 多表跨表连接查询
// 支持 3 个及以上分表的连接查询
func CrossTableMultiJoin(
	db *gorm.DB,
	config MultiJoinConfig,
	dest interface{},
	queryBuilder QueryBuilder,
) error {
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
		mainAlias = mainBaseName // 默认使用基础表名作为别名
	}

	// 为连接表构建别名映射
	joinAliases := make([]string, len(config.JoinTables))
	for i, joinInfo := range config.JoinTables {
		if joinInfo.Alias != "" {
			joinAliases[i] = joinInfo.Alias
		} else {
			joinAliases[i] = joinInfo.Strategy.GetBaseTableName() // 默认使用基础表名作为别名
		}
	}

	var allResults []map[string]interface{}

	// 对所有可能的表组合进行连接查询
	tableCombinations := generateTableCombinations(mainTableNames, joinTableNamesList)

	for _, combination := range tableCombinations {
		mainTableName := combination[0]
		
		// 为主表设置别名（使用基础表名作为别名，这样在 WHERE 条件中可以使用 users.user_id）
		query := db.Table(fmt.Sprintf("%s AS %s", mainTableName, mainAlias))

		// 依次添加 JOIN
		for i := 0; i < len(config.JoinTables); i++ {
			joinInfo := config.JoinTables[i]
			joinTableName := combination[i+1] // 连接表名
			joinAlias := joinAliases[i]

			// 替换 ON 条件中的基础表名为别名
			onCondition := replaceTableNamesInCondition(
				joinInfo.OnCondition, 
				mainBaseName, mainAlias, 
				joinInfo.Strategy.GetBaseTableName(), joinAlias,
			)

			joinSQL := fmt.Sprintf("%s JOIN %s AS %s ON %s", joinInfo.JoinType, joinTableName, joinAlias, onCondition)
			query = query.Joins(joinSQL)
		}

		// 应用查询构建器
		// 注意：在 queryBuilder 中应该使用别名（基础表名），如 users.user_id，而不是 users_0.user_id
		if queryBuilder != nil {
			query = queryBuilder(query)
		}

		// 执行查询
		var results []map[string]interface{}
		if err := query.Find(&results).Error; err != nil {
			errMsg := strings.ToLower(err.Error())
			if strings.Contains(errMsg, "doesn't exist") ||
				strings.Contains(errMsg, "unknown table") ||
				strings.Contains(errMsg, "table") && strings.Contains(errMsg, "not found") ||
				strings.Contains(errMsg, "unknown column") {
				continue // 表不存在或列不存在，跳过
			}
			return fmt.Errorf("query error on tables %v: %w", combination, err)
		}

		allResults = append(allResults, results...)
	}

	// 对结果进行去重
	deduplicateFields := config.DeduplicateFields
	if len(deduplicateFields) == 0 {
		deduplicateFields = GetDefaultDeduplicateFields()
	}
	allResults = deduplicateResults(allResults, deduplicateFields)

	// 将结果转换为目标类型
	return convertResults(allResults, dest)
}

// generateTableCombinations 生成所有可能的表组合
// 例如：主表有 [users_0, users_1]，连接表有 [orders_0, orders_1]
// 返回：[[users_0, orders_0], [users_0, orders_1], [users_1, orders_0], [users_1, orders_1]]
func generateTableCombinations(mainTableNames []string, joinTableNamesList [][]string) [][]string {
	if len(joinTableNamesList) == 0 {
		// 只有主表，没有连接表
		combinations := make([][]string, len(mainTableNames))
		for i, name := range mainTableNames {
			combinations[i] = []string{name}
		}
		return combinations
	}

	// 使用递归生成所有组合
	return generateCombinationsRecursive(mainTableNames, joinTableNamesList, 0, []string{})
}

// generateCombinationsRecursive 递归生成表组合
func generateCombinationsRecursive(mainTableNames []string, joinTableNamesList [][]string, joinIndex int, current []string) [][]string {
	var result [][]string

	// 如果是第一次调用，遍历主表
	if joinIndex == 0 {
		for _, mainTable := range mainTableNames {
			newCurrent := append([]string{}, mainTable)
			subResults := generateCombinationsRecursive(mainTableNames, joinTableNamesList, joinIndex+1, newCurrent)
			result = append(result, subResults...)
		}
		return result
	}

	// 如果已经处理完所有连接表
	if joinIndex > len(joinTableNamesList) {
		return [][]string{current}
	}

	// 处理当前连接表
	joinTableNames := joinTableNamesList[joinIndex-1]
	for _, joinTable := range joinTableNames {
		newCurrent := append([]string{}, current...)
		newCurrent = append(newCurrent, joinTable)
		subResults := generateCombinationsRecursive(mainTableNames, joinTableNamesList, joinIndex+1, newCurrent)
		result = append(result, subResults...)
	}

	return result
}

// getTableNamesWithTimeRange 获取表名列表（考虑时间范围）
func getTableNamesWithTimeRange(strategy ShardingStrategy, baseTableName string, timeRanges map[string]TimeRange) []string {
	// 检查是否是时间分表
	timeStrategy, ok := strategy.(*TimeShardingStrategy)
	if !ok {
		// 非时间分表，直接获取所有表名
		return strategy.GetAllTableNames(baseTableName)
	}

	// 时间分表，需要检查是否有指定的时间范围
	if timeRange, hasRange := timeRanges[baseTableName]; hasRange {
		return timeStrategy.GetAllTableNamesInRange(baseTableName, timeRange.StartTime, timeRange.EndTime)
	}

	// 没有指定时间范围，使用默认（最近一年）
	endTime := time.Now()
	startTime := endTime.AddDate(-1, 0, 0)
	return timeStrategy.GetAllTableNamesInRange(baseTableName, startTime, endTime)
}

// CrossTableMultiJoinOptimized 优化的多表连接查询
// 只连接相关的表组合，而不是所有可能的组合
// 这需要根据 JOIN 条件中的分表键来确定哪些表需要连接
func CrossTableMultiJoinOptimized(
	db *gorm.DB,
	config MultiJoinConfig,
	joinKeys map[string]interface{}, // 连接键值映射，用于确定需要连接的表
	dest interface{},
	queryBuilder QueryBuilder,
) error {
	// 根据连接键值，确定每个表应该使用哪个分表
	// 例如：如果 joinKeys 包含 user_id=123，且所有表都基于 user_id 分表
	// 那么只需要连接 users_1, orders_1, payments_1 等相同索引的分表

	mainBaseName := config.MainTable.Strategy.GetBaseTableName()
	mainAlias := config.MainTable.Alias
	if mainAlias == "" {
		mainAlias = mainBaseName
	}

	// 获取主表的表名
	mainTableName := getTableNameByKey(config.MainTable.Strategy, mainBaseName, joinKeys)

	// 获取所有连接表的表名
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

	// 构建查询（使用别名）
	query := db.Table(fmt.Sprintf("%s AS %s", mainTableName, mainAlias))

	// 添加 JOIN
	for i, joinInfo := range config.JoinTables {
		joinAlias := joinAliases[i]
		
		// 替换 ON 条件中的表名
		onCondition := replaceTableNamesInCondition(
			joinInfo.OnCondition,
			mainBaseName, mainAlias,
			joinInfo.Strategy.GetBaseTableName(), joinAlias,
		)

		joinSQL := fmt.Sprintf("%s JOIN %s AS %s ON %s", joinInfo.JoinType, joinTableNames[i], joinAlias, onCondition)
		query = query.Joins(joinSQL)
	}

	// 应用查询构建器
	if queryBuilder != nil {
		query = queryBuilder(query)
	}

	// 执行查询
	return query.Find(dest).Error
}

// getTableNameByKey 根据连接键值获取表名
func getTableNameByKey(strategy ShardingStrategy, baseTableName string, joinKeys map[string]interface{}) string {
	// 尝试从 joinKeys 中提取分表键值
	// 遍历所有可能的键值，使用第一个非空值
	for _, value := range joinKeys {
		if value != nil {
			return strategy.GetTableName(baseTableName, value)
		}
	}

	// 如果没有找到匹配的键，返回基础表名
	return baseTableName
}

// replaceTableNamesInCondition 替换条件中的基础表名为别名
func replaceTableNamesInCondition(condition string, mainBaseName, mainAlias, joinBaseName, joinAlias string) string {
	result := condition
	
	// 替换主表名
	if mainBaseName != mainAlias {
		result = strings.ReplaceAll(result, mainBaseName+".", mainAlias+".")
	}
	
	// 替换连接表名
	if joinBaseName != joinAlias {
		result = strings.ReplaceAll(result, joinBaseName+".", joinAlias+".")
	}
	
	return result
}

// generateResultKey 生成结果的唯一键（用于去重）
// keyFieldGroups 是按优先级排序的字段组合列表
func generateResultKey(result map[string]interface{}, keyFieldGroups [][]string) string {
	// 尝试使用字段组合
	for _, fields := range keyFieldGroups {
		keyParts := make([]string, 0, len(fields))
		allFound := true
		
		for _, field := range fields {
			if val, ok := result[field]; ok && val != nil {
				strVal := fmt.Sprintf("%v", val)
				if strVal != "" && strVal != "<nil>" {
					keyParts = append(keyParts, fmt.Sprintf("%s:%v", field, val))
				} else {
					allFound = false
					break
				}
			} else {
				allFound = false
				break
			}
		}
		
		if allFound && len(keyParts) > 0 {
			return strings.Join(keyParts, "|")
		}
	}
	
	// 如果没有找到常见唯一字段组合，使用所有非 nil 字段的组合
	keyParts := make([]string, 0)
	for k, v := range result {
		if v != nil {
			strVal := fmt.Sprintf("%v", v)
			if strVal != "<nil>" && strVal != "" {
				keyParts = append(keyParts, fmt.Sprintf("%s:%v", k, v))
			}
		}
	}
	
	if len(keyParts) == 0 {
		// 如果没有找到任何有效字段，使用空字符串（可能导致重复，但比崩溃好）
		return ""
	}
	
	return strings.Join(keyParts, "|")
}

// deduplicateResults 对结果进行去重
// keyFieldGroups 是按优先级排序的字段组合列表，用于生成唯一键
func deduplicateResults(results []map[string]interface{}, keyFieldGroups [][]string) []map[string]interface{} {
	if len(results) == 0 {
		return results
	}
	
	seenKeys := make(map[string]bool)
	deduplicated := make([]map[string]interface{}, 0, len(results))
	
	for _, result := range results {
		key := generateResultKey(result, keyFieldGroups)
		if !seenKeys[key] {
			seenKeys[key] = true
			deduplicated = append(deduplicated, result)
		}
	}
	
	return deduplicated
}


package sharding

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// AutoMigrateOptions 自动迁移选项
type AutoMigrateOptions struct {
	SkipIfExists bool             // 如果表已存在则跳过
	TimeRange    *AutoMigrateTimeRange // 时间分表的时间范围（可选）
}

// AutoMigrateTimeRange 自动迁移的时间范围
type AutoMigrateTimeRange struct {
	StartTime time.Time
	EndTime   time.Time
}

// AutoMigrate 自动创建所有分表（基于 GORM AutoMigrate）
// 适用于 Hash 分表、范围分表、取模分表等固定分表数量的策略
func AutoMigrate(db *gorm.DB, strategy ShardingStrategy, model interface{}, options ...AutoMigrateOptions) error {
	baseTableName := strategy.GetBaseTableName()
	tableNames := strategy.GetAllTableNames(baseTableName)

	// 如果没有表名，可能是时间分表
	if len(tableNames) == 0 || (len(tableNames) == 1 && tableNames[0] == baseTableName) {
		// 尝试时间分表
		if timeStrategy, ok := strategy.(*TimeShardingStrategy); ok {
			return AutoMigrateTimeSharding(db, timeStrategy, model, options...)
		}
		return fmt.Errorf("no tables to migrate for strategy %s", baseTableName)
	}

	skipIfExists := false
	if len(options) > 0 && options[0].SkipIfExists {
		skipIfExists = true
	}

	// 创建所有分表
	for _, tableName := range tableNames {
		if err := migrateTable(db, tableName, model, skipIfExists); err != nil {
			return fmt.Errorf("failed to migrate table %s: %w", tableName, err)
		}
	}

	return nil
}

// AutoMigrateTimeSharding 自动创建时间分表
func AutoMigrateTimeSharding(db *gorm.DB, strategy *TimeShardingStrategy, model interface{}, options ...AutoMigrateOptions) error {
	baseTableName := strategy.GetBaseTableName()
	
	var timeRange *AutoMigrateTimeRange
	skipIfExists := false

	if len(options) > 0 {
		skipIfExists = options[0].SkipIfExists
		if options[0].TimeRange != nil {
			timeRange = options[0].TimeRange
		}
	}

	// 如果没有指定时间范围，使用默认范围（最近一年）
	if timeRange == nil {
		endTime := time.Now()
		startTime := endTime.AddDate(-1, 0, 0)
		timeRange = &AutoMigrateTimeRange{
			StartTime: startTime,
			EndTime:   endTime,
		}
	}

	tableNames := strategy.GetAllTableNamesInRange(baseTableName, timeRange.StartTime, timeRange.EndTime)

	for _, tableName := range tableNames {
		if err := migrateTable(db, tableName, model, skipIfExists); err != nil {
			return fmt.Errorf("failed to migrate table %s: %w", tableName, err)
		}
	}

	return nil
}

// migrateTable 迁移单个表
func migrateTable(db *gorm.DB, tableName string, model interface{}, skipIfExists bool) error {
	// 检查表是否存在
	if skipIfExists {
		if tableExists(db, tableName) {
			return nil // 表已存在，跳过
		}
	}

	// 使用 GORM 的 Table 方法指定表名进行迁移
	return db.Table(tableName).AutoMigrate(model)
}

// tableExists 检查表是否存在
func tableExists(db *gorm.DB, tableName string) bool {
	var exists bool
	query := "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)"
	err := db.Raw(query, tableName).Scan(&exists).Error
	return err == nil && exists
}

// AutoCreateTable 自动创建分表（如果不存在）
// 在插入数据时调用，如果表不存在则自动创建
func AutoCreateTable(db *gorm.DB, strategy ShardingStrategy, tableName string, model interface{}) error {
	if tableExists(db, tableName) {
		return nil // 表已存在
	}

	// 创建表
	return db.Table(tableName).AutoMigrate(model)
}

// AutoMigrateAll 批量自动迁移多个策略
func AutoMigrateAll(db *gorm.DB, strategies []ShardingStrategy, models map[string]interface{}, options ...AutoMigrateOptions) error {
	for _, strategy := range strategies {
		baseTableName := strategy.GetBaseTableName()
		model, ok := models[baseTableName]
		if !ok {
			return fmt.Errorf("model not found for strategy %s", baseTableName)
		}

		if err := AutoMigrate(db, strategy, model, options...); err != nil {
			return fmt.Errorf("failed to auto migrate strategy %s: %w", baseTableName, err)
		}
	}

	return nil
}

// CreateAllShardingTables 创建所有分表（使用 SQL）
// 这个方法适用于需要自定义表结构的情况
func CreateAllShardingTables(db *gorm.DB, strategy ShardingStrategy, createTableSQL string, skipIfExists bool) error {
	baseTableName := strategy.GetBaseTableName()
	tableNames := strategy.GetAllTableNames(baseTableName)

	// 如果是时间分表
	if len(tableNames) == 0 || (len(tableNames) == 1 && tableNames[0] == baseTableName) {
		if timeStrategy, ok := strategy.(*TimeShardingStrategy); ok {
			// 使用默认时间范围
			endTime := time.Now()
			startTime := endTime.AddDate(-1, 0, 0)
			tableNames = timeStrategy.GetAllTableNamesInRange(baseTableName, startTime, endTime)
		}
	}

	for _, tableName := range tableNames {
		// 替换表名
		sql := strings.ReplaceAll(createTableSQL, baseTableName, tableName)

		// 如果需要跳过已存在的表
		if skipIfExists {
			sql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", extractTableDefinition(sql))
		}

		if err := db.Exec(sql).Error; err != nil {
			// 如果表已存在且设置了跳过，忽略错误
			if skipIfExists && strings.Contains(strings.ToLower(err.Error()), "already exists") {
				continue
			}
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}

	return nil
}

// extractTableDefinition 从 CREATE TABLE SQL 中提取表定义部分
func extractTableDefinition(sql string) string {
	// 简化处理：如果 SQL 中已经包含 CREATE TABLE IF NOT EXISTS，直接返回
	sql = strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(sql)
	
	if strings.Contains(upperSQL, "CREATE TABLE IF NOT EXISTS") {
		return sql
	}
	
	// 替换 CREATE TABLE 为 CREATE TABLE IF NOT EXISTS
	if strings.HasPrefix(upperSQL, "CREATE TABLE") {
		sql = strings.Replace(sql, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	}
	
	return sql
}

// EnsureTableExists 确保表存在，如果不存在则创建
// 这个方法可以在插入数据前调用
func EnsureTableExists(db *gorm.DB, strategy ShardingStrategy, shardingValue interface{}, model interface{}) error {
	baseTableName := strategy.GetBaseTableName()
	tableName := strategy.GetTableName(baseTableName, shardingValue)

	if tableExists(db, tableName) {
		return nil
	}

	// 创建表
	return db.Table(tableName).AutoMigrate(model)
}


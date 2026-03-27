package sharding

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

var autoCleanupRunState = struct {
	mu      sync.Mutex
	lastRun map[string]time.Time
}{
	lastRun: make(map[string]time.Time),
}

// AutoMigrateOptions 自动迁移选项
type AutoMigrateOptions struct {
	SkipIfExists bool                  // 如果表已存在则跳过
	TimeRange    *AutoMigrateTimeRange // 时间分表的时间范围（可选）
}

// AutoMigrateTimeRange 自动迁移的时间范围
type AutoMigrateTimeRange struct {
	StartTime time.Time
	EndTime   time.Time
}

type cleanupExpiredTimeTablesOptions struct {
	ExpireBefore time.Time // 分表结束边界 <= 该时间时视为过期
	DryRun       bool      // 仅预览，不实际删除表
}

// CleanupRetainRecentTimeTablesOptions 按保留最近 N 个时间分片清理旧表的选项。
type CleanupRetainRecentTimeTablesOptions struct {
	RetainCount   int       // 保留最近 N 个分片（包含 ReferenceTime 所在分片）
	ReferenceTime time.Time // 参考时间；零值时默认使用 time.Now()
	DryRun        bool      // 仅预览，不实际删除表
}

// CleanupExpiredTimeTablesResult 清理过期时间分表的结果。
type CleanupExpiredTimeTablesResult struct {
	ScannedCount  int
	MatchedTables []string
	SkippedTables []string
	ExpiredTables []string
	DroppedTables []string
	DryRun        bool
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

// cleanupExpiredTimeTables 扫描数据库中属于当前时间分表策略的实际分表，并清理已过期的表。
// 过期判定规则：分表覆盖区间的结束时间 <= ExpireBefore。
func cleanupExpiredTimeTables(db *gorm.DB, strategy *TimeShardingStrategy, options cleanupExpiredTimeTablesOptions) (*CleanupExpiredTimeTablesResult, error) {
	if db == nil {
		return nil, fmt.Errorf("db is required")
	}
	if strategy == nil {
		return nil, fmt.Errorf("time sharding strategy is required")
	}
	if options.ExpireBefore.IsZero() {
		return nil, fmt.Errorf("ExpireBefore is required")
	}

	tableNames, err := db.Migrator().GetTables()
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	sort.Strings(tableNames)

	result := &CleanupExpiredTimeTablesResult{
		ScannedCount:  len(tableNames),
		MatchedTables: make([]string, 0),
		SkippedTables: make([]string, 0),
		ExpiredTables: make([]string, 0),
		DroppedTables: make([]string, 0),
		DryRun:        options.DryRun,
	}

	prefix := strategy.GetBaseTableName() + "_"
	for _, tableName := range tableNames {
		if !strings.HasPrefix(tableName, prefix) {
			continue
		}

		expired, err := strategy.IsTableExpired(tableName, options.ExpireBefore)
		if err != nil {
			result.SkippedTables = append(result.SkippedTables, tableName)
			continue
		}

		result.MatchedTables = append(result.MatchedTables, tableName)
		if !expired {
			continue
		}

		result.ExpiredTables = append(result.ExpiredTables, tableName)
		if options.DryRun {
			continue
		}

		if err := db.Migrator().DropTable(tableName); err != nil {
			return result, fmt.Errorf("failed to drop expired table %s: %w", tableName, err)
		}
		result.DroppedTables = append(result.DroppedTables, tableName)
	}

	return result, nil
}

// CleanupTimeTablesRetainingRecent 按保留最近 N 个时间分片的方式清理旧表。
// retainCount 包含 ReferenceTime 所在分片；例如按月分表时，referenceTime=2026-03-27、retainCount=3，
// 则保留 2026-01、2026-02、2026-03 对应的分表。
func CleanupTimeTablesRetainingRecent(db *gorm.DB, strategy *TimeShardingStrategy, options CleanupRetainRecentTimeTablesOptions) (*CleanupExpiredTimeTablesResult, error) {
	if strategy == nil {
		return nil, fmt.Errorf("time sharding strategy is required")
	}
	if options.RetainCount <= 0 {
		return nil, fmt.Errorf("RetainCount must be greater than 0")
	}

	referenceTime := options.ReferenceTime
	if referenceTime.IsZero() {
		referenceTime = time.Now()
	}

	currentShardStart := strategy.alignToShardStart(referenceTime)
	earliestRetainedShardStart := strategy.shiftShardTime(currentShardStart, -(options.RetainCount - 1))

	return cleanupExpiredTimeTables(db, strategy, cleanupExpiredTimeTablesOptions{
		ExpireBefore: earliestRetainedShardStart,
		DryRun:       options.DryRun,
	})
}

func maybeAutoCleanupTimeTables(db *gorm.DB, strategy *TimeShardingStrategy, shardingValue interface{}, options *TimeShardingRegisterOptions) error {
	if db == nil || strategy == nil || options == nil || options.AutoCleanup == nil || !options.AutoCleanup.Enabled {
		return nil
	}

	referenceTime := strategy.convertToTime(shardingValue)
	if referenceTime.IsZero() {
		referenceTime = time.Now()
	}

	cleanupKey := getAutoCleanupStateKey(db, strategy)
	now := time.Now()

	autoCleanupRunState.mu.Lock()
	lastRun, ok := autoCleanupRunState.lastRun[cleanupKey]
	if ok && options.AutoCleanup.MinInterval > 0 && now.Sub(lastRun) < options.AutoCleanup.MinInterval {
		autoCleanupRunState.mu.Unlock()
		return nil
	}
	autoCleanupRunState.lastRun[cleanupKey] = now
	autoCleanupRunState.mu.Unlock()

	_, err := CleanupTimeTablesRetainingRecent(db, strategy, CleanupRetainRecentTimeTablesOptions{
		RetainCount:   options.AutoCleanup.RetainCount,
		ReferenceTime: referenceTime,
	})
	if err != nil {
		autoCleanupRunState.mu.Lock()
		delete(autoCleanupRunState.lastRun, cleanupKey)
		autoCleanupRunState.mu.Unlock()
		return err
	}

	return nil
}

func getAutoCleanupStateKey(db *gorm.DB, strategy *TimeShardingStrategy) string {
	baseTableName := strategy.GetBaseTableName()
	unitName := strategy.GetUnitName()
	if db == nil || db.ConnPool == nil {
		return fmt.Sprintf("%s:%s", baseTableName, unitName)
	}

	connValue := reflect.ValueOf(db.ConnPool)
	if connValue.IsValid() && connValue.Kind() == reflect.Ptr && !connValue.IsNil() {
		return fmt.Sprintf("%x:%s:%s", connValue.Pointer(), baseTableName, unitName)
	}

	return fmt.Sprintf("%T:%s:%s", db.ConnPool, baseTableName, unitName)
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
	return db != nil && db.Migrator().HasTable(tableName)
}

// AutoCreateTable 自动创建分表（如果不存在）
// 在插入数据时调用，如果表不存在则自动创建
func AutoCreateTable(db *gorm.DB, _ ShardingStrategy, tableName string, model interface{}) error {
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

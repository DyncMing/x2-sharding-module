package sharding

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ShardingHelper 分表辅助工具
type ShardingHelper struct {
	db       *gorm.DB
	strategies map[string]ShardingStrategy // 按基础表名缓存策略
}

// NewShardingHelper 创建分表辅助工具
func NewShardingHelper(db *gorm.DB) *ShardingHelper {
	return &ShardingHelper{
		db:        db,
		strategies: make(map[string]ShardingStrategy),
	}
}

// RegisterStrategy 注册分表策略
func (h *ShardingHelper) RegisterStrategy(strategy ShardingStrategy) error {
	baseTableName := strategy.GetBaseTableName()
	h.strategies[baseTableName] = strategy
	return RegisterSharding(h.db, strategy)
}

// GetStrategy 获取分表策略
func (h *ShardingHelper) GetStrategy(baseTableName string) (ShardingStrategy, bool) {
	strategy, ok := h.strategies[baseTableName]
	return strategy, ok
}

// Create 创建记录（自动路由到正确的分表）
func (h *ShardingHelper) Create(value interface{}) error {
	// 尝试从所有已注册的策略中找到匹配的
	for baseTableName, strategy := range h.strategies {
		// 简单检查：如果 value 的某个字段名包含 baseTableName
		// 这里简化处理，实际使用中可能需要更智能的匹配
		if shardingValue, err := strategy.GetShardingValue(value); err == nil {
			tableName := strategy.GetTableName(baseTableName, shardingValue)
			return h.db.Table(tableName).Create(value).Error
		}
	}
	
	return fmt.Errorf("no matching sharding strategy found")
}

// CreateWithTable 在指定表创建记录
func (h *ShardingHelper) CreateWithTable(baseTableName string, value interface{}) error {
	strategy, ok := h.GetStrategy(baseTableName)
	if !ok {
		return fmt.Errorf("strategy not found for table: %s", baseTableName)
	}
	
	tableName := GetTableNameWithValue(strategy, value)
	return h.db.Table(tableName).Create(value).Error
}

// Find 查询记录（单表）
func (h *ShardingHelper) Find(baseTableName string, shardingValue interface{}, dest interface{}, conds ...interface{}) error {
	strategy, ok := h.GetStrategy(baseTableName)
	if !ok {
		return fmt.Errorf("strategy not found for table: %s", baseTableName)
	}
	
	tableName := strategy.GetTableName(baseTableName, shardingValue)
	query := h.db.Table(tableName)
	
	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}
	
	return query.Find(dest).Error
}

// FindAll 跨表查询所有记录
func (h *ShardingHelper) FindAll(baseTableName string, dest interface{}, queryBuilder QueryBuilder) error {
	strategy, ok := h.GetStrategy(baseTableName)
	if !ok {
		return fmt.Errorf("strategy not found for table: %s", baseTableName)
	}
	
	return CrossTableQuery(h.db, strategy, dest, queryBuilder)
}

// Paginate 跨表分页查询
func (h *ShardingHelper) Paginate(baseTableName string, dest interface{}, page, pageSize int, queryBuilder QueryBuilder) (*Paginator, error) {
	strategy, ok := h.GetStrategy(baseTableName)
	if !ok {
		return nil, fmt.Errorf("strategy not found for table: %s", baseTableName)
	}
	
	return CrossTablePaginate(h.db, strategy, dest, page, pageSize, queryBuilder)
}

// GenerateTableNames 生成所有分表的创建 SQL
func GenerateTableNames(baseTableName string, strategy ShardingStrategy, tableSQL string) []string {
	tableNames := strategy.GetAllTableNames(baseTableName)
	sqlStatements := make([]string, 0, len(tableNames))
	
	for _, tableName := range tableNames {
		// 替换表名
		sql := strings.ReplaceAll(tableSQL, baseTableName, tableName)
		sqlStatements = append(sqlStatements, sql)
	}
	
	return sqlStatements
}

// GenerateTimeTableNames 生成时间分表的创建 SQL（指定时间范围）
func GenerateTimeTableNames(baseTableName string, strategy ShardingStrategy, tableSQL string, startTime, endTime time.Time) []string {
	timeStrategy, ok := strategy.(*TimeShardingStrategy)
	if !ok {
		return []string{}
	}
	
	tableNames := timeStrategy.GetAllTableNamesInRange(baseTableName, startTime, endTime)
	sqlStatements := make([]string, 0, len(tableNames))
	
	for _, tableName := range tableNames {
		sql := strings.ReplaceAll(tableSQL, baseTableName, tableName)
		sqlStatements = append(sqlStatements, sql)
	}
	
	return sqlStatements
}

// CreateAllHashTables 批量创建所有 Hash 分表
func CreateAllHashTables(db *gorm.DB, baseTableName string, tableCount int, createTableSQL string) error {
	hashStrategy := NewHashShardingStrategy(baseTableName, "", tableCount)
	tableNames := hashStrategy.GetAllTableNames(baseTableName)
	
	for _, tableName := range tableNames {
		sql := strings.ReplaceAll(createTableSQL, baseTableName, tableName)
		if err := db.Exec(sql).Error; err != nil {
			// 如果表已存在，忽略错误
			if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
				return fmt.Errorf("failed to create table %s: %w", tableName, err)
			}
		}
	}
	
	return nil
}


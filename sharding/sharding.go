package sharding

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ShardingStrategy 分表策略接口
type ShardingStrategy interface {
	// GetTableName 根据分表键值获取实际表名
	GetTableName(baseTableName string, shardingValue interface{}) string

	// GetAllTableNames 获取所有分表名称
	GetAllTableNames(baseTableName string) []string

	// GetShardingValue 从模型对象中提取分表键值
	GetShardingValue(value interface{}) (interface{}, error)

	// GetBaseTableName 获取基础表名
	GetBaseTableName() string
}

// ShardingConfig 分表配置
type ShardingConfig struct {
	Strategy        ShardingStrategy
	BaseDB          *gorm.DB
	TableNames      map[string]string // 缓存表名映射
	AutoCreateTable bool              // 是否自动创建表
	Model           interface{}       // 用于自动创建表的模型
}

// RegisterSharding 注册分表策略到 GORM
func RegisterSharding(db *gorm.DB, strategy ShardingStrategy) error {
	return RegisterShardingWithConfig(db, strategy, false, nil)
}

// RegisterShardingWithAutoCreate 注册分表策略并启用自动创建表功能
func RegisterShardingWithAutoCreate(db *gorm.DB, strategy ShardingStrategy, model interface{}) error {
	return RegisterShardingWithConfig(db, strategy, true, model)
}

// RegisterShardingWithConfig 注册分表策略（带配置）
func RegisterShardingWithConfig(db *gorm.DB, strategy ShardingStrategy, autoCreate bool, model interface{}) error {
	// 使用 GORM 的插件机制
	db.Callback().Create().Before("gorm:create").Register("sharding:create", func(db *gorm.DB) {
		if db.Statement.Schema != nil && db.Statement.Schema.Table == strategy.GetBaseTableName() {
			if value := db.Statement.ReflectValue; value.IsValid() {
				if shardingValue, err := strategy.GetShardingValue(db.Statement.Dest); err == nil {
					tableName := strategy.GetTableName(strategy.GetBaseTableName(), shardingValue)
					db.Statement.Table = tableName

					// 如果启用了自动创建表，检查并创建表
					if autoCreate && model != nil {
						// 使用 dest 作为模型（如果 model 为 nil）
						tableModel := model
						if tableModel == nil {
							tableModel = db.Statement.Dest
						}
						// 异步创建表（避免影响插入性能）
						_ = AutoCreateTable(db, strategy, tableName, tableModel)
					}
				}
			}
		}
	})

	db.Callback().Query().Before("gorm:query").Register("sharding:query", func(db *gorm.DB) {
		// 查询时的表名替换由用户通过 Table() 方法指定
	})

	return nil
}

// GetTableNameWithValue 根据分表值获取表名（辅助函数）
func GetTableNameWithValue(strategy ShardingStrategy, value interface{}) string {
	shardingValue, err := strategy.GetShardingValue(value)
	if err != nil {
		return strategy.GetBaseTableName()
	}
	return strategy.GetTableName(strategy.GetBaseTableName(), shardingValue)
}

// SetTableName 设置表名到 GORM Statement
func SetTableName(db *gorm.DB, strategy ShardingStrategy, value interface{}) {
	tableName := GetTableNameWithValue(strategy, value)
	db.Statement.Table = tableName
}

// ExtractValue 从 interface{} 中提取值（支持结构体字段）
func ExtractValue(value interface{}, fieldName string) (interface{}, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, fmt.Errorf("value is nil")
		}
		rv = rv.Elem()
	}

	if rv.Kind() == reflect.Struct {
		// 首先尝试直接通过字段名查找
		field := rv.FieldByName(fieldName)
		if field.IsValid() && field.CanInterface() {
			return field.Interface(), nil
		}

		// 尝试通过 tag 查找（gorm column tag）
		t := rv.Type()
		for i := 0; i < t.NumField(); i++ {
			structField := t.Field(i)

			// 检查 gorm tag
			if gormTag := structField.Tag.Get("gorm"); gormTag != "" {
				if strings.Contains(gormTag, "column:") {
					parts := strings.Split(gormTag, "column:")
					if len(parts) > 1 {
						columnName := strings.TrimSpace(strings.Split(parts[1], ";")[0])
						if columnName == fieldName {
							field = rv.Field(i)
							if field.IsValid() && field.CanInterface() {
								return field.Interface(), nil
							}
						}
					}
				}
			}

			// 检查 json tag
			if jsonTag := structField.Tag.Get("json"); jsonTag != "" {
				jsonName := strings.TrimSpace(strings.Split(jsonTag, ",")[0])
				if jsonName == fieldName {
					field = rv.Field(i)
					if field.IsValid() && field.CanInterface() {
						return field.Interface(), nil
					}
				}
			}

			// 尝试下划线命名匹配（UserID -> user_id）
			if toSnakeCase(structField.Name) == fieldName ||
				strings.EqualFold(structField.Name, fieldName) {
				field = rv.Field(i)
				if field.IsValid() && field.CanInterface() {
					return field.Interface(), nil
				}
			}
		}

		return nil, fmt.Errorf("field %s not found", fieldName)
	}

	return nil, fmt.Errorf("unsupported value type: %v", rv.Kind())
}

// toSnakeCase 转换为下划线命名
func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// FormatTimeTableName 格式化时间表名（辅助函数）
func FormatTimeTableName(baseTableName string, t time.Time, format string) string {
	return fmt.Sprintf("%s_%s", baseTableName, t.Format(format))
}

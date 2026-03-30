package sharding

import (
	"encoding/json"
	"fmt"
	"os"
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
	Strategy         ShardingStrategy
	BaseDB           *gorm.DB
	TableNames       map[string]string // 缓存表名映射
	AutoCreateTable  bool              // 是否自动创建表
	AutoUpdateSchema bool              // 是否自动更新已存在分表的表结构
	Model            interface{}       // 用于自动创建表的模型
}

// RegisterShardingOptions 注册分表策略时的运行时选项。
type RegisterShardingOptions struct {
	Model            interface{}
	AutoCreateTable  bool
	AutoUpdateSchema bool
	TimeSharding     *TimeShardingRegisterOptions
}

// TimeShardingRegisterOptions 时间分表注册选项。
type TimeShardingRegisterOptions struct {
	AutoCleanup *TimeShardingAutoCleanupOptions
}

// TimeShardingAutoCleanupOptions 时间分表自动清理选项。
type TimeShardingAutoCleanupOptions struct {
	Enabled     bool
	RetainCount int
	MinInterval time.Duration
}

// RegisterShardingFileConfig JSON 配置文件对应的注册选项。
type RegisterShardingFileConfig struct {
	AutoCreateTable  bool                            `json:"autoCreateTable"`
	AutoUpdateSchema bool                            `json:"autoUpdateSchema,omitempty"`
	TimeSharding     *TimeShardingRegisterFileConfig `json:"timeSharding,omitempty"`
}

// TimeShardingRegisterFileConfig JSON 配置文件中的时间分表注册选项。
type TimeShardingRegisterFileConfig struct {
	AutoCleanup         *TimeShardingAutoCleanupFileConfig         `json:"autoCleanup,omitempty"`
	AutoCleanupPolicies *TimeShardingAutoCleanupPoliciesFileConfig `json:"autoCleanupPolicies,omitempty"`
}

// TimeShardingAutoCleanupPoliciesFileConfig 多时间分表策略共享的自动清理配置。
// 优先级：ByBaseTable > ByUnit > Default。
type TimeShardingAutoCleanupPoliciesFileConfig struct {
	Default     *TimeShardingAutoCleanupFileConfig           `json:"default,omitempty"`
	ByUnit      map[string]TimeShardingAutoCleanupFileConfig `json:"byUnit,omitempty"`
	ByBaseTable map[string]TimeShardingAutoCleanupFileConfig `json:"byBaseTable,omitempty"`
}

// TimeShardingAutoCleanupFileConfig JSON 配置文件中的自动清理选项。
type TimeShardingAutoCleanupFileConfig struct {
	Enabled     bool   `json:"enabled"`
	RetainCount int    `json:"retainCount"`
	MinInterval string `json:"minInterval,omitempty"`
}

// ConfigFileShardingRegistration 表示一次批量配置文件注册中的单个策略项。
type ConfigFileShardingRegistration struct {
	Strategy ShardingStrategy
	Model    interface{}
}

// RegisterSharding 注册分表策略到 GORM
func RegisterSharding(db *gorm.DB, strategy ShardingStrategy) error {
	return RegisterShardingWithOptions(db, strategy, RegisterShardingOptions{})
}

// RegisterShardingWithAutoCreate 注册分表策略并启用自动创建表功能
func RegisterShardingWithAutoCreate(db *gorm.DB, strategy ShardingStrategy, model interface{}) error {
	return RegisterShardingWithOptions(db, strategy, RegisterShardingOptions{
		Model:            model,
		AutoCreateTable:  true,
		AutoUpdateSchema: true,
	})
}

// RegisterShardingWithConfig 注册分表策略（带配置）
func RegisterShardingWithConfig(db *gorm.DB, strategy ShardingStrategy, autoCreate bool, model interface{}) error {
	return RegisterShardingWithOptions(db, strategy, RegisterShardingOptions{
		Model:           model,
		AutoCreateTable: autoCreate,
	})
}

// RegisterShardingWithOptions 注册分表策略（推荐入口）。
func RegisterShardingWithOptions(db *gorm.DB, strategy ShardingStrategy, options RegisterShardingOptions) error {
	if db == nil {
		return fmt.Errorf("db is required")
	}
	if strategy == nil {
		return fmt.Errorf("strategy is required")
	}
	if err := validateRegisterShardingOptions(strategy, options); err != nil {
		return err
	}

	callbackName := fmt.Sprintf("sharding:create:%s", strategy.GetBaseTableName())
	registeredModelType := indirectModelType(options.Model)

	return db.Callback().Create().Before("gorm:create").Replace(callbackName, func(tx *gorm.DB) {
		if !shouldApplyCreateSharding(tx, strategy, registeredModelType) {
			return
		}

		shardingValue, err := strategy.GetShardingValue(tx.Statement.Dest)
		if err != nil {
			tx.AddError(err)
			return
		}

		tableName := strategy.GetTableName(strategy.GetBaseTableName(), shardingValue)
		tx.Statement.Table = tableName
		tx.Statement.TableExpr = nil

		baseDB := tx.Session(&gorm.Session{NewDB: true})
		if options.AutoCreateTable || options.AutoUpdateSchema {
			tableModel := options.Model
			if tableModel == nil {
				tableModel = tx.Statement.Dest
			}

			var err error
			if options.AutoUpdateSchema {
				err = AutoCreateTableWithSchemaSync(baseDB, strategy, tableName, tableModel)
			} else {
				err = AutoCreateTable(baseDB, strategy, tableName, tableModel)
			}
			if err != nil {
				tx.AddError(err)
				return
			}
		}

		if timeStrategy, ok := strategy.(*TimeShardingStrategy); ok {
			if err := maybeAutoCleanupTimeTables(baseDB, timeStrategy, shardingValue, options.TimeSharding); err != nil {
				tx.AddError(err)
			}
		}
	})
}

// LoadRegisterShardingOptionsFromJSON 从 JSON 配置文件加载注册选项。
func LoadRegisterShardingOptionsFromJSON(filePath string, model interface{}) (RegisterShardingOptions, error) {
	return loadRegisterShardingOptionsFromJSON(filePath, nil, model)
}

// LoadRegisterShardingOptionsForStrategyFromJSON 从 JSON 配置文件加载指定分表策略的注册选项。
func LoadRegisterShardingOptionsForStrategyFromJSON(filePath string, strategy ShardingStrategy, model interface{}) (RegisterShardingOptions, error) {
	return loadRegisterShardingOptionsFromJSON(filePath, strategy, model)
}

func loadRegisterShardingOptionsFromJSON(filePath string, strategy ShardingStrategy, model interface{}) (RegisterShardingOptions, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return RegisterShardingOptions{}, fmt.Errorf("failed to read sharding config file %s: %w", filePath, err)
	}

	var fileConfig RegisterShardingFileConfig
	if err := json.Unmarshal(content, &fileConfig); err != nil {
		return RegisterShardingOptions{}, fmt.Errorf("failed to parse sharding config file %s: %w", filePath, err)
	}

	options := RegisterShardingOptions{
		Model:            model,
		AutoCreateTable:  fileConfig.AutoCreateTable,
		AutoUpdateSchema: fileConfig.AutoUpdateSchema,
	}
	if fileConfig.TimeSharding != nil {
		autoCleanupConfig := fileConfig.TimeSharding.AutoCleanup
		if resolvedConfig, ok, err := resolveTimeShardingAutoCleanupFileConfig(fileConfig.TimeSharding.AutoCleanupPolicies, strategy); err != nil {
			return RegisterShardingOptions{}, fmt.Errorf("failed to resolve time sharding auto cleanup policy in %s: %w", filePath, err)
		} else if ok {
			autoCleanupConfig = resolvedConfig
		}

		if autoCleanupConfig != nil {
			autoCleanup, err := buildTimeShardingAutoCleanupOptions(*autoCleanupConfig, filePath)
			if err != nil {
				return RegisterShardingOptions{}, err
			}
			options.TimeSharding = &TimeShardingRegisterOptions{AutoCleanup: autoCleanup}
		}
	}

	return options, nil
}

// RegisterShardingWithConfigFile 使用 JSON 配置文件注册分表策略。
func RegisterShardingWithConfigFile(db *gorm.DB, strategy ShardingStrategy, model interface{}, filePath string) error {
	options, err := LoadRegisterShardingOptionsForStrategyFromJSON(filePath, strategy, model)
	if err != nil {
		return err
	}

	return RegisterShardingWithOptions(db, strategy, options)
}

// RegisterShardingsWithConfigFile 使用同一个 JSON 配置文件批量注册多个分表策略。
func RegisterShardingsWithConfigFile(db *gorm.DB, filePath string, registrations []ConfigFileShardingRegistration) error {
	if db == nil {
		return fmt.Errorf("db is required")
	}
	if len(registrations) == 0 {
		return fmt.Errorf("registrations are required")
	}

	seenBaseTables := make(map[string]struct{}, len(registrations))
	for _, item := range registrations {
		if item.Strategy == nil {
			return fmt.Errorf("registration strategy is required")
		}
		baseTableName := item.Strategy.GetBaseTableName()
		if _, exists := seenBaseTables[baseTableName]; exists {
			return fmt.Errorf("duplicate sharding strategy registration for base table %s", baseTableName)
		}
		seenBaseTables[baseTableName] = struct{}{}
	}

	for _, item := range registrations {
		if err := RegisterShardingWithConfigFile(db, item.Strategy, item.Model, filePath); err != nil {
			return err
		}
	}

	return nil
}

func buildTimeShardingAutoCleanupOptions(config TimeShardingAutoCleanupFileConfig, filePath string) (*TimeShardingAutoCleanupOptions, error) {
	autoCleanup := &TimeShardingAutoCleanupOptions{
		Enabled:     config.Enabled,
		RetainCount: config.RetainCount,
	}
	if config.MinInterval != "" {
		minInterval, err := time.ParseDuration(config.MinInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to parse auto cleanup minInterval in %s: %w", filePath, err)
		}
		autoCleanup.MinInterval = minInterval
	}

	return autoCleanup, nil
}

func resolveTimeShardingAutoCleanupFileConfig(policies *TimeShardingAutoCleanupPoliciesFileConfig, strategy ShardingStrategy) (*TimeShardingAutoCleanupFileConfig, bool, error) {
	if policies == nil || strategy == nil {
		return nil, false, nil
	}

	timeStrategy, ok := strategy.(*TimeShardingStrategy)
	if !ok {
		return nil, false, nil
	}

	if policies.ByUnit != nil {
		for configuredUnit := range policies.ByUnit {
			if !IsValidTimeShardingUnitName(configuredUnit) {
				return nil, false, fmt.Errorf("unsupported time sharding unit %q", configuredUnit)
			}
		}
	}

	if policies.ByBaseTable != nil {
		if config, ok := policies.ByBaseTable[strategy.GetBaseTableName()]; ok {
			resolved := config
			return &resolved, true, nil
		}
	}

	if policies.ByUnit != nil {
		unitName := timeStrategy.GetUnitName()
		if config, ok := policies.ByUnit[unitName]; ok {
			resolved := config
			return &resolved, true, nil
		}
	}

	if policies.Default != nil {
		return policies.Default, true, nil
	}

	return nil, false, nil
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
	db.Statement.TableExpr = nil
}

// CreateSharded 显式按分表策略创建记录。
func CreateSharded(db *gorm.DB, strategy ShardingStrategy, value interface{}) error {
	return createShardedRecord(db, strategy, value, nil, false, false)
}

// CreateShardedWithAutoCreate 显式按分表策略创建记录；目标表不存在时自动建表，已存在时自动同步表结构。
func CreateShardedWithAutoCreate(db *gorm.DB, strategy ShardingStrategy, value interface{}, model interface{}) error {
	return createShardedRecord(db, strategy, value, model, true, true)
}

// CreateShardedWithSchemaSync 显式按分表策略创建记录，并确保目标分表结构与当前模型同步。
func CreateShardedWithSchemaSync(db *gorm.DB, strategy ShardingStrategy, value interface{}, model interface{}) error {
	return createShardedRecord(db, strategy, value, model, false, true)
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

func createShardedRecord(db *gorm.DB, strategy ShardingStrategy, value interface{}, model interface{}, autoCreate bool, autoUpdateSchema bool) error {
	if db == nil {
		return fmt.Errorf("db is required")
	}
	if strategy == nil {
		return fmt.Errorf("strategy is required")
	}

	shardingValue, err := strategy.GetShardingValue(value)
	if err != nil {
		return err
	}

	tableName := strategy.GetTableName(strategy.GetBaseTableName(), shardingValue)
	if autoCreate || autoUpdateSchema {
		tableModel := model
		if tableModel == nil {
			tableModel = value
		}

		var err error
		if autoUpdateSchema {
			err = AutoCreateTableWithSchemaSync(db.Session(&gorm.Session{NewDB: true}), strategy, tableName, tableModel)
		} else {
			err = AutoCreateTable(db.Session(&gorm.Session{NewDB: true}), strategy, tableName, tableModel)
		}
		if err != nil {
			return err
		}
	}

	return db.Table(tableName).Create(value).Error
}

func shouldApplyCreateSharding(db *gorm.DB, strategy ShardingStrategy, registeredModelType reflect.Type) bool {
	if db == nil || db.Statement == nil || strategy == nil || db.Statement.Dest == nil {
		return false
	}

	if registeredModelType != nil && sameModelType(db.Statement.Dest, registeredModelType) {
		return true
	}

	if db.Statement.Schema != nil && db.Statement.Schema.Table == strategy.GetBaseTableName() {
		return true
	}

	if db.Statement.Table == strategy.GetBaseTableName() {
		return true
	}

	return false
}

func sameModelType(value interface{}, target reflect.Type) bool {
	return indirectModelType(value) == target
}

func indirectModelType(value interface{}) reflect.Type {
	if value == nil {
		return nil
	}

	t := reflect.TypeOf(value)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
			continue
		}
		if t.Len() == 0 && t.Kind() == reflect.Array {
			t = t.Elem()
			continue
		}
		if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
			t = t.Elem()
			continue
		}
	}

	if t.Kind() == reflect.Invalid {
		return nil
	}

	return t
}

func validateRegisterShardingOptions(strategy ShardingStrategy, options RegisterShardingOptions) error {
	if (options.AutoCreateTable || options.AutoUpdateSchema) && options.Model == nil {
		return fmt.Errorf("model is required when AutoCreateTable or AutoUpdateSchema is enabled")
	}

	if options.TimeSharding == nil || options.TimeSharding.AutoCleanup == nil || !options.TimeSharding.AutoCleanup.Enabled {
		return nil
	}

	if _, ok := strategy.(*TimeShardingStrategy); !ok {
		return fmt.Errorf("time sharding auto cleanup is only supported for TimeShardingStrategy")
	}
	if options.TimeSharding.AutoCleanup.RetainCount <= 0 {
		return fmt.Errorf("time sharding auto cleanup RetainCount must be greater than 0")
	}
	if options.TimeSharding.AutoCleanup.MinInterval < 0 {
		return fmt.Errorf("time sharding auto cleanup MinInterval must be greater than or equal to 0")
	}

	return nil
}

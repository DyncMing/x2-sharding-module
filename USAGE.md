# 使用指南

## 概述

X2 Sharding Module 是一个基于 GORM 的 MySQL 分表模块，支持多种分表策略和跨表查询功能。

## 安装

```bash
go mod tidy
```

## 核心概念

### 分表策略 (ShardingStrategy)

分表策略定义了如何将数据分散到不同的表中。目前支持两种策略：

1. **Hash 分表** - 基于字段值的 Hash 值进行分表
2. **时间分表** - 基于时间字段（年/月/日/小时/分钟）进行分表

### 分表键 (Sharding Key)

分表键是用于决定数据存储在哪个分表的字段。例如：
- Hash 分表：使用 `user_id` 作为分表键
- 时间分表：使用 `created_at` 作为分表键

## 使用方法

### 0. 数据库连接和自动创建

#### 自动创建数据库

模块支持在连接数据库时自动创建数据库（如果不存在）：

```go
import (
    "x2-sharding-module/sharding"
    "gorm.io/gorm"
)

// 方式 1: 使用默认字符集（utf8mb4）
dsn := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
db, err := sharding.OpenMySQLWithAutoCreateDB(dsn, &gorm.Config{})
if err != nil {
    log.Fatal("Failed to connect:", err)
}

// 方式 2: 自定义字符集和排序规则
db, err = sharding.OpenWithAutoCreateDB(
    dsn,
    &gorm.Config{},
    "utf8mb4",           // 字符集
    "utf8mb4_unicode_ci", // 排序规则
)

// 方式 3: 手动检查和创建数据库
serverDSN := "root:password@tcp(localhost:3306)/?charset=utf8mb4"
serverDB, _ := sharding.OpenMySQLWithAutoCreateDB(serverDSN, &gorm.Config{})

// 检查数据库是否存在
exists, _ := sharding.DatabaseExists(serverDB, "mydb")
if !exists {
    // 创建数据库
    sharding.CreateDatabase(serverDB, "mydb", "utf8mb4", "utf8mb4_unicode_ci")
}

// 或使用便捷方法
sharding.EnsureDatabaseExists(serverDB, "mydb", "utf8mb4", "utf8mb4_unicode_ci")
```

#### DSN 工具函数

```go
// 解析 DSN
dsnInfo, err := sharding.ParseDSN("root:password@tcp(localhost:3306)/testdb")
// 提取数据库名
dbName, _ := sharding.ExtractDatabaseFromDSN(dsn)
// 替换数据库名
newDSN, _ := sharding.ReplaceDatabaseInDSN(dsn, "newdb")
```

### 1. 自动创建分表

分表模块支持自动创建所有分表，有两种方式：

#### 方式 1: 预先创建所有分表

```go
// Hash 分表：自动创建所有 4 张表
hashStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
err := sharding.AutoMigrate(db, hashStrategy, &User{}, sharding.AutoMigrateOptions{
    SkipIfExists: true, // 如果表已存在则跳过
})

// 时间分表：创建指定时间范围的表
timeStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)
err = sharding.AutoMigrate(db, timeStrategy, &Log{}, sharding.AutoMigrateOptions{
    SkipIfExists: true,
    TimeRange: &sharding.AutoMigrateTimeRange{
        StartTime: time.Now().AddDate(0, -3, 0), // 最近 3 个月
        EndTime:   time.Now(),
    },
})
```

#### 方式 2: 插入数据时自动创建

```go
// 注册策略并启用自动创建表功能
err := sharding.RegisterShardingWithAutoCreate(db, hashStrategy, &User{})

// 插入数据时，如果表不存在会自动创建
user := &User{UserID: 123, Name: "John"}
tableName := hashStrategy.GetTableName("users", 123)
db.Table(tableName).Create(user) // 如果表不存在会自动创建
```

#### 方式 3: 手动确保表存在

```go
// 在插入数据前确保表存在
err := sharding.EnsureTableExists(db, hashStrategy, 123, &User{})
if err == nil {
    db.Table(tableName).Create(user)
}
```

### 1. Hash 分表

#### 创建 Hash 分表策略

```go
import "x2-sharding-module/sharding"

// 创建 4 张分表（users_0, users_1, users_2, users_3）
hashStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
```

参数说明：
- `baseTableName`: 基础表名（如 "users"）
- `shardingKey`: 分表键字段名（结构体字段名，如 "UserID"）
- `tableCount`: 分表数量

#### 插入数据

```go
// 方式 1: 自动获取表名并插入
user := &User{UserID: 123, Name: "John"}
tableName := hashStrategy.GetTableName("users", 123)
db.Table(tableName).Create(user)

// 方式 2: 使用辅助函数
sharding.SetTableName(db, hashStrategy, user)
db.Statement.Create(user)
```

#### 查询数据

```go
// 单表查询（需要知道分表键值）
userID := int64(123)
tableName := hashStrategy.GetTableName("users", userID)
var user User
db.Table(tableName).Where("user_id = ?", userID).First(&user)

// 跨表查询（查询所有分表）
var users []User
err := sharding.CrossTableQuery(db, hashStrategy, &users, func(tx *gorm.DB) *gorm.DB {
    return tx.Where("name LIKE ?", "%John%")
})
```

#### 分页查询

```go
var users []User
paginator, err := sharding.CrossTablePaginate(
    db, 
    hashStrategy, 
    &users, 
    1,      // 页码
    10,     // 每页数量
    func(tx *gorm.DB) *gorm.DB {
        return tx.Order("id DESC")
    },
)

fmt.Printf("Page: %d, Total: %d\n", paginator.Page, paginator.Total)
```

### 2. 时间分表

#### 创建时间分表策略

```go
// 按月分表（自动识别时间类型）
timeStrategy := sharding.NewTimeShardingStrategy(
    "logs",           // 基础表名
    "CreatedAt",      // 时间字段名
    sharding.TimeShardingByMonth, // 分表单位
)
```

支持的分表单位：
- `TimeShardingByYear` - 按年分表（格式：logs_2006）
- `TimeShardingByMonth` - 按月分表（格式：logs_200601）
- `TimeShardingByDay` - 按日分表（格式：logs_20060102）
- `TimeShardingByHour` - 按小时分表（格式：logs_2006010215）
- `TimeShardingByMinute` - 按分钟分表（格式：logs_200601021504）

#### 指定时间字段类型

时间分表支持多种时间字段类型，可以通过 `NewTimeShardingStrategyWithType` 指定：

```go
// 方式 1: time.Time 类型
timeStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs",
    "CreatedAt",
    sharding.TimeShardingByMonth,
    sharding.TimeFieldTypeTime,
)

// 方式 2: int64 Unix 时间戳（秒）
timestampStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs",
    "CreatedAt",
    sharding.TimeShardingByDay,
    sharding.TimeFieldTypeTimestamp,
)

// 方式 3: int64 Unix 时间戳（毫秒）
timestampMsStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs",
    "CreatedAt",
    sharding.TimeShardingByHour,
    sharding.TimeFieldTypeTimestampMs,
)

// 方式 4: string 日期格式 (YYYY-MM-DD)
dateStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs",
    "CreatedAt",
    sharding.TimeShardingByDay,
    sharding.TimeFieldTypeDate,
)

// 方式 5: string 日期时间格式 (YYYY-MM-DD HH:MM:SS)
dateTimeStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs",
    "CreatedAt",
    sharding.TimeShardingByMonth,
    sharding.TimeFieldTypeDateTime,
)

// 方式 6: 自动识别（默认）
autoStrategy := sharding.NewTimeShardingStrategy(
    "logs",
    "CreatedAt",
    sharding.TimeShardingByMonth,
)
```

**支持的时间类型：**
- `TimeFieldTypeAuto` - 自动识别（默认）
- `TimeFieldTypeTime` - time.Time 类型
- `TimeFieldTypeTimestamp` - int64/uint64 Unix 时间戳（秒）
- `TimeFieldTypeTimestampMs` - int64 Unix 时间戳（毫秒）
- `TimeFieldTypeDate` - string 日期格式 (YYYY-MM-DD)
- `TimeFieldTypeDateTime` - string 日期时间格式 (YYYY-MM-DD HH:MM:SS)

#### 插入数据

```go
log := &Log{
    CreatedAt: time.Now(),
    Message:   "Test log",
}
tableName := timeStrategy.GetTableName("logs", log.CreatedAt)
db.Table(tableName).Create(log)
```

#### 查询指定时间范围的数据

```go
// 方式 1: 使用 time.Time
startTime := time.Now().AddDate(0, -1, 0) // 1个月前
endTime := time.Now()

tableNames := timeStrategy.GetAllTableNamesInRange("logs", startTime, endTime)

// 方式 2: 使用时间戳（支持混合类型）
startTimestamp := time.Now().AddDate(0, -1, 0).Unix()
endTimestamp := time.Now().Unix()

tableNames = timeStrategy.GetAllTableNamesInRangeWithValues(
    "logs",
    startTimestamp,  // int64 时间戳
    endTime,         // time.Time（支持混合类型）
)

// 方式 3: 使用日期字符串
tableNames = timeStrategy.GetAllTableNamesInRangeWithValues(
    "logs",
    "2024-01-01",    // string 日期
    "2024-01-31",    // string 日期
)

// 在这些表中查询
for _, tableName := range tableNames {
    var logs []Log
    db.Table(tableName).Where("level = ?", "INFO").Find(&logs)
}
```

#### 跨表查询

```go
// 方式 1: 默认查询最近一年的数据
var logs []Log
err := sharding.CrossTableQuery(db, timeStrategy, &logs, func(tx *gorm.DB) *gorm.DB {
    return tx.Where("level = ?", "INFO").Order("created_at DESC")
})

// 方式 2: 指定时间范围（支持多种时间类型）
// 使用 time.Time
startTime := time.Now().AddDate(0, -1, 0)
endTime := time.Now()
err = sharding.CrossTableQueryWithTimeRange(
    db,
    timeStrategy,
    &logs,
    func(tx *gorm.DB) *gorm.DB {
        return tx.Where("level = ?", "INFO")
    },
    startTime,
    endTime,
)

// 使用时间戳
startTimestamp := time.Now().AddDate(0, -1, 0).Unix()
endTimestamp := time.Now().Unix()
err = sharding.CrossTableQueryWithTimeRange(
    db,
    timestampStrategy,
    &logs,
    func(tx *gorm.DB) *gorm.DB {
        return tx.Where("level = ?", "INFO")
    },
    startTimestamp,
    endTimestamp,
)

// 使用日期字符串
err = sharding.CrossTableQueryWithTimeRange(
    db,
    dateStrategy,
    &logs,
    func(tx *gorm.DB) *gorm.DB {
        return tx.Where("level = ?", "INFO")
    },
    "2024-01-01",
    "2024-01-31",
)
```

### 3. 跨表连接查询

#### 两个表的连接查询

当需要连接两个分表时，可以使用 `CrossTableJoin` 函数：

```go
userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
orderStrategy := sharding.NewHashShardingStrategy("orders", "UserID", 4)

	var results []map[string]interface{}
	err := sharding.CrossTableJoin(
		db,
		userStrategy,
		orderStrategy,
		sharding.LeftJoin,
		"users.user_id = orders.user_id",
		&results,
		func(tx *gorm.DB) *gorm.DB {
			return tx.Select("users.name, orders.amount").
				Where("users.user_id = ?", 123)
		},
	)
```

#### 多表连接查询分页

```go
// 三表连接查询分页
config := sharding.MultiJoinConfig{
    MainTable: sharding.JoinInfo{
        Strategy:    userStrategy,
        JoinType:    sharding.InnerJoin,
        OnCondition: "1=1",
    },
    JoinTables: []sharding.JoinInfo{
        {
            Strategy:    orderStrategy,
            JoinType:    sharding.LeftJoin,
            OnCondition: "users.user_id = orders.user_id",
        },
        {
            Strategy:    paymentStrategy,
            JoinType:    sharding.LeftJoin,
            OnCondition: "orders.order_id = payments.order_id",
        },
    },
}

paginator, err := sharding.CrossTableMultiJoinPaginate(
    db,
    config,
    &results,
    1,      // 页码
    10,     // 每页数量
    func(tx *gorm.DB) *gorm.DB {
        // 重要：在 queryBuilder 中使用表别名（基础表名），如 users.user_id
        // 系统会自动为表设置别名，别名就是基础表名
        // 例如：users_0 表的别名是 users，orders_0 表的别名是 orders
        return tx.Select("users.name, orders.order_id, payments.amount").
            Where("users.user_id > ?", 0).  // 使用 users 而不是 users_0
            Order("users.user_id DESC")
    },
)

fmt.Printf("Page: %d, Total: %d\n", paginator.Page, paginator.Total)

// 优化的多表连接分页（已知连接键值，只查询相关表）
joinKeys := map[string]interface{}{
    "user_id": 123,
}
optimizedPaginator, err := sharding.CrossTableMultiJoinPaginateOptimized(
    db, config, joinKeys, &results, 1, 10, queryBuilder,
)
```

#### Hash 分表的分页连接查询示例

```go
// Hash 分表策略
userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
orderStrategy := sharding.NewHashShardingStrategy("orders", "UserID", 4)
paymentStrategy := sharding.NewHashShardingStrategy("payments", "OrderID", 4)

config := sharding.MultiJoinConfig{
    MainTable: sharding.JoinInfo{
        Strategy:    userStrategy,
        JoinType:    sharding.InnerJoin,
        OnCondition: "1=1",
    },
    JoinTables: []sharding.JoinInfo{
        {
            Strategy:    orderStrategy,
            JoinType:    sharding.LeftJoin,
            OnCondition: "users.user_id = orders.user_id",
        },
        {
            Strategy:    paymentStrategy,
            JoinType:    sharding.LeftJoin,
            OnCondition: "orders.order_id = payments.order_id",
        },
    },
}

var pageResults []map[string]interface{}

// 分页（自动去重，使用别名 users/orders/payments）
hashPaginator, err := sharding.CrossTableMultiJoinPaginate(
    db,
    config,
    &pageResults,
    1,      // 页码
    20,     // 每页数量
    func(tx *gorm.DB) *gorm.DB {
        return tx.
            Select("users.user_id, users.name AS user_name, orders.order_id, orders.amount AS order_amount, payments.amount AS payment_amount, payments.status AS payment_status").
            Where("users.user_id > ?", 0).
            Order("users.user_id DESC")
    },
)
```

#### 时间分表的分页连接查询示例

```go
// 时间分表策略（按月分表）
logStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)
eventStrategy := sharding.NewTimeShardingStrategy("events", "CreatedAt", sharding.TimeShardingByMonth)

// 设置时间范围（只分页查询 2024-01 到 2024-03 的分表）
startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
endTime   := time.Date(2024, 3, 31, 23, 59, 59, 0, time.UTC)

timeConfig := sharding.MultiJoinConfig{
    MainTable: sharding.JoinInfo{
        Strategy:    logStrategy,
        JoinType:    sharding.InnerJoin,
        OnCondition: "1=1",
    },
    JoinTables: []sharding.JoinInfo{
        {
            Strategy:    eventStrategy,
            JoinType:    sharding.LeftJoin,
            OnCondition: "logs.id = events.log_id",
        },
    },
    TimeRanges: map[string]TimeRange{
        "logs":   {StartTime: startTime, EndTime: endTime},
        "events": {StartTime: startTime, EndTime: endTime},
    },
}

var timeResults []map[string]interface{}

timePaginator, err := sharding.CrossTableMultiJoinPaginate(
    db,
    timeConfig,
    &timeResults,
    1,      // 页码
    20,     // 每页数量
    func(tx *gorm.DB) *gorm.DB {
        return tx.
            Select("logs.id, logs.created_at, events.event_type, events.payload").
            Where("events.event_type = ?", "login").
            Order("logs.created_at DESC")
    },
)
```

**注意事项：**

1. **表别名**：系统会自动为所有表设置别名，别名就是基础表名。在 `queryBuilder` 中应使用别名（如 `users.user_id`）而不是实际分表名（如 `users_0.user_id`）。

2. **自动去重**：
   - 多表连接查询会自动对结果进行去重，避免因表组合产生的重复数据
   - **计数函数也会自动去重**，确保 `CrossTableMultiJoinCount` 返回的数量与查询结果一致
   - 去重逻辑会智能识别唯一字段组合（如 user_id + order_id + payment_id）

3. **自定义去重字段**：可以配置 `DeduplicateFields` 来自定义去重逻辑：
   ```go
   config := sharding.MultiJoinConfig{
       // ... 其他配置 ...
       // 自定义去重字段配置（按优先级从高到低）
       DeduplicateFields: [][]string{
           {"payment_id"},                    // 最精确：使用支付ID作为唯一键
           {"order_id", "payment_id"},        // 次精确：订单ID + 支付ID组合
           {"user_id", "order_id"},           // 通用：用户ID + 订单ID组合
           {"user_id"},                       // 最通用：仅使用用户ID
       },
   }
   ```
   
   - 如果不配置 `DeduplicateFields`，将使用默认的去重字段配置（见 `GetDefaultDeduplicateFields()`）
   - 去重逻辑会按优先级尝试每个字段组合，直到找到完全匹配的字段
   - 如果所有配置的字段组合都不匹配，将使用所有非 nil 字段的组合作为唯一键

4. **ON 条件**：在配置 `OnCondition` 时也应使用基础表名，系统会自动替换为正确的别名。

5. **计数准确性**：由于跨表连接可能产生重复数据，所有计数函数都会先查询结果，去重后再计数，确保计数准确。
```

**注意**：如果两个表使用相同的分表键和分表数量，建议直接在同一分表内进行 JOIN，效率更高：

```go
userID := int64(123)
userTable := userStrategy.GetTableName("users", userID)
orderTable := orderStrategy.GetTableName("orders", userID)

	db.Table(userTable).
		Select("users.name, orders.amount").
		Joins(fmt.Sprintf("LEFT JOIN %s ON users.user_id = orders.user_id", orderTable)).
		Where("users.user_id = ?", userID).
		Find(&results)
```

#### 多表连接查询（3个及以上表）

使用 `CrossTableMultiJoin` 函数可以连接 3 个或更多的分表：

```go
userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
orderStrategy := sharding.NewHashShardingStrategy("orders", "UserID", 4)
paymentStrategy := sharding.NewHashShardingStrategy("payments", "OrderID", 4)

config := sharding.MultiJoinConfig{
    MainTable: sharding.JoinInfo{
        Strategy:    userStrategy,
        JoinType:    sharding.InnerJoin,
        OnCondition: "1=1", // 主表不需要 ON 条件
    },
    JoinTables: []sharding.JoinInfo{
        {
            Strategy:    orderStrategy,
            JoinType:    sharding.LeftJoin,
            OnCondition: "users.user_id = orders.user_id",
        },
        {
            Strategy:    paymentStrategy,
            JoinType:    sharding.LeftJoin,
            OnCondition: "orders.order_id = payments.order_id",
        },
    },
}

var results []map[string]interface{}
err := sharding.CrossTableMultiJoin(db, config, &results, func(tx *gorm.DB) *gorm.DB {
    return tx.Select("users.name, orders.order_id, payments.amount").
        Where("users.user_id = ?", 123)
})
```

**优化的多表连接查询**：如果已知连接键值，可以使用 `CrossTableMultiJoinOptimized` 只查询相关的表组合：

```go
joinKeys := map[string]interface{}{
    "user_id":  123,
    "order_id": 456,
}

err := sharding.CrossTableMultiJoinOptimized(db, config, joinKeys, &results, queryBuilder)
```

### 4. 自定义分表策略

如果内置的 Hash 和时间分表策略无法满足需求，可以使用自定义分表策略：

#### 使用自定义函数

```go
// 定义自定义分表函数
customFunc := func(baseTableName string, shardingValue interface{}) string {
    // 例如：根据类别名称的第一个字母分表
    category := shardingValue.(string)
    if len(category) == 0 {
        return baseTableName
    }
    
    firstLetter := strings.ToUpper(string(category[0]))
    if firstLetter >= "A" && firstLetter <= "M" {
        return fmt.Sprintf("%s_0", baseTableName)
    }
    return fmt.Sprintf("%s_1", baseTableName)
}

// 定义值提取函数
valueFunc := func(value interface{}) (interface{}, error) {
    return sharding.ExtractValue(value, "Category")
}

// 定义获取所有表名函数
getAllFunc := func(baseTableName string) []string {
    return []string{
        fmt.Sprintf("%s_0", baseTableName),
        fmt.Sprintf("%s_1", baseTableName),
    }
}

// 创建自定义分表策略
customStrategy := sharding.NewCustomShardingStrategy(
    "products",
    "Category",
    customFunc,
    valueFunc,
    getAllFunc,
)
```

#### 使用范围分表策略（内置）

按数值范围进行分表：

```go
// 每个分表存储 10000 条数据，共 10 张表
rangeStrategy := sharding.NewRangeShardingStrategy("products", "ProductID", 10000, 10)

// ProductID 0-9999 在 products_0
// ProductID 10000-19999 在 products_1
// 以此类推
```

#### 使用取模分表策略（内置）

按取模运算进行分表：

```go
// 根据 ProductID % 4 分表
moduloStrategy := sharding.NewModuloShardingStrategy("products", "ProductID", 4)

// ProductID % 4 = 0 在 products_0
// ProductID % 4 = 1 在 products_1
// 以此类推
```

## 最佳实践

### 1. 选择合适的分表策略

- **Hash 分表**：适用于数据分布均匀，需要快速路由的场景
- **时间分表**：适用于有时间序列特征的数据（如日志、订单等）
- **范围分表**：适用于按 ID 范围分表，便于数据迁移和管理
- **取模分表**：适用于需要按数值取模进行简单分表的场景
- **自定义分表**：适用于有特殊业务需求，需要自定义分表规则的场景

### 2. 分表数量选择

- Hash 分表：建议选择 2 的幂次（2, 4, 8, 16...），便于扩展
- 时间分表：根据数据量和查询模式选择合适的时间粒度
- 范围分表：根据单个分表的容量需求确定分表数量和范围大小
- 自定义分表：根据业务逻辑灵活设计分表规则

### 3. 查询优化

- 单表查询优先：如果知道分表键值，直接查询对应分表
- 跨表查询：仅在必要时使用，注意性能影响
- 分页查询：大数据量时考虑使用游标分页替代偏移分页

### 4. 表结构设计

- 所有分表必须具有相同的表结构
- 分表键字段应该有索引
- 考虑在分表键上创建唯一索引（如果需要）

### 5. 数据迁移

- 在分表前，确保所有表已创建
- 可以使用脚本批量创建分表
- 迁移数据时注意保持数据一致性

## 示例代码

完整示例请参考 `examples/` 目录：

- `hash_sharding_example.go` - Hash 分表示例
- `time_sharding_example.go` - 时间分表示例
- `join_example.go` - 跨表连接查询示例

## 注意事项

1. **表不存在错误**：跨表查询时，如果某个分表不存在，会自动跳过，不会报错
2. **性能考虑**：跨表查询会查询所有分表，对于大数据量可能影响性能
3. **事务支持**：分表操作支持事务，但跨表查询在事务中可能有限制
4. **表名大小写**：MySQL 在 Windows/macOS 上表名大小写不敏感，但在 Linux 上敏感

## 常见问题

### Q: 如何扩展分表数量？

A: Hash 分表扩展需要迁移数据。建议在初期设计时就预留足够的分表数量，或者使用一致性 Hash。

### Q: 时间分表如何清理旧数据？

A: 可以直接删除对应的分表：`DROP TABLE logs_202301;`

### Q: 跨表查询性能如何优化？

A: 
- 限制查询的分表数量（时间范围）
- 使用 UNION ALL 代替多次查询
- 考虑使用读写分离
- 必要时使用缓存

### Q: 支持分布式数据库吗？

A: 当前版本仅支持单个 MySQL 实例的分表。分布式数据库的分库分表需要额外的路由逻辑。

## License

MIT


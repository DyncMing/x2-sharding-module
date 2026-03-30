# X2 Sharding Module

一个基于 GORM 的 MySQL 分表模块，支持基于 Hash 和时间维度的分表，并提供跨表查询和分页功能。

## 功能特性

- ✅ **基于 Hash 的分表策略** - 支持自定义分表数量，自动路由到对应分表
- ✅ **基于时间的分表策略** - 支持按年/月/日/小时/分钟进行分表
- ✅ **自定义分表策略** - 支持用户自定义分表逻辑，灵活适配各种业务场景
- ✅ **范围分表策略** - 按数值范围进行分表（内置实现）
- ✅ **取模分表策略** - 按取模运算进行分表（内置实现）
- ✅ **跨表查询** - 支持在所有分表中查询并自动合并结果
- ✅ **跨表分页查询** - 支持跨多个分表进行分页查询
- ✅ **跨表连接查询** - 支持两个分表之间的 JOIN 操作
- ✅ **多表连接查询** - 支持 3 个及以上分表的连接查询
- ✅ **GORM 插件机制** - 无缝集成 GORM，无需修改现有代码
- ✅ **自动创建 / 自动更新分表结构** - 支持自动创建所有分表，插入数据时自动创建表，并可同步已存在分表的新增字段
- ✅ **过期分表清理** - 支持按时间分表策略识别并清理过期表，支持 DryRun 预览
- ✅ **辅助工具** - 提供便捷的辅助函数和批量操作工具

## 快速开始

### 安装

```bash
go mod tidy
```

### 基本使用

#### Hash 分表

```go
package main

import (
    "x2-sharding-module/sharding"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
)

func main() {
    dsn := "user:password@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
    // 自动创建数据库（如果不存在）
    db, _ := sharding.OpenMySQLWithAutoCreateDB(dsn, &gorm.Config{})
    // 或使用标准方式
    // db, _ := gorm.Open(mysql.Open(dsn), &gorm.Config{})
    
    // 创建 Hash 分表策略：4 张表（users_0, users_1, users_2, users_3）
    hashStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
    sharding.RegisterSharding(db, hashStrategy)
    
    // 插入数据（自动路由到对应分表）
    user := &User{UserID: 123, Name: "John"}
    _ = sharding.RegisterShardingWithAutoCreate(db, hashStrategy, &User{})
    db.Create(user)

    // 或使用显式封装：按策略写入，并自动建表 / 同步已存在分表结构
    _ = sharding.CreateShardedWithAutoCreate(db, hashStrategy, user, &User{})
    
    // 跨表查询
    var users []User
    sharding.CrossTableQuery(db, hashStrategy, &users, func(tx *gorm.DB) *gorm.DB {
        return tx.Where("status = ?", "active")
    })
}
```

> **Hash 分表结构升级提示**
>
> 如果历史 Hash 分表已经存在，而模型后来新增了字段（例如新增 `email` 列），可以直接选下面任一方式：
>
> - 保持原有 `db.Create(...)` 写法：`RegisterShardingWithAutoCreate(db, hashStrategy, &User{})`
> - 单次写入前显式补齐目标分表结构：`CreateShardedWithSchemaSync(...)`
> - 一次性批量升级历史 Hash 分表：`AutoMigrateExistingTables(...)`
>
> 完整可运行示例：`examples/hash_schema_sync/`

#### 时间分表

```go
// 按月分表（logs_200601, logs_200602, ...）
timeStrategy := sharding.NewTimeShardingStrategy(
    "logs", 
    "CreatedAt", 
    sharding.TimeShardingByMonth,
)

// 支持多种时间类型
// 1. time.Time 类型
timeStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs", "CreatedAt", sharding.TimeShardingByMonth, sharding.TimeFieldTypeTime)

// 2. int64 时间戳（秒）
timestampStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs", "CreatedAt", sharding.TimeShardingByDay, sharding.TimeFieldTypeTimestamp)

// 3. int64 时间戳（毫秒）
timestampMsStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs", "CreatedAt", sharding.TimeShardingByHour, sharding.TimeFieldTypeTimestampMs)

// 4. string 日期格式
dateStrategy := sharding.NewTimeShardingStrategyWithType(
    "logs", "CreatedAt", sharding.TimeShardingByDay, sharding.TimeFieldTypeDate)

// 查询指定时间范围的表（支持混合类型）
startTimestamp := time.Now().AddDate(0, -1, 0).Unix()
endTime := time.Now()
tableNames := timeStrategy.GetAllTableNamesInRangeWithValues("logs", startTimestamp, endTime)

// 清理旧分表 / 自动同步已存在分表结构：可通过同一个配置文件统一启用
now := time.Now()
_ = sharding.RegisterShardingWithConfigFile(
    db,
    timeStrategy,
    &Log{},
    "examples/time_cleanup/config.json",
)

// 正常写入时会自动建表、同步已存在分表结构，并根据配置清理过期分表
_ = db.Create(&Log{CreatedAt: now, Message: "hello"}).Error

// config.json 示例：
// {
//   "autoCreateTable": true,
//   "autoUpdateSchema": true,
//   "timeSharding": {
//     "autoCleanupPolicies": {
//       "default": {"enabled": true, "retainCount": 1, "minInterval": "1h"},
//       "byUnit": {
//         "day": {"enabled": true, "retainCount": 7, "minInterval": "30m"},
//         "hour": {"enabled": true, "retainCount": 24, "minInterval": "15m"}
//       },
//       "byBaseTable": {
//         "logs": {"enabled": true, "retainCount": 3, "minInterval": "0s"}
//       }
//     }
//   }
// }

// 你可以对 logs / metrics / traces / audits 等多个时间分表策略重复使用同一个配置文件；
// 系统会按优先级自动选择策略：byBaseTable > byUnit > default。
// 如果要一次性注册多个策略，可使用 RegisterShardingsWithConfigFile(...)
// 参考完整示例：examples/time_cleanup_multi/

// 如需手工执行一次清理，也可以继续使用 CleanupTimeTablesRetainingRecent
```

#### 已存在分表结构自动更新

```go
// Hash 分表：假设 users_3 已经存在，但还是旧结构（例如没有 email 字段）
type LegacyUser struct {
    ID     uint   `gorm:"primarykey;column:id"`
    UserID int64  `gorm:"column:user_id;not null"`
    Name   string `gorm:"column:name"`
}

userID := int64(123)
legacyHashTable := hashStrategy.GetTableName("users", userID)
_ = db.Table(legacyHashTable).AutoMigrate(&LegacyUser{})

// 方式 1：单次写入前，自动同步目标 Hash 分表结构
_ = sharding.CreateShardedWithSchemaSync(db, hashStrategy, &User{
    UserID: userID,
    Name:   "Alice",
    Email:  "alice@example.com",
}, &User{})

// 方式 2：使用 RegisterShardingWithAutoCreate 后，正常 db.Create(...) 命中已有分表时也会自动同步
_ = sharding.RegisterShardingWithAutoCreate(db, hashStrategy, &User{})
_ = db.Create(&User{UserID: 124, Name: "Bob", Email: "bob@example.com"}).Error

// 方式 3：批量同步数据库中所有已存在的该 Hash 分表结构
_ = sharding.AutoMigrateExistingTables(db, hashStrategy, &User{})

// Time 分表同样支持：
type LegacyLog struct {
    ID        uint      `gorm:"primarykey;column:id"`
    CreatedAt time.Time `gorm:"column:created_at;not null"`
    Message   string    `gorm:"column:message"`
}

targetTime := time.Date(2027, 3, 15, 10, 0, 0, 0, time.UTC)
legacyTimeTable := timeStrategy.GetTableName("logs", targetTime)
_ = db.Table(legacyTimeTable).AutoMigrate(&LegacyLog{})

_ = sharding.CreateShardedWithSchemaSync(db, timeStrategy, &Log{
    CreatedAt: targetTime,
    Message:   "schema evolved",
    Level:     "INFO",
}, &Log{})

_ = sharding.AutoMigrateExistingTables(db, timeStrategy, &Log{})
```

说明：当前增强主要面向 **新增字段/新增列** 这类 GORM `AutoMigrate` 可安全处理的结构演进。
字段删除、字段重命名、复杂索引差异等高风险 DDL，仍建议由业务方显式执行迁移脚本。

完整可运行示例：`examples/hash_schema_sync/`、`examples/auto_migrate/`

#### 跨表分页

```go
var users []User
paginator, _ := sharding.CrossTablePaginate(
    db, 
    hashStrategy, 
    &users, 
    1,      // 页码
    10,     // 每页数量
    func(tx *gorm.DB) *gorm.DB {
        return tx.Order("id DESC")
    },
)
fmt.Printf("Total: %d, Pages: %d\n", paginator.Total, paginator.TotalPages)
```

#### 跨表连接查询

```go
// 连接两个分表
userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
orderStrategy := sharding.NewHashShardingStrategy("orders", "UserID", 4)

var results []map[string]interface{}
sharding.CrossTableJoin(
    db,
    userStrategy,
    orderStrategy,
    sharding.LeftJoin,
    "users.user_id = orders.user_id",
    &results,
    func(tx *gorm.DB) *gorm.DB {
        return tx.Where("users.user_id = ?", 123)
    },
)
```

#### 两个分表的连接分页（Hash 分表）

```go
userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
orderStrategy := sharding.NewHashShardingStrategy("orders", "UserID", 4)

type JoinPageRow struct {
    UserID   int64  `gorm:"column:user_id"`
    UserName string `gorm:"column:user_name"`
    OrderID  int64  `gorm:"column:order_id"`
}

var rows []JoinPageRow
paginator, _ := sharding.CrossTableJoinPaginate(
    db,
    userStrategy,
    orderStrategy,
    sharding.LeftJoin,
    "users.user_id = orders.user_id",
    &rows,
    1,
    10,
    func(tx *gorm.DB) *gorm.DB {
        return tx.
            Select("users.user_id, users.name AS user_name, orders.order_id").
            Order("users.user_id ASC")
    },
)
fmt.Printf("Total: %d, Pages: %d\n", paginator.Total, paginator.TotalPages)
```

#### 支持时间范围的两表连接分页（可用于 Time+Time 或 Hash+Time）

```go
logStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)
eventStrategy := sharding.NewTimeShardingStrategy("events", "CreatedAt", sharding.TimeShardingByMonth)

type LogEventRow struct {
    LogID     int64  `gorm:"column:log_id"`
    EventID   int64  `gorm:"column:event_id"`
    EventName string `gorm:"column:event_name"`
}

var rows []LogEventRow
paginator, _ := sharding.CrossTableJoinPaginateWithTimeRange(
    db,
    logStrategy,
    eventStrategy,
    sharding.LeftJoin,
    "logs.log_id = events.log_id",
    &rows,
    1,
    20,
    func(tx *gorm.DB) *gorm.DB {
        return tx.Select("logs.log_id, events.event_id, events.name AS event_name")
    },
    time.Date(2026, 3, 31, 23, 59, 59, 0, time.UTC),
    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), // 起止时间写反也会自动纠正
)
fmt.Printf("Range Total: %d\n", paginator.Total)
```

#### 多表连接查询（3个及以上表）

```go
// 连接用户、订单、支付三个分表
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

sharding.CrossTableMultiJoin(db, config, &results, func(tx *gorm.DB) *gorm.DB {
    return tx.Select("users.name, orders.order_id, payments.amount")
})
```

#### 多表连接查询分页

```go
// 三表连接查询分页
paginator, _ := sharding.CrossTableMultiJoinPaginate(
    db,
    config,
    &results,
    1,      // 页码
    10,     // 每页数量
    func(tx *gorm.DB) *gorm.DB {
        return tx.Select("users.name, orders.order_id, payments.amount").
            Order("users.user_id DESC")
    },
)

// 优化的多表连接分页（已知连接键值）
joinKeys := map[string]interface{}{"user_id": 123}
optimizedPaginator, _ := sharding.CrossTableMultiJoinPaginateOptimized(
    db, config, joinKeys, &results, 1, 10, queryBuilder,
)

// 注意：在 queryBuilder 中使用表别名（基础表名），如 users.user_id
// 系统会自动为表设置别名，别名就是基础表名
// 多表连接查询和计数都会自动去重，避免重复数据
```

#### 自定义分表策略

```go
// 方式 1: 使用自定义函数
customFunc := func(baseTableName string, shardingValue interface{}) string {
    // 自定义分表逻辑
    return fmt.Sprintf("%s_%v", baseTableName, shardingValue)
}

customStrategy := sharding.NewCustomShardingStrategy(
    "products",
    "Category",
    customFunc,
    nil, // 使用默认值提取
    nil, // 使用默认获取所有表名
)

// 方式 2: 使用内置的范围分表
rangeStrategy := sharding.NewRangeShardingStrategy("products", "ProductID", 10000, 10)

// 方式 3: 使用内置的取模分表
moduloStrategy := sharding.NewModuloShardingStrategy("products", "ProductID", 4)
```

## 项目结构

```
x2-sharding-module/
├── sharding/           # 核心分表模块
│   ├── sharding.go          # 分表策略接口和基础功能
│   ├── hash_sharding.go     # Hash 分表实现
│   ├── time_sharding.go     # 时间分表实现
│   ├── cross_table_query.go # 跨表查询功能
│   ├── pagination.go        # 跨表分页功能
│   ├── join_query.go        # 跨表连接查询
│   └── helper.go            # 辅助工具函数
├── examples/          # 示例代码
│   ├── internal/models/      # 示例共享模型
│   ├── hash_sharding/main.go
│   ├── hash_schema_sync/main.go
│   ├── time_sharding/main.go
│   ├── time_cleanup/main.go
│   ├── time_cleanup/config.json
│   ├── time_cleanup_multi/main.go
│   ├── time_cleanup_multi/config.json
│   ├── join/main.go
│   ├── multi_join/main.go
│   └── examples/README.md
├── go.mod
├── README.md
└── USAGE.md          # 详细使用文档
```

## 详细文档

- [使用指南 (USAGE.md)](USAGE.md) - 完整的使用说明和最佳实践
- [示例索引 (examples/README.md)](examples/README.md) - 各种使用场景的示例入口与运行说明

## 核心 API

### 分表策略

- `NewHashShardingStrategy(baseTableName, shardingKey string, tableCount int)` - 创建 Hash 分表策略
- `NewTimeShardingStrategy(baseTableName, timeField string, unit TimeShardingUnit)` - 创建时间分表策略

### 数据库连接

- `OpenMySQLWithAutoCreateDB(dsn, config)` - 打开数据库连接，自动创建数据库（如果不存在）
- `OpenWithAutoCreateDB(dsn, config, charset, collation)` - 打开数据库连接（自定义字符集）
- `ParseDSN(dsn)` - 解析 DSN 字符串
- `DatabaseExists(db, databaseName)` - 检查数据库是否存在
- `CreateDatabase(db, databaseName, charset, collation)` - 创建数据库
- `EnsureDatabaseExists(db, databaseName, charset, collation)` - 确保数据库存在

### 自动创建分表

- `AutoMigrate(db, strategy, model, options)` - 自动创建所有分表
- `AutoMigrateTimeSharding(db, strategy, model, options)` - 按时间范围自动创建时间分表
- `AutoMigrateExistingTables(db, strategy, model)` - 批量同步数据库中已存在的分表结构
- `RegisterShardingWithOptions(db, strategy, options)` - 使用统一选项注册分表策略
- `RegisterShardingWithConfigFile(db, strategy, model, filePath)` - 使用 JSON 配置文件注册分表策略
- `RegisterShardingsWithConfigFile(db, filePath, registrations)` - 使用同一个 JSON 配置文件批量注册多个分表策略
- `LoadRegisterShardingOptionsFromJSON(filePath, model)` - 从 JSON 配置文件加载通用注册选项
- `LoadRegisterShardingOptionsForStrategyFromJSON(filePath, strategy, model)` - 按当前策略解析共享 JSON 配置文件中的自动清理策略（支持 `default` / `byUnit` / `byBaseTable`）
- `CleanupTimeTablesRetainingRecent(db, strategy, options)` - 按保留最近 N 个时间分片清理旧表

### 连接查询与分页

- `CrossTableJoin(...)` - 两个分表的连接查询
- `CrossTableJoinCount(...)` - 两个分表的连接计数
- `CrossTableJoinPaginate(...)` - 两个分表的连接分页（适用于 Hash 分表）
- `CrossTableJoinWithTimeRange(...)` - 支持时间范围的两表连接查询（适用于时间分表或混合策略）
- `CrossTableJoinCountWithTimeRange(...)` - 支持时间范围的两表连接计数（适用于时间分表或混合策略）
- `CrossTableJoinPaginateWithTimeRange(...)` - 支持时间范围的两表连接分页（适用于时间分表或混合策略）
- `CrossTableMultiJoin(...)` - 多表连接查询
- `CrossTableMultiJoinCount(...)` - 多表连接计数
- `CrossTableMultiJoinPaginate(...)` - 多表连接分页
- `CrossTableMultiJoinOptimized(...)` - 基于连接键的多表优化连接查询
- `CrossTableMultiJoinPaginateWithTimeRange(...)` - 多表时间分表连接分页
- `CrossTableMultiJoinCountWithTimeRange(...)` - 多表时间分表连接计数
- `CrossTableMultiJoinPaginateOptimized(...)` - 基于连接键的多表优化连接分页
- `RegisterShardingWithAutoCreate(db, strategy, model)` - 注册策略并启用自动创建；目标分表已存在时自动同步表结构
- `CreateSharded(db, strategy, value)` - 显式按分表策略写入记录
- `CreateShardedWithAutoCreate(db, strategy, value, model)` - 显式按分表策略写入，并自动建表 / 同步已存在分表结构
- `CreateShardedWithSchemaSync(db, strategy, value, model)` - 显式按分表策略写入，并确保目标分表结构与当前模型同步
- `EnsureTableExists(db, strategy, shardingValue, model)` - 确保表存在
- `EnsureTableSchema(db, strategy, shardingValue, model)` - 确保分表存在，并同步已存在分表结构
- `AutoMigrateAll(db, strategies, models, options)` - 批量自动创建所有策略的分表

### 查询操作

- `CrossTableQuery(db, strategy, dest, queryBuilder)` - 跨表查询
- `CrossTableQueryWithTimeRange(db, strategy, dest, queryBuilder, startValue, endValue)` - 时间分表范围查询
- `CrossTablePaginate(db, strategy, dest, page, pageSize, queryBuilder)` - 跨表分页
- `CrossTableCountWithTimeRange(db, strategy, queryBuilder, startValue, endValue)` - 时间分表范围计数
- `CrossTablePaginateWithTimeRange(db, strategy, dest, page, pageSize, queryBuilder, startValue, endValue)` - 时间分表范围分页
- `CrossTableJoin(db, strategy1, strategy2, joinType, onCondition, dest, queryBuilder)` - 跨表连接
- `CrossTableCount(db, strategy, queryBuilder)` - 跨表计数

### 多表连接查询

- `CrossTableMultiJoin(db, config, dest, queryBuilder)` - 多表连接查询

## 注意事项

1. **性能考虑** - 跨表查询会查询所有分表，大数据量时注意性能影响
2. **表不存在** - 跨表查询时，不存在的表会被自动跳过
3. **事务支持** - 支持事务，但跨表查询在事务中可能有限制

## 系统要求

- Go 1.21+
- GORM v1.25+
- MySQL 5.7+ / MariaDB 10.2+

## 贡献

欢迎提交 Issue 和 Pull Request！

## License

MIT License


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

// 插入数据时，如果表不存在会自动创建；如果表已存在，则会自动同步新增字段
user := &User{UserID: 123, Name: "John"}
db.Create(user) // 会自动路由到实际分表，并在目标表不存在时自动创建
```

说明：

- `RegisterShardingWithAutoCreate` 的推荐用法是直接 `db.Create(&model)`；
- `RegisterShardingWithAutoCreate` 现在默认也会在目标分表“已存在”时执行一次懒 `AutoMigrate`，适合模型新增字段后的平滑升级；
- 如果你调用了 `db.Table(...)`，也建议传入注册时的 `model`，这样插件可以按模型类型识别并改写到真实分表；
- 如果你不想依赖回调，也可以直接使用显式封装 `CreateShardedWithAutoCreate(...)`。

```go
// 显式按分表策略写入，并自动建表
err = sharding.CreateShardedWithAutoCreate(db, hashStrategy, user, &User{})
```

#### 方式 2.0: 已存在分表自动同步表结构

如果你的分表已经存在，而模型后来新增了字段，推荐按下面顺序选择：

1. **单次写入前补齐目标分表**：`CreateShardedWithSchemaSync(...)`
2. **沿用原有 `db.Create(...)` 写法**：`RegisterShardingWithAutoCreate(...)`
3. **批量升级历史分表**：`AutoMigrateExistingTables(...)`

下面以 Hash 分表为例：

```go
type LegacyUser struct {
    ID     uint   `gorm:"primarykey;column:id"`
    UserID int64  `gorm:"column:user_id;not null"`
    Name   string `gorm:"column:name"`
}

type User struct {
    ID     uint   `gorm:"primarykey;column:id"`
    UserID int64  `gorm:"column:user_id;not null"`
    Name   string `gorm:"column:name"`
    Email  string `gorm:"column:email"`
}

tableName := hashStrategy.GetTableName("users", int64(123))
_ = db.Table(tableName).AutoMigrate(&LegacyUser{}) // 模拟旧结构分表

// 方式 1：单次写入前，确保目标分表结构与当前模型同步
err = sharding.CreateShardedWithSchemaSync(db, hashStrategy, &User{
    UserID: 123,
    Name:   "John",
    Email:  "john@example.com",
}, &User{})

// 方式 2：批量同步数据库中“已存在”的该策略所有分表
err = sharding.AutoMigrateExistingTables(db, hashStrategy, &User{})
```

如果你仍希望保持最少改造，也可以直接：

```go
err = sharding.RegisterShardingWithAutoCreate(db, hashStrategy, &User{})
err = db.Create(&User{UserID: 124, Name: "Bob", Email: "bob@example.com"}).Error
```

完整可运行示例：`examples/hash_schema_sync/`

说明：当前能力主要覆盖 GORM `AutoMigrate` 可安全处理的结构演进，尤其是**新增字段 / 新增列**。
对于删除列、重命名列、复杂索引变更等高风险 DDL，仍建议使用显式迁移脚本。

#### 方式 2.1: 通过配置启用时间分表自动清理

如果你希望“正常写入时，自动根据当前时间分表策略清理过期表”，推荐把自动建表、自动更新分表结构和自动清理统一放到注册配置里：

```go
options := sharding.RegisterShardingOptions{
    Model:           &Log{},
    AutoCreateTable: true,
    AutoUpdateSchema: true,
    TimeSharding: &sharding.TimeShardingRegisterOptions{
        AutoCleanup: &sharding.TimeShardingAutoCleanupOptions{
            Enabled:     true,
            RetainCount: 3,          // 保留当前分片在内最近 3 个分片
            MinInterval: time.Hour,  // 至少间隔 1 小时才再次执行清理
        },
    },
}

err := sharding.RegisterShardingWithOptions(db, timeStrategy, options)
if err != nil {
    log.Fatal(err)
}

// 后续正常写入时，会自动建表 / 同步分表结构，并按当前时间分片触发懒清理
err = db.Create(&Log{
    CreatedAt: time.Now(),
    Message:   "auto cleanup on write",
}).Error
```

如果你更偏好配置文件，也可以直接使用 JSON：

```json
{
  "autoCreateTable": true,
  "autoUpdateSchema": true,
  "timeSharding": {
    "autoCleanupPolicies": {
      "default": {
        "enabled": true,
        "retainCount": 1,
        "minInterval": "1h"
      },
      "byUnit": {
        "day": {
          "enabled": true,
          "retainCount": 7,
          "minInterval": "30m"
        },
        "hour": {
          "enabled": true,
          "retainCount": 24,
          "minInterval": "15m"
        }
      },
      "byBaseTable": {
        "logs": {
          "enabled": true,
          "retainCount": 3,
          "minInterval": "0s"
        },
        "audit_logs": {
          "enabled": true,
          "retainCount": 2,
          "minInterval": "24h"
        }
      }
    }
  }
}
```

```go
err := sharding.RegisterShardingWithConfigFile(
    db,
    timeStrategy,
    &Log{},
    "examples/time_cleanup/config.json",
)
```

如果系统启动时需要一次性注册多个时间分表策略，也可以批量方式：

```go
err := sharding.RegisterShardingsWithConfigFile(
    db,
    "examples/time_cleanup_multi/config.json",
    []sharding.ConfigFileShardingRegistration{
        {Strategy: logsStrategy, Model: &Log{}},
        {Strategy: metricsStrategy, Model: &Metric{}},
        {Strategy: tracesStrategy, Model: &Trace{}},
        {Strategy: auditStrategy, Model: &AuditLog{}},
    },
)
```

完整可运行示例可参考：`examples/time_cleanup_multi/`

同一个配置文件可以被多个时间分表策略复用，例如：

- `logs` 按月分表，保留最近 3 个分片；
- `metrics` 按日分表，按 `byUnit.day` 保留最近 7 个分片；
- `traces` 按小时分表，按 `byUnit.hour` 保留最近 24 个分片；
- `audit_logs` 按年分表，按 `byBaseTable.audit_logs` 保留最近 2 个分片；
- 其他未单独声明的时间分表策略，则回退到 `default`。

优先级固定为：

1. `byBaseTable`
2. `byUnit`
3. `default`

说明：

- 自动清理只对 `TimeShardingStrategy` 生效；
- `RetainCount` 包含当前写入记录所在的时间分片；
- `MinInterval` 用于避免每次写入都做一次清理；
- `autoUpdateSchema` 开启后，命中已有时间分表时也会自动同步新增字段；
- 清理会根据当前时间分表策略自动推导范围，无需业务方再手工调用清理方法。

#### 方式 3: 手动确保表存在

```go
// 在插入数据前确保表存在
err := sharding.EnsureTableExists(db, hashStrategy, 123, &User{})
if err == nil {
    db.Table(tableName).Create(user)
}
```

如果你希望“表存在即可，但同时把结构补齐到当前模型”，可以使用：

```go
err := sharding.EnsureTableSchema(db, hashStrategy, 123, &User{})
```

#### 方式 4: 手工按保留最近 N 个时间分片清理旧表（高级）

如果你不想依赖“写入时自动清理”，也可以使用唯一保留的高级手工入口：

- `CleanupTimeTablesRetainingRecent(...)` 适用于任意时间分片单位；
- `RetainCount` 包含 `ReferenceTime` 所在当前分片；
- `ReferenceTime` 传零值时默认使用 `time.Now()`；
- `DryRun` 可先预览，再正式删除。

```go
timeStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)

preview, err := sharding.CleanupTimeTablesRetainingRecent(db, timeStrategy, sharding.CleanupRetainRecentTimeTablesOptions{
    RetainCount:   3,
    ReferenceTime: time.Now(),
    DryRun:        true,
})
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Would drop: %v\n", preview.ExpiredTables)

result, err := sharding.CleanupTimeTablesRetainingRecent(db, timeStrategy, sharding.CleanupRetainRecentTimeTablesOptions{
    RetainCount:   3,
    ReferenceTime: time.Now(),
})
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Dropped: %v\n", result.DroppedTables)
```

以按月分表为例：当 `ReferenceTime = 2026-03-27` 且 `RetainCount = 3` 时，保留的是：

- `logs_202601`
- `logs_202602`
- `logs_202603`

更早的月表会被识别为可清理对象。

说明：内部仍然基于时间分片边界计算实际清理范围，因此月/日/年分表都能正确处理。

因此当 `ExpireBefore = 2026-03-01 00:00:00` 时，`logs_202601` 和 `logs_202602` 都会被视为已过期。

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

#### 解析分表时间与判断是否过期

```go
startTime, endTime, err := timeStrategy.GetTableTimeRange("logs_202602")
if err != nil {
    log.Fatal(err)
}

expired, err := timeStrategy.IsTableExpired(
    "logs_202602",
    time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("range=[%s, %s), expired=%v\n", startTime, endTime, expired)
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

连接查询支持在同一个方法里混用不同的分表策略，例如：主表使用 Hash 分表、关联表使用 Time 分表。
如果传入时间范围，只有时间分表的一侧会按时间范围裁剪，Hash 分表的一侧仍按其全部分表参与查询。

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

#### 两个表的连接分页

当只连接两个 Hash 分表时，推荐直接使用 `CrossTableJoinPaginate`：

```go
userStrategy := sharding.NewHashShardingStrategy("users", "UserID", 4)
orderStrategy := sharding.NewHashShardingStrategy("orders", "UserID", 4)

type JoinPageRow struct {
  UserID   int64  `gorm:"column:user_id"`
  UserName string `gorm:"column:user_name"`
  OrderID  int64  `gorm:"column:order_id"`
}

var pageResults []JoinPageRow

paginator, err := sharding.CrossTableJoinPaginate(
  db,
  userStrategy,
  orderStrategy,
  sharding.LeftJoin,
  "users.user_id = orders.user_id",
  &pageResults,
  1,  // 页码
  20, // 每页数量
  func(tx *gorm.DB) *gorm.DB {
    return tx.
      Select("users.user_id, users.name AS user_name, orders.order_id").
      Where("users.user_id > ?", 0).
      Order("users.user_id ASC")
  },
)

fmt.Printf("Page=%d Total=%d TotalPages=%d\n", paginator.Page, paginator.Total, paginator.TotalPages)
```

#### 支持时间范围的两表连接分页（可用于 Time+Time 或 Hash+Time）

当连接两个时间分表，或者主表是 Hash 分表、关联表是 Time 分表时，都可以使用 `CrossTableJoinPaginateWithTimeRange` 并直接传入时间范围：

```go
logStrategy := sharding.NewTimeShardingStrategy("logs", "CreatedAt", sharding.TimeShardingByMonth)
eventStrategy := sharding.NewTimeShardingStrategy("events", "CreatedAt", sharding.TimeShardingByMonth)

type LogEventRow struct {
  LogID     int64  `gorm:"column:log_id"`
  EventID   int64  `gorm:"column:event_id"`
  EventName string `gorm:"column:event_name"`
}

var timePageResults []LogEventRow

paginator, err := sharding.CrossTableJoinPaginateWithTimeRange(
  db,
  logStrategy,
  eventStrategy,
  sharding.LeftJoin,
  "logs.log_id = events.log_id",
  &timePageResults,
  1,
  20,
  func(tx *gorm.DB) *gorm.DB {
    return tx.
      Select("logs.log_id, events.event_id, events.name AS event_name").
      Order("logs.log_id ASC")
  },
  time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC),
  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), // 起止时间写反也会自动纠正
)

fmt.Printf("Range Total=%d Rows=%d\n", paginator.Total, len(timePageResults))
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

#### Hash 分表的分页连接查询示例（3 个及以上表）

```go
// 适用于 3 个及以上表的 Hash 分表连接分页
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

#### 时间分表的分页连接查询示例（3 个及以上表）

```go
// 适用于 3 个及以上表的时间分表连接分页
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
   - 两表连接分页和多表连接查询都会自动对结果进行去重，避免因表组合产生的重复数据
   - **计数函数也会自动去重**，确保 `CrossTableJoinCount` / `CrossTableMultiJoinCount` 返回的数量与查询结果一致
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

完整示例请参考 `examples/README.md`：

- `hash_sharding/main.go` - Hash 分表示例
- `time_sharding/main.go` - 时间分表示例
- `join/main.go` - 跨表连接查询示例
- `join_pagination/main.go` - Hash + Time 混合连接分页示例
- `multi_join_pagination/main.go` - 多表连接分页示例
- `internal/models/models.go` - 示例共享模型定义

## 注意事项

1. **表不存在错误**：跨表查询时，如果某个分表不存在，会自动跳过，不会报错
2. **性能考虑**：跨表查询会查询所有分表，对于大数据量可能影响性能
3. **事务支持**：分表操作支持事务，但跨表查询在事务中可能有限制
4. **表名大小写**：MySQL 在 Windows/macOS 上表名大小写不敏感，但在 Linux 上敏感

## 常见问题

### Q: 如何扩展分表数量？

A: Hash 分表扩展需要迁移数据。建议在初期设计时就预留足够的分表数量，或者使用一致性 Hash。

### Q: 时间分表如何清理旧数据？

A: 推荐两种方式：

- **推荐**：在 `RegisterShardingWithOptions(...)` 或 `RegisterShardingWithConfigFile(...)` 中开启时间分表自动清理，让系统在正常写入时按当前策略自动清理；
- **高级手工方式**：使用 `CleanupTimeTablesRetainingRecent(...)` 按“保留最近 N 个时间分片”的规则执行一次清理。

除非你非常确定当前表就是待删除的历史分表，否则不建议业务侧直接手写 `DROP TABLE ...`。

### Q: 如果分表结构发生变化（例如新增字段），会自动更新已存在的分表吗？

A: 现在支持，推荐按下面三种方式选择：

- **最省事**：使用 `RegisterShardingWithAutoCreate(...)`，后续 `db.Create(...)` 命中已有分表时会自动同步结构；
- **显式单表同步**：使用 `CreateShardedWithSchemaSync(...)` 或 `EnsureTableSchema(...)`；
- **批量同步历史分表**：使用 `AutoMigrateExistingTables(...)`。

当前实现主要适合新增列这类 `AutoMigrate` 安全支持的变更；删除列、重命名列、复杂索引调整等，仍建议通过正式迁移脚本处理。

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


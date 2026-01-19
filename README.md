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
- ✅ **自动创建分表** - 支持自动创建所有分表，插入数据时自动创建表
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
    tableName := hashStrategy.GetTableName("users", 123)
    db.Table(tableName).Create(user)
    
    // 跨表查询
    var users []User
    sharding.CrossTableQuery(db, hashStrategy, &users, func(tx *gorm.DB) *gorm.DB {
        return tx.Where("status = ?", "active")
    })
}
```

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
```

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
│   ├── hash_sharding_example.go
│   ├── time_sharding_example.go
│   └── join_example.go
├── go.mod
├── README.md
└── USAGE.md          # 详细使用文档
```

## 详细文档

- [使用指南 (USAGE.md)](USAGE.md) - 完整的使用说明和最佳实践
- [示例代码](examples/) - 各种使用场景的示例

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
- `RegisterShardingWithAutoCreate(db, strategy, model)` - 注册策略并启用自动创建
- `EnsureTableExists(db, strategy, shardingValue, model)` - 确保表存在
- `AutoMigrateAll(db, strategies, models, options)` - 批量自动创建所有策略的分表

### 查询操作

- `CrossTableQuery(db, strategy, dest, queryBuilder)` - 跨表查询
- `CrossTablePaginate(db, strategy, dest, page, pageSize, queryBuilder)` - 跨表分页
- `CrossTableJoin(db, strategy1, strategy2, joinType, onCondition, dest, queryBuilder)` - 跨表连接
- `CrossTableCount(db, strategy, queryBuilder)` - 跨表计数

### 多表连接查询

- `CrossTableMultiJoin(db, config, dest, queryBuilder)` - 多表连接查询
- `CrossTableMultiJoinCount(db, config, queryBuilder)` - 多表连接查询计数
- `CrossTableMultiJoinPaginate(db, config, dest, page, pageSize, queryBuilder)` - 多表连接查询分页
- `CrossTableMultiJoinPaginateOptimized(db, config, joinKeys, dest, page, pageSize, queryBuilder)` - 优化的多表连接查询分页

### 辅助工具

- `ShardingHelper` - 分表辅助工具类，简化常用操作
- `GenerateTableNames()` - 生成所有分表的创建 SQL
- `CreateAllHashTables()` - 批量创建 Hash 分表

## 注意事项

1. **表结构一致性** - 所有分表必须具有相同的表结构
2. **性能考虑** - 跨表查询会查询所有分表，大数据量时注意性能影响
3. **表不存在** - 跨表查询时，不存在的表会被自动跳过
4. **事务支持** - 支持事务，但跨表查询在事务中可能有限制

## 系统要求

- Go 1.21+
- GORM v1.25+
- MySQL 5.7+ / MariaDB 10.2+

## 贡献

欢迎提交 Issue 和 Pull Request！

## License

MIT License


# Examples

本目录中的示例已经按功能拆分为独立子目录，避免多个 `main()` 和重复结构体定义互相冲突。
公共示例模型统一放在 `internal/models/models.go` 中。

## 运行前提

大多数示例默认使用下面的 MySQL DSN，请按本地环境自行调整：

```text
root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local
```

运行示例时，请在仓库根目录执行：

```bash
go run ./examples/hash_sharding
go run ./examples/time_sharding
go run ./examples/time_cleanup
go run ./examples/time_cleanup_multi
go run ./examples/join
go run ./examples/join_pagination
go run ./examples/multi_join_pagination
```

## 示例索引

- `auto_migrate/main.go`  
  演示自动创建 Hash / Time / Range / Modulo 分表，以及按需建表。

- `custom_sharding/main.go`  
  演示自定义分表函数、按类别分表、复杂表名生成逻辑。

- `db_connection/main.go`  
  演示自动创建数据库、DSN 解析和数据库存在性检查。

- `hash_sharding/main.go`  
  演示基于 Hash 的分表、跨表查询和分页。

- `join/main.go`  
  演示两个分表的连接查询，以及跨分表 JOIN。

- `join_pagination/main.go`  
  演示 `Hash 主表 + Time 子表` 的混合连接分页查询，以及时间范围分页。

- `multi_join/main.go`  
  演示 3 个及以上分表的多表连接查询和优化查询。

- `multi_join_pagination/main.go`  
  演示多表连接分页、优化分页、时间范围分页。

- `time_sharding/main.go`  
  演示按时间分表、时间范围内表名推导、跨表分页。

- `time_cleanup/main.go`  
  演示通过 `config.json` 启用时间分表自动建表与自动清理，正常写入时自动触发。

- `time_cleanup/config.json`  
  时间分表自动清理配置示例，支持通过同一个文件为多个时间分表策略按 `baseTable` / `unit` 配置不同的保留数量和清理间隔。

- `time_cleanup_multi/main.go`  
  演示 `month/day/hour/year` 多个时间分表策略通过同一个配置文件批量注册，并在写入时各自按不同策略自动清理。

- `time_cleanup_multi/config.json`  
  多时间分表策略共享配置文件示例，展示 `default / byUnit / byBaseTable` 三层优先级。

- `time_types/main.go`  
  演示 `time.Time`、秒级时间戳、毫秒时间戳、日期字符串等多种时间字段类型。

- `internal/models/models.go`  
  示例共享模型定义，避免示例之间重复声明 `User`、`Log`、`Product` 等结构体。


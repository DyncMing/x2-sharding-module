package models

import "time"

// UserWithEmail 适用于包含 email 字段的用户表示例。
type UserWithEmail struct {
	ID     uint   `gorm:"primarykey;column:id"`
	UserID int64  `gorm:"column:user_id;not null;index"`
	Name   string `gorm:"column:name"`
	Email  string `gorm:"column:email"`
}

// UserBasic 适用于仅包含基础用户信息的连接查询示例。
type UserBasic struct {
	ID     uint   `gorm:"primarykey;column:id"`
	UserID int64  `gorm:"column:user_id;not null"`
	Name   string `gorm:"column:name"`
}

// Log 适用于按 created_at 分表的日志示例。
type Log struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null;index"`
	Message   string    `gorm:"column:message"`
	Level     string    `gorm:"column:level"`
}

// Metric 适用于按天分表的指标示例。
type Metric struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null;index"`
	Message   string    `gorm:"column:message"`
}

// Trace 适用于按小时分表的链路追踪示例。
type Trace struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null;index"`
	Message   string    `gorm:"column:message"`
}

// AuditLog 适用于按年分表的审计日志示例。
type AuditLog struct {
	ID        uint      `gorm:"primarykey;column:id"`
	CreatedAt time.Time `gorm:"column:created_at;not null;index"`
	Message   string    `gorm:"column:message"`
}

// Event 适用于按 created_at 分表的事件表示例。
type Event struct {
	ID        uint      `gorm:"primarykey;column:id"`
	EventID   int64     `gorm:"column:event_id;not null;index"`
	UserID    int64     `gorm:"column:user_id;not null;index"`
	Name      string    `gorm:"column:name"`
	CreatedAt time.Time `gorm:"column:created_at;not null;index"`
}

// JoinOrder 适用于双表连接示例中的订单结构。
type JoinOrder struct {
	ID        uint      `gorm:"primarykey;column:id"`
	UserID    int64     `gorm:"column:user_id;not null"`
	Amount    float64   `gorm:"column:amount"`
	Status    string    `gorm:"column:status"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

// MultiJoinOrder 适用于多表连接和分页示例中的订单结构。
type MultiJoinOrder struct {
	ID      uint    `gorm:"primarykey;column:id"`
	UserID  int64   `gorm:"column:user_id;not null"`
	OrderID int64   `gorm:"column:order_id;not null"`
	Amount  float64 `gorm:"column:amount"`
}

// Payment 适用于多表连接示例中的支付结构。
type Payment struct {
	ID      uint    `gorm:"primarykey;column:id"`
	OrderID int64   `gorm:"column:order_id;not null"`
	Amount  float64 `gorm:"column:amount"`
	Status  string  `gorm:"column:status"`
}

// Product 适用于自定义分表和范围分表示例。
type Product struct {
	ID        uint   `gorm:"primarykey;column:id"`
	ProductID int64  `gorm:"column:product_id;not null;index"`
	Name      string `gorm:"column:name"`
	Category  string `gorm:"column:category"`
}

// RangeProduct 适用于范围分表自动迁移示例。
type RangeProduct struct {
	ID        uint   `gorm:"primarykey;column:id"`
	ProductID int64  `gorm:"column:product_id;not null;index"`
	Name      string `gorm:"column:name"`
}

// ModuloOrder 适用于取模分表自动迁移示例。
type ModuloOrder struct {
	ID      uint   `gorm:"primarykey;column:id"`
	OrderID int64  `gorm:"column:order_id;not null;index"`
	Amount  string `gorm:"column:amount"`
}

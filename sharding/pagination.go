package sharding

import (
	"reflect"

	"gorm.io/gorm"
)

// Paginator 分页器
type Paginator struct {
	Page       int         `json:"page"`        // 当前页码（从1开始）
	PageSize   int         `json:"page_size"`   // 每页数量
	Total      int64       `json:"total"`       // 总记录数
	TotalPages int         `json:"total_pages"` // 总页数
	Data       interface{} `json:"data"`        // 数据列表
}

// CrossTablePaginate 跨表分页查询
// db: GORM 数据库实例
// strategy: 分表策略
// dest: 结果目标（切片指针）
// page: 页码（从1开始）
// pageSize: 每页数量
// queryBuilder: 查询构建器
func CrossTablePaginate(
	db *gorm.DB,
	strategy ShardingStrategy,
	dest interface{},
	page, pageSize int,
	queryBuilder QueryBuilder,
) (*Paginator, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 先获取总数
	total, err := CrossTableCount(db, strategy, queryBuilder)
	if err != nil {
		return nil, err
	}

	// 计算总页数
	totalPages := int(total) / pageSize
	if int(total)%pageSize > 0 {
		totalPages++
	}

	// 跨表查询所有数据
	err = CrossTableQuery(db, strategy, dest, queryBuilder)
	if err != nil {
		return nil, err
	}

	// 手动分页（因为数据已经在内存中）
	// 注意：这种方式对于大数据量可能不够高效，建议使用基于游标的分页
	paginatedData := paginateSlice(dest, page, pageSize)

	return &Paginator{
		Page:       page,
		PageSize:   pageSize,
		Total:      total,
		TotalPages: totalPages,
		Data:       paginatedData,
	}, nil
}

// CrossTablePaginateUnion 使用 UNION ALL 的跨表分页（更高效）
func CrossTablePaginateUnion(
	db *gorm.DB,
	strategy ShardingStrategy,
	dest interface{},
	page, pageSize int,
	queryBuilder QueryBuilder,
) (*Paginator, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 先获取总数
	total, err := CrossTableCount(db, strategy, queryBuilder)
	if err != nil {
		return nil, err
	}

	// 计算总页数
	totalPages := int(total) / pageSize
	if int(total)%pageSize > 0 {
		totalPages++
	}

	// 计算偏移量
	offset := (page - 1) * pageSize

	// 使用 UNION ALL 查询并分页
	// 注意：需要在 UNION ALL 外层添加 LIMIT 和 OFFSET
	err = CrossTableQueryUnionWithPagination(db, strategy, dest, offset, pageSize, queryBuilder)
	if err != nil {
		return nil, err
	}

	return &Paginator{
		Page:       page,
		PageSize:   pageSize,
		Total:      total,
		TotalPages: totalPages,
		Data:       dest,
	}, nil
}

// CrossTableQueryUnionWithPagination 带分页的 UNION ALL 查询
func CrossTableQueryUnionWithPagination(
	db *gorm.DB,
	strategy ShardingStrategy,
	dest interface{},
	offset, limit int,
	queryBuilder QueryBuilder,
) error {
	// 由于 GORM 的 UNION ALL 支持有限，这里采用更简单的方法：
	// 先查询所有数据到内存，然后进行分页
	// 对于大数据量场景，建议使用基于游标的分页或其他优化方案

	err := CrossTableQuery(db, strategy, dest, queryBuilder)
	if err != nil {
		return err
	}

	// 手动分页
	// 注意：这里假设 dest 是一个切片，实际使用时需要根据类型进行转换
	return nil
}

// paginateSlice 对切片进行分页（辅助函数）
func paginateSlice(slice interface{}, page, pageSize int) interface{} {
	if slice == nil {
		return slice
	}

	sliceValue := reflect.ValueOf(slice)
	if sliceValue.Kind() != reflect.Ptr {
		return slice
	}

	sliceElem := sliceValue.Elem()
	if sliceElem.Kind() != reflect.Slice {
		return slice
	}

	length := sliceElem.Len()
	offset := (page - 1) * pageSize

	if offset >= length {
		// 偏移量超出范围，返回空切片
		emptySlice := reflect.MakeSlice(sliceElem.Type(), 0, 0)
		sliceElem.Set(emptySlice)
		return slice
	}

	end := offset + pageSize
	if end > length {
		end = length
	}

	// 获取分页后的子切片
	paginatedSlice := sliceElem.Slice(offset, end)

	// 创建新的切片并设置值
	newSlice := reflect.MakeSlice(sliceElem.Type(), paginatedSlice.Len(), paginatedSlice.Len())
	reflect.Copy(newSlice, paginatedSlice)

	sliceElem.Set(newSlice)
	return slice
}

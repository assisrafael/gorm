package gorm

import (
	"fmt"
)

type search struct {
	db               *DB
	whereConditions  []map[string]interface{}
	orConditions     []map[string]interface{}
	notConditions    []map[string]interface{}
	havingConditions []map[string]interface{}
	joinConditions   []map[string]interface{}
	initAttrs        []interface{}
	assignAttrs      []interface{}
	selects          map[string]interface{}
	omits            []string
	orders           []interface{}
	preload          []searchPreload
	offset           interface{}
	limit            interface{}
	group            string
	tableName        string
	schemaName       string
	raw              bool
	Unscoped         bool
	ignoreOrderQuery bool
}

type searchPreload struct {
	schema     string
	conditions []interface{}
}

func (s *search) clone() *search {
	clone := search{
		db:               s.db,
		whereConditions:  make([]map[string]interface{}, len(s.whereConditions)),
		orConditions:     make([]map[string]interface{}, len(s.orConditions)),
		notConditions:    make([]map[string]interface{}, len(s.notConditions)),
		havingConditions: make([]map[string]interface{}, len(s.havingConditions)),
		joinConditions:   make([]map[string]interface{}, len(s.joinConditions)),
		initAttrs:        make([]interface{}, len(s.initAttrs)),
		assignAttrs:      make([]interface{}, len(s.assignAttrs)),
		selects:          s.selects,
		omits:            make([]string, len(s.omits)),
		orders:           make([]interface{}, len(s.orders)),
		preload:          make([]searchPreload, len(s.preload)),
		offset:           s.offset,
		limit:            s.limit,
		group:            s.group,
		tableName:        s.tableName,
		raw:              s.raw,
		Unscoped:         s.Unscoped,
		ignoreOrderQuery: s.ignoreOrderQuery,
	}
	for i, value := range s.whereConditions {
		clone.whereConditions[i] = value
	}
	for i, value := range s.orConditions {
		clone.orConditions[i] = value
	}
	for i, value := range s.notConditions {
		clone.notConditions[i] = value
	}
	for i, value := range s.havingConditions {
		clone.havingConditions[i] = value
	}
	for i, value := range s.joinConditions {
		clone.joinConditions[i] = value
	}
	for i, value := range s.initAttrs {
		clone.initAttrs[i] = value
	}
	for i, value := range s.assignAttrs {
		clone.assignAttrs[i] = value
	}
	for i, value := range s.omits {
		clone.omits[i] = value
	}
	for i, value := range s.orders {
		clone.orders[i] = value
	}
	for i, value := range s.preload {
		clone.preload[i] = value
	}
	return &clone
}

func (s *search) Where(query interface{}, values ...interface{}) *search {
	s.whereConditions = append(s.whereConditions, map[string]interface{}{"query": query, "args": values})
	return s
}

func (s *search) Not(query interface{}, values ...interface{}) *search {
	s.notConditions = append(s.notConditions, map[string]interface{}{"query": query, "args": values})
	return s
}

func (s *search) Or(query interface{}, values ...interface{}) *search {
	s.orConditions = append(s.orConditions, map[string]interface{}{"query": query, "args": values})
	return s
}

func (s *search) Attrs(attrs ...interface{}) *search {
	s.initAttrs = append(s.initAttrs, toSearchableMap(attrs...))
	return s
}

func (s *search) Assign(attrs ...interface{}) *search {
	s.assignAttrs = append(s.assignAttrs, toSearchableMap(attrs...))
	return s
}

func (s *search) Order(value interface{}, reorder ...bool) *search {
	if len(reorder) > 0 && reorder[0] {
		s.orders = []interface{}{}
	}

	if value != nil && value != "" {
		s.orders = append(s.orders, value)
	}
	return s
}

func (s *search) Select(query interface{}, args ...interface{}) *search {
	s.selects = map[string]interface{}{"query": query, "args": args}
	return s
}

func (s *search) Omit(columns ...string) *search {
	s.omits = columns
	return s
}

func (s *search) Limit(limit interface{}) *search {
	s.limit = limit
	return s
}

func (s *search) Offset(offset interface{}) *search {
	s.offset = offset
	return s
}

func (s *search) Group(query string) *search {
	s.group = s.getInterfaceAsSQL(query)
	return s
}

func (s *search) Having(query interface{}, values ...interface{}) *search {
	if val, ok := query.(*SqlExpr); ok {
		s.havingConditions = append(s.havingConditions, map[string]interface{}{"query": val.expr, "args": val.args})
	} else {
		s.havingConditions = append(s.havingConditions, map[string]interface{}{"query": query, "args": values})
	}
	return s
}

func (s *search) Joins(query string, values ...interface{}) *search {
	s.joinConditions = append(s.joinConditions, map[string]interface{}{"query": query, "args": values})
	return s
}

func (s *search) Preload(schema string, values ...interface{}) *search {
	var preloads []searchPreload
	for _, preload := range s.preload {
		if preload.schema != schema {
			preloads = append(preloads, preload)
		}
	}
	preloads = append(preloads, searchPreload{schema, values})
	s.preload = preloads
	return s
}

func (s *search) Raw(b bool) *search {
	s.raw = b
	return s
}

func (s *search) unscoped() *search {
	s.Unscoped = true
	return s
}

func (s *search) Table(name string) *search {
	s.tableName = name
	return s
}

func (s *search) getInterfaceAsSQL(value interface{}) (str string) {
	switch value.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		str = fmt.Sprintf("%v", value)
	default:
		s.db.AddError(ErrInvalidSQL)
	}

	if str == "-1" {
		return ""
	}
	return
}

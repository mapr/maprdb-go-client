package private_maprdb_go_client

import (
	"errors"
	"reflect"
)

// Query structure which lets users build an OJAI Query that can be executed on an OJAI DocumentStore.
type Query struct {
	content map[string]interface{}
	isBuilt bool
}

type Order int

// Constants which indicate order of response
const (
	ASC Order = iota
	DESC
)

// String representation of order
var orders = [...]string{
	"asc",
	"desc",
}

// Query operation type
type Operation int

// Query operations constants
const (
	SELECT Operation = iota
	WHERE
	ORDER_BY
	OFFSET
	LIMIT
)

// String representation of query operations
var operations = [...]string{
	"$select",
	"$where",
	"$orderby",
	"$offset",
	"$limit",
}

type QueryOptions func(query *Query) (*Query, error)

// MakeQuery creates empty Query struct
func MakeQuery(operations ...QueryOptions) (*Query, error) {
	var query = &Query{content: map[string]interface{}{}}
	var err error = nil
	for _, operation := range operations {
		query, err = operation(query)
		if err != nil {
			return nil, err
		}
	}
	return query, err
}

// Select adds the list of field paths to the list of projected fields.
// If not specified, the entire Document will be returned.
func Select(fields ...interface{}) QueryOptions {
	return func(query *Query) (*Query, error) {
		if len(fields) == 0 {
			return query, nil
		}
		var fieldPaths []interface{}
		for _, field := range fields {
			if _, ok := field.(string); !ok {
				return nil, errors.New("field path type can be only string")
			}
			fieldPaths = append(fieldPaths, field)
		}
		resMap, err := mergeQueryMaps(query.content,
			map[string]interface{}{operations[SELECT]: fieldPaths})
		if err != nil {
			return nil, err
		}
		query.content = resMap.(map[string]interface{})
		return query, nil
	}
}

// CleanSelect removes select argument from query content if it exists
func (query *Query) CleanSelect() *Query {
	return query.clean(SELECT)
}

// WhereCondition sets the filtering condition for the query.
func WhereCondition(where *Condition) QueryOptions {
	return func(query *Query) (*Query, error) {
		if where.IsEmpty() {
			return nil, errors.New("condition can't be empty")
		}
		ojaiCondition, err := convertConditionMap(where.AsMap())
		if err != nil {
			return nil, err
		}
		resMap, err := mergeQueryMaps(query.content,
			map[string]interface{}{operations[WHERE]: ojaiCondition})
		if err != nil {
			return nil, err
		}
		query.content = resMap.(map[string]interface{})
		return query, nil
	}
}

// WhereMap sets the filtering condition for the query.
func WhereMap(where map[string]interface{}) QueryOptions {
	return func(query *Query) (*Query, error) {
		if len(where) == 0 {
			return nil, errors.New("condition can't be empty")
		}
		ojaiCondition, err := convertConditionMap(where)
		if err != nil {
			return nil, err
		}
		resMap, err := mergeQueryMaps(query.content,
			map[string]interface{}{operations[WHERE]: ojaiCondition})
		if err != nil {
			return nil, err
		}
		query.content = resMap.(map[string]interface{})
		return query, nil
	}
}

// OrderBy sets the sort ordering of the returned Documents to the order of specified field path.
func OrderBy(order Order, fieldPaths ...interface{}) QueryOptions {
	return func(query *Query) (*Query, error) {
		if len(fieldPaths) == 0 {
			return query, nil
		}
		var fp []interface{}
		for _, field := range fieldPaths {
			if fieldString, ok := field.(string); ok {
				fp = append(fp, map[string]interface{}{fieldString: orders[order]})
			} else {
				return nil, errors.New("field path type can be only string")
			}
		}
		resMap, err := mergeQueryMaps(query.content,
			map[string]interface{}{operations[ORDER_BY]: fp})
		if err != nil {
			return nil, err
		}
		query.content = resMap.(map[string]interface{})
		return query, nil
	}
}

// CleanSelect removes orderBy from query content if it exists
func (query *Query) CleanOrderBy() *Query {
	return query.clean(ORDER_BY)
}

// Offset adds index which specifies number of Documents to skip before returning any result.
// Negative values are not permitted. Multiple invocation will overwrite the previous value.
func Offset(offset int) QueryOptions {
	return func(query *Query) (*Query, error) {
		if offset < 0 {
			return nil, errors.New("offset can't be negative")
		}
		resMap, err := mergeQueryMaps(query.content,
			map[string]interface{}{operations[OFFSET]: offset})
		if err != nil {
			return nil, err
		}
		query.content = resMap.(map[string]interface{})
		return query, nil
	}
}

// CleanOffset removes offset from query content if it exists
func (query *Query) CleanOffset() *Query {
	return query.clean(OFFSET)
}

// Limit restricts the maximum number of documents returned from this query
// to the specified value. Negative values are not permitted.
func Limit(limit int) QueryOptions {
	return func(query *Query) (*Query, error) {
		if limit < 0 {
			return nil, errors.New("limit can't be negative")
		}
		resMap, err := mergeQueryMaps(query.content,
			map[string]interface{}{operations[LIMIT]: limit})
		if err != nil {
			return nil, err
		}
		query.content = resMap.(map[string]interface{})
		return query, nil
	}
}

// CleanLimit removes limit from query content if it exists
func (query *Query) CleanLimit() *Query {
	return query.clean(LIMIT)
}

func (query *Query) clean(operator Operation) *Query {
	if _, ok := query.content[operations[operator]]; ok {
		delete(query.content, operations[operator])
	}
	return query
}

// CleanQuery cleans entire query content
func (query *Query) CleanQuery() *Query {
	query.content = make(map[string]interface{})
	return query
}

// Build builds the Query.
func (query *Query) Build() {
	query.isBuilt = true
}

// IsBuild checks is query built and ready to send
func (query *Query) IsBuild() bool {
	return query.isBuilt
}

func mergeQueryMaps(map1, map2 interface{}) (interface{}, error) {
	if m2, ok := map2.(map[string]interface{}); ok {
		if m1, ok := map1.(map[string]interface{}); ok {
			mergedMap := copyMap(m1)
			for k, v := range m2 {
				if newValue, ok := mergedMap[k]; ok {
					vt := reflect.TypeOf(newValue)
					switch vt.Kind() {
					case reflect.Map:
						resMap, err := mergeQueryMaps(mergedMap, newValue)
						if err != nil {
							return nil, err
						}
						mergedMap[k] = resMap
					case reflect.Array, reflect.Slice:
						if km, ok := mergedMap[k].([]interface{}); ok {
							mergedMap[k] = mergeArrayValues(km, v)
						}
					default:
						mergedMap[k] = v
					}
				} else {
					mergedMap[k] = v
				}
			}
			return mergedMap, nil
		} else {
			return nil, errors.New("invalid case. first argument always must be map")
		}
	} else {
		return map2, nil
	}
}

func mergeArrayValues(v1 []interface{}, v2 interface{}) []interface{} {
	workingArray := make([]interface{}, len(v1))
	copy(workingArray, v1)

	valueType := reflect.TypeOf(v2)
	switch valueType.Kind() {
	case reflect.Array, reflect.Slice:
		if km, ok := v2.([]interface{}); ok {
			for _, element := range km {
				if elementMap, ok := element.(map[string]interface{}); ok {
					workingArray = addMapElementInSlice(elementMap, workingArray)
				} else if !sliceContains(element, workingArray) {
					workingArray = append(workingArray, element)
				}
			}
		}
	default:
		if sliceContains(v2, workingArray) {
			workingArray = append(workingArray, v2)
		}
	}
	return workingArray
}

func sliceContains(element interface{}, list []interface{}) bool {
	for _, e := range list {
		if e == element {
			return true
		}
	}
	return false
}

func addMapElementInSlice(newValues map[string]interface{}, workingArray []interface{}) []interface{} {
	for k, v := range newValues {
		for _, element := range workingArray {
			if mElement, ok := element.(map[string]interface{}); ok {
				if _, ok := mElement[k]; ok {
					mElement[k] = v
					return workingArray
				}
			}
		}
		workingArray = append(workingArray, newValues)
		return workingArray
	}
	return nil
}

// Returns query's map
func (query *Query) AsMap() map[string]interface{} {
	if query.isBuilt {
		return query.content
	} else {
		return nil
	}
}

// Util method convert Condition map to new formatted OJAI format map
func convertConditionMap(content map[string]interface{}) (map[string]interface{}, error) {
	return throughMap(content)
}

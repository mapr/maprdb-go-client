package private_maprdb_go_client

import (
	"errors"
	"fmt"
	"gopkg.in/karalabe/cookiejar.v1/collections/deque"
)

type endType int

const (
	END endType = iota
)

var endT = [...]string{
	"$end",
}

// QueryCondition logical operation type
type logicalOperation int

// QueryCondition logical operations constants
const (
	AND logicalOperation = iota
	OR
	ELEMENT_AND
)

// String representation of QueryCondition logical operations
var logicalOperations = [...]string{
	"$and",
	"$or",
	"$elementAnd",
}

// QueryCondition operation type
type conditionQueryOperation int

// QueryCondition operations constants
const (
	EXISTS conditionQueryOperation = iota
	NOT_EXISTS
	IN
	NOT_IN
	TYPE_OF
	NOT_TYPE_OF
	MATCHES
	NOT_MATCHES
	LIKE
	NOT_LIKE
)

// String representation of QueryCondition operations
var conditionQueryOperations = [...]string{
	"$exists",
	"$notexists",
	"$in",
	"$notin",
	"$typeof",
	"$nottypeof",
	"$matches",
	"$notmatches",
	"$like",
	"$notlike",
}

// QueryCondition comparison operation type
type Comparison int

// QueryCondition comparison operations constants
const (
	LESS Comparison = iota
	LESS_OR_EQUAL
	GREATER
	GREATER_OR_EQUAL
	EQUAL
	NOT_EQUAL
)

// String representation of QueryCondition comparison operations constants
var comparisonQueryOperations = [...]string{
	"$lt",
	"$le",
	"$gt",
	"$ge",
	"$eq",
	"$ne",
}

// Condition structure with required fields
type Condition struct {
	tokens           *deque.Deque
	conditionContent map[string]interface{}
	isBuilt          bool
}

type ConditionOptions func(condition *Condition) (*Condition, error)

// MakeCondition returns new empty Condition
func MakeCondition(conditions ...ConditionOptions) (*Condition, error) {
	condition := &Condition{tokens: deque.New(), conditionContent: make(map[string]interface{}), isBuilt: false}
	var err error = nil

	for _, opt := range conditions {
		condition, err = opt(condition)
		if err != nil {
			return nil, err
		}
	}
	return condition, nil
}

// IsEmpty checks Condition and returns boolean result
func (condition *Condition) IsEmpty() bool {
	return len(condition.conditionContent) == 0
}

// IsBuilt checks is Condition built
func (condition *Condition) IsBuilt() bool {
	return condition.isBuilt
}

// And adds the logical operator 'and' in query condition
func And() ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(AND)
		return condition, nil
	}
}

// Or adds the logical operator 'or' in query condition
func Or() ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(OR)
		return condition, nil
	}
}

// ElementAnd adds the logical operator 'elementAnd' in query condition
func ElementAnd(fieldPath string) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		if len(fieldPath) == 0 {
			return nil, fmt.Errorf("fieldPath can't be empty")
		}
		condition.tokens.PushRight(ELEMENT_AND)
		condition.tokens.PushRight(fieldPath)
		return condition, nil
	}
}

// Close adds the 'end' operation in condition queue.
// Close required after each logical operation as 'or', 'and'
// or 'elementAnd' and all query condition must ends on Close
func Close() ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(END)
		return condition, nil
	}
}

// Adds a condition that tests for existence of the specified.
func Exists(fieldPath string) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{conditionQueryOperations[EXISTS]: fieldPath})
		return condition, nil
	}
}

// Adds a condition that tests for non-existence of the specified fieldPath.
func NotExists(fieldPath string) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{conditionQueryOperations[NOT_EXISTS]: fieldPath})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath is equal to at least one of the values in the specified list.
func In(fieldPath string, valueList []interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			conditionQueryOperations[IN]: map[string]interface{}{fieldPath: valueList}})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath is not equal to any of the values in the
func NotIn(fieldPath string, valueList []interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			conditionQueryOperations[NOT_IN]: map[string]interface{}{fieldPath: valueList}})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath is of the specified valueType.
func TypeOf(fieldPath string, valueType interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			conditionQueryOperations[TYPE_OF]: map[string]interface{}{fieldPath: valueType}})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath is not of the specified valueType.
func NotTypeOf(fieldPath string, valueType interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			conditionQueryOperations[NOT_TYPE_OF]: map[string]interface{}{fieldPath: valueType}})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath is a string and matches the specified regular expression.
func Matches(fieldPath string, regex interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			conditionQueryOperations[MATCHES]: map[string]interface{}{fieldPath: regex}})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath is a string and does not match the specified regular expression.
func NotMatches(fieldPath string, regex interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			conditionQueryOperations[NOT_MATCHES]: map[string]interface{}{fieldPath: regex}})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath is a string and matches the specified SQL LIKE
// expression optionally escaped with the specified escape character.
func Like(fieldPath string, likeExpr interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			conditionQueryOperations[LIKE]: map[string]interface{}{fieldPath: likeExpr}})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath is a string and does not match the specified SQL LIKE
// expression optionally escaped with the specified escape character.
func NotLike(fieldPath string, likeExpr interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			conditionQueryOperations[NOT_LIKE]: map[string]interface{}{fieldPath: likeExpr}})
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath equals the specified value. Two values are considered equal if and only if they contain the same
// key-value pair in the same order.
func Equals(fieldPath string, value interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			comparisonQueryOperations[EQUAL]: map[string]interface{}{fieldPath: value}})
		return condition, nil
	}
}

// Adds a condition that tests if the Value at the specified
// fieldPath does not equal the specified value.
// Two values are considered equal if and only if they contain the same key-value pair in the same order.
func NotEquals(fieldPath string, value interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{
			comparisonQueryOperations[NOT_EQUAL]: map[string]interface{}{fieldPath: value}})
		return condition, nil
	}
}

// Adds existing condition into new Query Condition
func AddCondition(conditionToAdd *Condition) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		if conditionToAdd.IsBuilt() {
			condition.tokens.PushRight(conditionToAdd.AsMap())
		} else {
			return nil, errors.New("build condition before add it")
		}
		return condition, nil
	}
}

// Adds existing condition into new Query Condition
func AddConditionMap(conditionToAdd map[string]interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(conditionToAdd)
		return condition, nil
	}
}

// Adds a condition that tests if the value at the specified
// fieldPath satisfies the given Op against the specified value.
func Is(fieldPath string, op Comparison, value interface{}) ConditionOptions {
	return func(condition *Condition) (*Condition, error) {
		condition.tokens.PushRight(map[string]interface{}{comparisonQueryOperations[op]: map[string]interface{}{fieldPath: value}})
		return condition, nil
	}
}

func (condition *Condition) parseQueue() error {
	for !condition.tokens.Empty() {
		token := condition.tokens.PopLeft()
		if condition.tokens.Empty() {
			if token != END {
				return errors.New("all statements in condition must be closed")
			} else {
				continue
			}
		}
		switch t := token.(type) {
		case logicalOperation:
			con, err := condition.buildBlock(t)
			if err != nil {
				return err
			}
			resMap, err := mergeQueryMaps(condition.conditionContent, con)
			if err != nil {
				return err
			}
			condition.conditionContent =
				resMap.(map[string]interface{})
		case map[string]interface{}:
			resMap, err := mergeQueryMaps(condition.conditionContent, t)
			if err != nil {
				return err
			}
			condition.conditionContent =
				resMap.(map[string]interface{})
		case endType:
			if !condition.tokens.Empty() {
				return errors.New("all statements in condition must be closed")
			}
		}
	}
	return nil
}

func (condition *Condition) buildBlock(token logicalOperation) (map[string]interface{}, error) {
	var statementList []interface{}
	var elementAndEntry interface{}

	if token == ELEMENT_AND {
		elementAndEntry = condition.tokens.PopLeft()
	}

	for !condition.tokens.Empty() {
		operation := condition.tokens.PopLeft()
		switch t := operation.(type) {
		case logicalOperation:
			block, err := condition.buildBlock(t)
			if err != nil {
				return nil, err
			}
			statementList = append(statementList, block)
		case endType:
			if elementAndEntry == nil {
				return map[string]interface{}{logicalOperations[token]: statementList}, nil
			} else {
				return map[string]interface{}{logicalOperations[token]: map[string]interface{}{elementAndEntry.(string): statementList}}, nil
			}
		default:
			statementList = append(statementList, t)
		}
	}
	return nil, errors.New("all statements in condition must be closed")
}

// Returns condition as Map
func (condition *Condition) AsMap() map[string]interface{} {
	return condition.conditionContent
}

// Builds Query Condition
func (condition *Condition) Build() (*Condition, error) {
	err := condition.parseQueue()
	if err != nil {
		return nil, err
	}
	condition.isBuilt = true
	return condition, nil
}

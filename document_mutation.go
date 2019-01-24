package private_maprdb_go_client

import (
	"errors"
)

type MutationOp int

const (
	None MutationOp = iota
	SET
	SET_OR_REPLACE
	DELETE
	APPEND
	INCREMENT
	DECREMENT
	MERGE
)

var mutationOperations = [...]string{
	"none",
	"$set",
	"$put",
	"$delete",
	"$append",
	"$increment",
	"$decrement",
	"$merge",
}

// DocumentMutation struct
type DocumentMutation struct {
	mutationMap map[string]interface{}
}

// Type for DocumentMutation functional options
type MutationOperations func(documentMutation *DocumentMutation) (*DocumentMutation, error)

// MakeDocumentationMutation function process and returns new DocumentMutation and error
func MakeDocumentMutation(mutations ...MutationOperations) (*DocumentMutation, error) {
	var mutation = &DocumentMutation{mutationMap: make(map[string]interface{})}
	var err error = nil

	for _, op := range mutations {
		mutation, err = op(mutation)
		if err != nil {
			return nil, err
		}
	}
	return mutation, err
}

// validateFieldPath function validates is field path valid
func validateFieldPath(fieldPath string) error {
	if len(fieldPath) == 0 {
		return errors.New("field path can't be empty")
	}
	if fieldPath == "_id" {
		return errors.New("_id field cannot be set or updated")
	}
	return nil
}

// Sets the field at the given fieldPath to given value
// fieldPath: path of the field that needs to be updated.
// the new value to set at the path.
func Set(fieldPath string, value interface{}) MutationOperations {
	return mutation(fieldPath, SET, value)
}

// Sets or replaces the field at the given fieldPath to value
// fieldPath in the document that needs to be updated.
// value: the new value to set or replace at the given path.
func SetOrReplace(fieldPath string, value interface{}) MutationOperations {
	return mutation(fieldPath, SET_OR_REPLACE, value)
}

// Appends the given value to an existing value at the given path
func AppendString(fieldPath, value string) MutationOperations {
	return mutation(fieldPath, APPEND, value)
}

// Appends the given value to an existing value at the given path
func AppendSlice(fieldPath string, value []interface{}) MutationOperations {
	return mutation(fieldPath, APPEND, value)
}

// Merges the existing MAP at the given fieldPath.
func MergeMap(fieldPath string, value map[string]interface{}) MutationOperations {
	return mutation(fieldPath, MERGE, value)
}

// Merges the existing MAP at the given fieldPath.
func MergeDocument(fieldPath string, value *Document) MutationOperations {
	return mutation(fieldPath, MERGE, value.documentMap)
}

// Atomically increment the existing value at given the fieldPath by the given value.
func IncrementInt(fieldPath string, value int) MutationOperations {
	if value <= 0 {
		return func(mutation *DocumentMutation) (*DocumentMutation, error) {
			return nil, errors.New("increment value must be a positive number")
		}
	}
	return mutation(fieldPath, INCREMENT, value)
}

// Atomically increment the existing value at given the fieldPath by the given value.
func IncrementIntByOne(fieldPath string) MutationOperations {
	return mutation(fieldPath, INCREMENT, 1)
}

// Atomically increment the existing value at given the fieldPath by the given value.
func IncrementFloat64(fieldPath string, value float64) MutationOperations {
	if value <= 0 {
		return func(mutation *DocumentMutation) (*DocumentMutation, error) {
			return nil, errors.New("increment value must be a positive number")
		}
	}
	return mutation(fieldPath, INCREMENT, value)
}

// Atomically increment the existing value at given the fieldPath by the given value.
func IncrementFloat64ByOne(fieldPath string) MutationOperations {
	return mutation(fieldPath, INCREMENT, float64(1))
}

// Atomically decrement the existing value at given the fieldPath by the given value.
func DecrementInt(fieldPath string, value int) MutationOperations {
	if value <= 0 {
		return func(mutation *DocumentMutation) (*DocumentMutation, error) {
			return nil, errors.New("decrement value must be a positive number")
		}
	}
	return mutation(fieldPath, DECREMENT, value)
}

// Atomically decrement the existing value at given the fieldPath by the given value.
func DecrementIntByOne(fieldPath string) MutationOperations {
	return mutation(fieldPath, DECREMENT, 1)
}

// Atomically decrement the existing value at given the fieldPath by the given value.
func DecrementFloat64(fieldPath string, value float64) MutationOperations {
	if value <= 0 {
		return func(mutation *DocumentMutation) (*DocumentMutation, error) {
			return nil, errors.New("decrement value must be a positive number")
		}
	}
	return mutation(fieldPath, DECREMENT, value)
}

// Atomically decrement the existing value at given the fieldPath by the given value.
func DecrementFloat64ByOne(fieldPath string) MutationOperations {
	return mutation(fieldPath, DECREMENT, float64(1))
}

func Delete(fieldPath string) MutationOperations {
	return func(mutation *DocumentMutation) (*DocumentMutation, error) {
		err := validateFieldPath(fieldPath)
		if err != nil {
			return nil, err
		}
		mutation.mutationMap = deleteMutation(fieldPath, mutation.mutationMap, mutationOperations[DELETE])
		return mutation, nil
	}
}

// Deletes the field at the given path
func mutation(fieldPath string, mutationOperation MutationOp, value interface{}) MutationOperations {
	return func(mutation *DocumentMutation) (*DocumentMutation, error) {
		err := validateFieldPath(fieldPath)
		if err != nil {
			return nil, err
		}
		mutation.mutationMap = commonMutation(
			fieldPath,
			value,
			mutationOperations[mutationOperation],
			mutation.mutationMap,
		)
		return mutation, nil
	}
}

func commonMutation(
	fieldPath string,
	value interface{},
	opType string,
	mutationMap map[string]interface{},
) map[string]interface{} {
	if k, ok := mutationMap[opType]; ok {
		if _, ok := k.([]interface{}); !ok {
			if mv, ok := k.(map[string]interface{}); ok {
				if _, ok := mv[fieldPath]; ok {
					mv[fieldPath] = value
					mutationMap[opType] = mv
					return mutationMap
				}
			}
			k = []interface{}{k}
		}
		if values, ok := k.([]interface{}); ok {
			for _, mutateValue := range values {
				if mv, ok := mutateValue.(map[string]interface{}); ok {
					if _, ok := mv[fieldPath]; ok {
						mv[fieldPath] = value
						mutationMap[opType] = values
						return mutationMap
					}
				}
			}
			mutationMap[opType] = append(values, map[string]interface{}{fieldPath: value})
		}
	} else {
		mutationMap[opType] = map[string]interface{}{fieldPath: value}
	}
	return mutationMap
}

func deleteMutation(
	fieldPath string,
	mutationMap map[string]interface{},
	opType string,
) map[string]interface{} {
	if k, ok := mutationMap[opType]; ok {
		if _, ok := k.([]interface{}); !ok {
			k = []interface{}{k}
		}
		if values, ok := k.([]interface{}); ok {
			if contains(values, fieldPath) {
				return mutationMap
			} else {
				values = append(values, fieldPath)
				mutationMap[opType] = values
			}
		}
	} else {
		mutationMap[opType] = fieldPath
	}
	return mutationMap
}

func contains(slice []interface{}, element interface{}) bool {
	for _, n := range slice {
		if element == n {
			return true
		}
	}
	return false
}

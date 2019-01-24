package private_maprdb_go_client

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestMakeCondition(t *testing.T) {
	tests := []struct {
		name string
		want *Condition
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition, err := MakeCondition()
			if err != nil {
				panic(err)
			}
			if got := condition; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MakeCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMixedOperations(t *testing.T) {
	condition, err := MakeCondition(And(), In("testPath", []interface{}{"1", "2", "3"}), Equals("testPath2", 55), Close(), Close())
	if err != nil {
		panic(err)
	}
	condition.Build()
	jc, err := json.Marshal(condition.conditionContent)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, "{\"$and\":[{\"$notin\":{\"testPath\":[\"1\",\"2\",\"3\"]}},{\"$eq\":{\"testPath2\":55}}]}", string(jc))
}

func TestAndIsOperations(t *testing.T) {
	condition, err := MakeCondition(And(), Is("age", GREATER_OR_EQUAL, 18), Is("city", EQUAL, "London"), Close())
	if err != nil {
		panic(err)
	}
	condition.Build()
	jc, err := json.Marshal(condition.conditionContent)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, "{\"$and\":[{\"$ge\":{\"age\":18}},{\"$eq\":{\"city\":\"London\"}}]}", string(jc))
}

func TestAndOrOperations(t *testing.T) {
	condition, err := MakeCondition(And(), Or(), Is("name", EQUAL, "Jhon"), Is("city", NOT_EQUAL, "London"), Close(),
		Or(), Is("name", NOT_EQUAL, "Jhon"), Is("city", EQUAL, "London"), Close(), Close())
	if err != nil {
		panic(err)
	}
	condition.Build()
	jc, err := json.Marshal(condition.conditionContent)
	if err != nil {
		panic(err)
	}
	assert.Equal(t,
		"{\"$and\":[{\"$or\":[{\"$eq\":{\"name\":\"Jhon\"}},{\"$ne\":{\""+
			"city\":\"London\"}}]},{\"$or\":[{\"$ne\":{\"name\":\"Jhon\"}},{\"$eq\":{\"city\":\"London\"}}]}]}",
		string(jc))
}

func TestMultipleAndOperations(t *testing.T) {
	condition, err := MakeCondition(And(), And(), Is("age", GREATER_OR_EQUAL, 18), Is("city", EQUAL, "London"), Close(),
		And(), Is("age", GREATER_OR_EQUAL, 21), Is("city", EQUAL, "NY"), Close(),
		Is("document", EQUAL, true), Close(), Is("drunk", EQUAL, false), Close())
	if err != nil {
		panic(err)
	}
	condition.Build()
	jc, err := json.Marshal(condition.conditionContent)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, "{\"$and\":[{\"$and\":[{\"$ge\":{\"age\":18}},{\"$eq\":{\""+
		"city\":\"London\"}}]},{\"$and\":[{\"$ge\":{\"age\":21}},{\"$eq\":{\"city\":\"NY\"}}]}"+
		",{\"$eq\":{\"document\":true}}],\"$eq\":{\"drunk\":false}}",
		string(jc))
}

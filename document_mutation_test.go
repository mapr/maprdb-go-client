package private_maprdb_go_client

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetMultipleTime(t *testing.T) {
	docMutation, err := MakeDocumentMutation(Set("a", 12), Set("b", 55), Set("a", 33))
	if err != nil {
		t.Errorf(err.Error())
	}
	assert.Equal(t, map[string]interface{}{"$set": []interface{}{
		map[string]interface{}{"a": 33}, map[string]interface{}{"b": 55},
	}}, docMutation.mutationMap)
}

func TestMultipleMutationOperations(t *testing.T) {
	docMutation, err := MakeDocumentMutation(Set("a", 12), Set("b", 55), SetOrReplace("s.o.r", "replace"),
		IncrementInt("inc1", 2), IncrementIntByOne("inc2"), IncrementFloat64("inc3", 25), DecrementFloat64("dec1", 5))
	if err != nil {
		t.Errorf(err.Error())
	}
	assert.Equal(t,
		"map[$set:[map[a:12] map[b:55]] $put:map[s.o.r:replace] $increment:[map[inc1:2] map[inc2:1] map[inc3:25]] $decrement:map[dec1:5]]",
		fmt.Sprintf("%v", docMutation.mutationMap))
}

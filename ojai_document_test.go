package private_maprdb_go_client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeDocument(t *testing.T) {
	tests := []struct {
		name string
		want *Document
	}{
		{name: "MakeEmptyDocument", want: &Document{}},
	}
	for _, test := range tests {
		res, err := MakeDocument()
		assert.Nil(t, err)
		assert.IsType(t, test.want, res)
		assert.Equal(t, make(map[string]interface{}), res.documentMap)
	}
}

func TestMakeDocumentFromMap(t *testing.T) {
	type args struct {
		initialData map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want *Document
	}{
		{
			name: "DocumentFromMap",
			args: args{initialData: map[string]interface{}{"_id": "testId", "name": "Jhon", "age": 75}},
			want: &Document{},
		},
		{
			name: "DocumentFromEmptyMap",
			args: args{initialData: make(map[string]interface{})},
			want: &Document{},
		},
	}
	for _, test := range tests {
		doc := MakeDocumentFromMap(test.args.initialData)
		assert.IsType(t, test.want, doc)
		assert.Equal(t, test.args.initialData, doc.documentMap)
	}
}

func TestMakeDocumentFromJson(t *testing.T) {
	type args struct {
		jsonDocument string
	}
	tests := []struct {
		name string
		args args
		want *Document
	}{
		{
			name: "DocumentFromJson",
			args: args{jsonDocument: "{\"_id\":\"testId\",\"age\":75,\"name\":\"Jhon\"}"},
			want: &Document{},
		},
		{
			name: "DocumentFromEmptyJson",
			args: args{jsonDocument: "{}"},
			want: &Document{},
		},
	}
	for _, test := range tests {
		doc, _ := MakeDocumentFromJson(test.args.jsonDocument)
		message := fmt.Sprintf("TestCase : %v", test.name)
		assert.IsType(t, test.want, doc, message)
		assert.Equal(t, test.args.jsonDocument, doc.AsJsonString(), message)
	}
}
func TestSetIdString(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
		err  bool
	}{
		{
			name: "SetIdStringEmpty",
			args: args{id: ""},
			want: map[string]interface{}{},
			err:  true,
		},
		{
			name: "SetIdString1",
			args: args{id: "idfield"},
			want: map[string]interface{}{"_id": "idfield"},
			err:  false,
		},
		{
			name: "SetIdString2",
			args: args{id: "1234"},
			want: map[string]interface{}{"_id": "1234"},
			err:  false,
		},
	}
	for _, test := range tests {
		if test.err {
			doc := &Document{documentMap: make(map[string]interface{})}
			doc, err := SetIdString(test.args.id)(doc)
			assert.Error(t, err)
		} else {
			doc := &Document{documentMap: make(map[string]interface{})}
			doc, err := SetIdString(test.args.id)(doc)
			assert.NoError(t, err)
			assert.Equal(t, test.want, doc.documentMap)
		}
	}
}

func TestSetIdBinary(t *testing.T) {
	type args struct {
		id []byte
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
		err  bool
	}{
		{
			name: "SetIdBinaryEmpty",
			args: args{id: []byte{}},
			want: map[string]interface{}{},
			err:  true,
		},
		{
			name: "SetIdBinary1",
			args: args{id: []byte{97, 98, 99}},
			want: map[string]interface{}{"_id": []byte{97, 98, 99}},
			err:  false,
		},
		{
			name: "SetIdBinary1",
			args: args{id: []byte("id1")},
			want: map[string]interface{}{"_id": []byte("id1")},
			err:  false,
		},
	}
	for _, test := range tests {
		if test.err {
			doc := &Document{documentMap: make(map[string]interface{})}
			doc, err := SetIdBinary(test.args.id)(doc)
			assert.Error(t, err)
		} else {
			doc := &Document{documentMap: make(map[string]interface{})}
			doc, err := SetIdBinary(test.args.id)(doc)
			assert.NoError(t, err)
			assert.Equal(t, test.want, doc.documentMap)
		}
	}
}

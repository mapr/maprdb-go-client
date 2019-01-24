package private_maprdb_go_client

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeQuery(t *testing.T) {
	tests := []struct {
		name string
		want *Query
	}{
		{
			name: "MakeQuery#1",
			want: &Query{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := MakeQuery()
			if err != nil {
				t.Errorf("unexpected error, %v", err.Error())
			}
			if got := query; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MakeQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuery_Select(t *testing.T) {
	type fields struct {
		content map[string]interface{}
		isBuild bool
	}
	type args struct {
		fields []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Query
	}{
		{
			name:   "TestSelect#1",
			fields: fields{content: make(map[string]interface{}), isBuild: false},
			args:   args{fields: []interface{}{"arg1", "arg2", "arg3", "arg4"}},
			want: &Query{content: map[string]interface{}{"$select": []interface{}{"arg1", "arg2", "arg3", "arg4"}},
				isBuilt: false},
		},
	}
	for _, test := range tests {
		query, err := MakeQuery(Select(test.args.fields[0], test.args.fields[1], test.args.fields[2], test.args.fields[3]))
		if err != nil {
			t.Errorf("unexpected error, %v", err.Error())
		}
		assert.Equal(t, test.want.content, query.content)
	}
}

func TestQuery_OrderBy(t *testing.T) {
	type fields struct {
		content map[string]interface{}
		isBuild bool
	}
	type args struct {
		order      Order
		fieldPaths []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Query
	}{
		{
			name:   "TestOrderBy#1",
			fields: fields{content: make(map[string]interface{}), isBuild: false},
			args:   args{order: ASC, fieldPaths: []interface{}{"arg1", "arg2"}},
			want: &Query{content: map[string]interface{}{"$orderby": []interface{}{map[string]interface{}{"arg1": "asc"}, map[string]interface{}{"arg2": "asc"}}},
				isBuilt: false},
		},
	}
	for _, test := range tests {
		query, err := MakeQuery(OrderBy(test.args.order, test.args.fieldPaths[0], test.args.fieldPaths[1]))
		if err != nil {
			t.Errorf("unexpected error, %v", err.Error())
		}
		assert.Equal(t, test.want.content, query.content)
	}
}

func TestQuery_Limit(t *testing.T) {
	type fields struct {
		content map[string]interface{}
		isBuild bool
	}
	type args struct {
		limit int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Query
	}{
		{
			name:   "TestLimit#1",
			fields: fields{content: make(map[string]interface{}), isBuild: false},
			args:   args{limit: 55},
			want:   &Query{content: map[string]interface{}{"$limit": 55}},
		},
	}
	for _, test := range tests {
		query, err := MakeQuery(Limit(test.args.limit))
		if err != nil {
			t.Errorf("unexpected error, %v", err.Error())
		}
		assert.Equal(t, test.want.content, query.content)
	}
}

func TestQuery_Offset(t *testing.T) {
	type fields struct {
		content map[string]interface{}
		isBuild bool
	}
	type args struct {
		offset int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Query
	}{
		{
			name:   "TestOffset#1",
			fields: fields{content: make(map[string]interface{}), isBuild: false},
			args:   args{offset: 55},
			want:   &Query{content: map[string]interface{}{"$offset": 55}},
		},
	}
	for _, test := range tests {
		query, err := MakeQuery(Offset(test.args.offset))
		if err != nil {
			t.Errorf("unexpected error, %v", err.Error())
		}
		assert.Equal(t, test.want.content, query.content)
	}
}

package private_maprdb_go_client

import (
	"encoding/json"
	"fmt"
	"gopkg.in/karalabe/cookiejar.v1/collections/deque"
	"io"
)

// Result of Find RPC request
type QueryResult struct {
	resultList       []interface{}
	queryPlan        string
	resultAsDocument bool
	cache            *deque.Deque
}

type DocumentStream struct {
	responseStream   MapRDbServer_FindClient
	resultAsDocument bool
}

// MakeQueryResult creates and returns new QueryResult for each Find gRPC request
// responseStream gRPC response stream from server
// findOptions options which were passed in Find request
func MakeQueryResult(responseStream MapRDbServer_FindClient, findOptions *FindOptions) (*QueryResult, error) {
	queryResult := &QueryResult{resultAsDocument: findOptions.ResultAsDocument}
	if findOptions.IncludeQueryPlan {
		element, err := responseStream.Recv()
		if err == io.EOF {
			return nil, fmt.Errorf("invalid response stream, according to input " +
				"parameters query plan must be included into response stream")
		}
		if err != nil {
			return nil, err
		}
		queryResult.queryPlan = element.GetJsonResponse()
	}
	resultList, err := queryResult.parseResponseStream(responseStream)
	if err != nil {
		return nil, err
	}
	queryResult.resultList = resultList
	return queryResult, nil
}

// parse Find gRPC response stream and returns a slice of documents
func (queryResult *QueryResult) parseResponseStream(responseStream MapRDbServer_FindClient) ([]interface{}, error) {
	var resultList []interface{}
	for {
		element, err := responseStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if element.GetError().ErrCode != ErrorCode_NO_ERROR {
			return nil, fmt.Errorf("unexpected error code received from server.\n %v.\n %v.\n %v\n",
				element.GetError().ErrCode,
				element.GetError().ErrorMessage,
				element.GetError().JavaStackTrace)
		}
		doc, err := MakeDocument()
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(element.GetJsonResponse()), doc)
		if err != nil {
			return nil, err
		}
		if queryResult.resultAsDocument {
			resultList = append(resultList, doc)
		} else {
			resultList = append(resultList, doc.AsMap())
		}
	}
	return resultList, nil
}

// QueryPlan method returns the query plan if the corresponding option was set in QueryOptions
func (queryResult *QueryResult) QueryPlan() string {
	return queryResult.queryPlan
}

// DocumentList method returns slice of document which can be represent as map[string]interface{} or Document instances
func (queryResult *QueryResult) DocumentList() []interface{} {
	return queryResult.resultList
}

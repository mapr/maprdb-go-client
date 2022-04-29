package private_maprdb_go_client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"time"
)

type DocumentStore struct {
	connection *Connection
	storeName  string
}

type FindOptions struct {
	IncludeQueryPlan bool
	ResultAsDocument bool
}

// InsertDocument method inserts Document into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertDocument(doc *Document) error {
	return documentStore.InsertDocumentWithContext(doc, nil)
}

// InsertDocumentWithContext method inserts Document into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertDocumentWithContext(doc *Document, ctx context.Context) error {
	_, err := documentStore.executeInsertOrReplace(InsertMode_INSERT, doc, nil, ctx)
	return err
}

// InsertDocumentWithIdString method changes the Document id if exists or add new id field
// and inserts the Document into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertDocumentWithIdString(doc *Document, id string) error {
	return documentStore.InsertDocumentWithIdStringContext(doc, id, nil)
}

// InsertDocumentWithIdStringContext method changes the Document id if exists or add new id field
// and inserts the Document into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertDocumentWithIdStringContext(
	doc *Document,
	id string,
	ctx context.Context,
) error {
	err := doc.SetIdString(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT, doc, nil, ctx)
	return err
}

// InsertDocumentWithIdBinary method changes the Document id if exists or add new id field
// and inserts the Document into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertDocumentWithIdBinary(doc *Document, id []byte) error {
	return documentStore.InsertDocumentWithIdBinaryContext(doc, id, nil)
}

// InsertDocumentWithIdBinaryContext method changes the Document id if exists or add new id field
// and inserts the Document into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertDocumentWithIdBinaryContext(
	doc *Document,
	id []byte,
	ctx context.Context,
) error {
	err := doc.SetIdBinary(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT, doc, nil, ctx)
	return err
}

// InsertOrReplaceDocument method inserts or replaces the Document into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceDocument(doc *Document) error {
	_, err := documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, nil)
	return err
}

// InsertOrReplaceDocumentWithContext method inserts or replaces the Document into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceDocumentWithContext(doc *Document, ctx context.Context) error {
	_, err := documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, ctx)
	return err
}

// InsertOrReplaceDocumentWithIdString method changes the Document id if exists or add new id field and
// inserts or replaces Document into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceDocumentWithIdString(doc *Document, id string) error {
	err := doc.SetIdString(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, nil)
	return err
}

// InsertOrReplaceDocumentWithIdStringContext method changes the Document id if exists or add new id field and
// inserts or replaces Document into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceDocumentWithIdStringContext(
	doc *Document,
	id string,
	ctx context.Context,
) error {
	err := doc.SetIdString(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, ctx)
	return err
}

// InsertOrReplaceDocumentWithIdBinary method changes the Document id if exists or add new id field and
// inserts or replaces Document into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceDocumentWithIdBinary(doc *Document, id []byte) error {
	err := doc.SetIdBinary(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, nil)
	return err
}

// InsertOrReplaceDocumentWithIdBinaryContext method changes the Document id if exists or add new id field and
// inserts or replaces Document into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceDocumentWithIdBinaryContext(
	doc *Document,
	id []byte,
	ctx context.Context) error {
	err := doc.SetIdBinary(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, ctx)
	return err
}

// ReplaceDocument method replaces the Document into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceDocument(doc *Document) error {
	return documentStore.ReplaceDocumentWithContext(doc, nil)
}

// ReplaceDocumentWithContext method replaces the Document into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceDocumentWithContext(doc *Document, ctx context.Context) error {
	_, err := documentStore.executeInsertOrReplace(InsertMode_REPLACE, doc, nil, ctx)
	return err
}

// ReplaceDocumentWithIdString method changes the Document id if exists or add new if field and
// replaces the Document into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceDocumentWithIdString(doc *Document, id string) error {
	return documentStore.ReplaceDocumentWithIdStringContext(doc, id, nil)
}

// ReplaceDocumentWithIdStringContext method changes the Document id if exists or add new if field and
// replaces the Document into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceDocumentWithIdStringContext(
	doc *Document,
	id string,
	ctx context.Context,
) error {
	err := doc.SetIdString(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_REPLACE, doc, nil, ctx)
	return err
}

// ReplaceDocumentWithIdBinary method changes the Document id if exists or add new if field and
// replaces the Document into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceDocumentWithIdBinary(doc *Document, id []byte) error {
	return documentStore.ReplaceDocumentWithIdBinaryContext(doc, id, nil)
}

// ReplaceDocumentWithIdBinaryContext method changes the Document id if exists or add new if field and
// replaces the Document into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceDocumentWithIdBinaryContext(
	doc *Document,
	id []byte,
	ctx context.Context,
) error {
	err := doc.SetIdBinary(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_REPLACE, doc, nil, ctx)
	return err
}

// InsertMap method inserts map[string]interface{} into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertMap(docMap map[string]interface{}) error {
	return documentStore.InsertDocument(MakeDocumentFromMap(docMap))
}

// InsertMapWithContext method inserts map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertMapWithContext(docMap map[string]interface{}, ctx context.Context) error {
	_, err := documentStore.executeInsertOrReplace(InsertMode_INSERT, MakeDocumentFromMap(docMap), nil, ctx)
	return err
}

// InsertMapWithIdString method changes id if exists or add new if field
// and inserts map[string]interface{} into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertMapWithIdString(docMap map[string]interface{}, id string) error {
	return documentStore.InsertDocumentWithIdString(MakeDocumentFromMap(docMap), id)
}

// InsertMapWithIdStringContext method changes id if exists or add new if field
// and inserts map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertMapWithIdStringContext(
	docMap map[string]interface{},
	id string,
	ctx context.Context,
) error {
	doc := MakeDocumentFromMap(docMap)
	err := doc.SetIdString(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT, doc, nil, ctx)
	return err
}

// InsertMapWithIdBinary method changes id if exists or add new if field
// and inserts map[string]interface{} into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertMapWithIdBinary(docMap map[string]interface{}, id []byte) error {
	return documentStore.InsertDocumentWithIdBinary(MakeDocumentFromMap(docMap), id)
}

// InsertMapWithIdBinaryContext method changes id if exists or add new if field
// and inserts map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertMapWithIdBinaryContext(
	docMap map[string]interface{},
	id []byte,
	ctx context.Context,
) error {
	doc := MakeDocumentFromMap(docMap)
	err := doc.SetIdBinary(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT, doc, nil, ctx)
	return err
}

// InsertOrReplaceMap method inserts or replaces map[string]interface{} into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceMap(docMap map[string]interface{}) error {
	return documentStore.InsertOrReplaceDocument(MakeDocumentFromMap(docMap))
}

// InsertOrReplaceMapWithContext method inserts or replaces map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceMapWithContext(
	docMap map[string]interface{},
	ctx context.Context,
) error {
	_, err := documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, MakeDocumentFromMap(docMap), nil, ctx)
	return err
}

// InsertOrReplaceMapWithIdString method changes id if exists or add new if field and
// inserts or replaces map[string]interface{} into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceMapWithIdString(docMap map[string]interface{}, id string) error {
	return documentStore.InsertOrReplaceDocumentWithIdString(MakeDocumentFromMap(docMap), id)
}

// InsertOrReplaceMapWithIdStringContext method changes id if exists or add new if field and
// inserts or replaces map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceMapWithIdStringContext(
	docMap map[string]interface{},
	id string,
	ctx context.Context,
) error {
	doc := MakeDocumentFromMap(docMap)
	err := doc.SetIdString(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, ctx)
	return err
}

// InsertOrReplaceMapWithIdString method changes id if exists or add new if field and
// inserts or replaces map[string]interface{} into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceMapWithIdBinary(docMap map[string]interface{}, id []byte) error {
	return documentStore.InsertOrReplaceDocumentWithIdBinary(MakeDocumentFromMap(docMap), id)
}

// InsertOrReplaceMapWithIdBinaryContext method changes id if exists or add new if field and
// inserts or replaces map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceMapWithIdBinaryContext(
	docMap map[string]interface{},
	id []byte,
	ctx context.Context,
) error {
	doc := MakeDocumentFromMap(docMap)
	err := doc.SetIdBinary(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, ctx)
	return err
}

// ReplaceMap method replaces map[string]interface{} into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceMap(docMap map[string]interface{}) error {
	return documentStore.ReplaceDocument(MakeDocumentFromMap(docMap))
}

// ReplaceMapWithContext method replaces map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceMapWithContext(docMap map[string]interface{}, ctx context.Context) error {
	_, err := documentStore.executeInsertOrReplace(
		InsertMode_REPLACE,
		MakeDocumentFromMap(docMap),
		nil,
		nil)
	return err
}

// ReplaceMapWithIdString method changes id id exists or add new id field and
// replaces map[string]interface{} into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceMapWithIdString(docMap map[string]interface{}, id string) error {
	return documentStore.ReplaceDocumentWithIdString(MakeDocumentFromMap(docMap), id)
}

// ReplaceMapWithIdStringContext method changes id id exists or add new id field and
// replaces map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceMapWithIdStringContext(
	docMap map[string]interface{},
	id string,
	ctx context.Context,
) error {
	doc := MakeDocumentFromMap(docMap)
	err := doc.SetIdString(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_REPLACE, doc, nil, ctx)
	return err
}

// ReplaceMapWithIdBinary method changes id id exists or add new id field and
// replaces map[string]interface{} into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceMapWithIdBinary(docMap map[string]interface{}, id []byte) error {
	return documentStore.ReplaceDocumentWithIdBinary(MakeDocumentFromMap(docMap), id)
}

// ReplaceMapWithIdBinaryContext method changes id id exists or add new id field and
// replaces map[string]interface{} into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceMapWithIdBinaryContext(
	docMap map[string]interface{},
	id []byte,
	ctx context.Context,
) error {
	return documentStore.ReplaceDocumentWithIdBinaryContext(MakeDocumentFromMap(docMap), id, ctx)
}

// InsertString method inserts string into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertString(docString string) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	return documentStore.InsertDocument(doc)
}

// InsertString method inserts string into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertStringWithContext(docString string, ctx context.Context) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT, doc, nil, ctx)
	return err
}

// InsertStringWithIdString method changes id if exists or add new id field and
// inserts string into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertStringWithIdString(docString string, id string) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	return documentStore.InsertDocumentWithIdString(doc, id)
}

// InsertStringWithIdStringContext method changes id if exists or add new id field and
// inserts string into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertStringWithIdStringContext(
	docString string,
	id string,
	ctx context.Context,
) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	err = doc.SetIdString(id)
	if err != nil {
		return err
	}

	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT, doc, nil, ctx)
	return err
}

// InsertStringWithIdBinary method changes if if exists or add new id field and
// inserts string into the store in MapR-DB.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertStringWithIdBinary(docString string, id []byte) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	return documentStore.InsertDocumentWithIdBinary(doc, id)
}

// InsertStringWithIdBinaryContext method changes if if exists or add new id field and
// inserts string into the store in MapR-DB.
// User defined context is required for this method.
// This operation is successful only when the document with the given id doesn't exist.
// If "_id" already existed in the document, then an error will be thrown.
func (documentStore *DocumentStore) InsertStringWithIdBinaryContext(
	docString string,
	id []byte,
	ctx context.Context,
) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	err = doc.SetIdBinary(id)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT, doc, nil, ctx)
	return err
}

// InsertOrReplaceString method inserts or replaces string into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceString(docString string) error {
	return documentStore.InsertOrReplaceStringWithContext(docString, nil)
}

// InsertOrReplaceStringWithContext method inserts or replaces string into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceStringWithContext(docString string, ctx context.Context) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	_, err = documentStore.executeInsertOrReplace(InsertMode_INSERT_OR_REPLACE, doc, nil, ctx)
	return err
}

// InsertOrReplaceStringWithIdString method changes id if exists or and new id field and
// inserts or replaces string into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceStringWithIdString(docString string, id string) error {
	return documentStore.InsertOrReplaceStringWithIdStringContext(docString, id, nil)
}

// InsertOrReplaceStringWithIdStringContext method changes id if exists or and new id field and
// inserts or replaces string into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceStringWithIdStringContext(
	docString string,
	id string,
	ctx context.Context,
) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	return documentStore.InsertOrReplaceDocumentWithIdStringContext(doc, id, ctx)
}

// InsertOrReplaceStringWithIdBinary method changes id if exists or add new id field and
// inserts or replaces string into the store in MapR-DB.
func (documentStore *DocumentStore) InsertOrReplaceStringWithIdBinary(docString string, id []byte) error {
	return documentStore.InsertOrReplaceStringWithIdBinaryContext(docString, id, nil)
}

// InsertOrReplaceStringWithIdBinaryContext method changes id if exists or add new id field and
// inserts or replaces string into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) InsertOrReplaceStringWithIdBinaryContext(
	docString string,
	id []byte,
	ctx context.Context,
) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	return documentStore.InsertOrReplaceDocumentWithIdBinaryContext(doc, id, ctx)
}

// ReplaceString method replaces string into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceString(docString string) error {
	return documentStore.ReplaceStringWithContext(docString, nil)
}

// ReplaceStringWithContext method replaces string into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceStringWithContext(
	docString string,
	ctx context.Context,
) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	return documentStore.ReplaceDocumentWithContext(doc, ctx)
}

// ReplaceStringWithIdString method changes id if exists or add new id field and
// replaces string into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceStringWithIdString(docString string, id string) error {
	return documentStore.ReplaceStringWithIdStringContext(docString, id, nil)
}

// ReplaceStringWithIdStringContext method changes id if exists or add new id field and
// replaces string into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceStringWithIdStringContext(
	docString string,
	id string,
	ctx context.Context,
) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	return documentStore.ReplaceDocumentWithIdStringContext(doc, id, ctx)
}

// ReplaceStringWithIdBinary method changes id if exists or add new id field and
// replaces string into the store in MapR-DB.
func (documentStore *DocumentStore) ReplaceStringWithIdBinary(docString string, id []byte) error {
	return documentStore.ReplaceStringWithIdBinaryContext(docString, id, nil)
}

// ReplaceStringWithIdBinaryContext method changes id if exists or add new id field and
// replaces string into the store in MapR-DB.
// User defined context is required for this method.
func (documentStore *DocumentStore) ReplaceStringWithIdBinaryContext(
	docString string,
	id []byte,
	ctx context.Context,
) error {
	doc, err := MakeDocumentFromJson(docString)
	if err != nil {
		return err
	}
	return documentStore.ReplaceDocumentWithIdBinaryContext(doc, id, ctx)
}

// Atomically evaluates the condition on the given document and if the
// condition holds true for the document then it atomically replaces the document
// with the given document.
// id document id.
// condition the condition to evaluate on the document.
// document document to replace
func (documentStore *DocumentStore) CheckAndReplaceWithId(
	id *BinaryOrStringId,
	condition *MapOrStructCondition,
	document *Document,
) (bool, error) {
	return documentStore.CheckAndReplaceWithIdContext(id, condition, document, nil)
}

// CheckAndReplaceWithIdContext method atomically evaluates the condition on the given document and if the
// condition holds true for the document then it atomically replaces the document with the given document.
// User defined context is required for this method.
// id document id.
// condition the condition to evaluate on the document.
// document document to replace
func (documentStore *DocumentStore) CheckAndReplaceWithIdContext(
	id *BinaryOrStringId,
	condition *MapOrStructCondition,
	document *Document,
	ctx context.Context,
) (bool, error) {
	if id.IsBinary {
		err := document.SetIdBinary(id.Binary)
		if err != nil {
			return false, err
		}
	} else {
		document.SetIdString(id.Str)
	}
	return documentStore.CheckAndReplaceWithContext(condition, document, ctx)
}

// Atomically evaluates the condition on the given document and if the
// condition holds true for the document then it atomically replaces the document
// with the given document.
// condition the condition to evaluate on the document.
// document document to replace
func (documentStore *DocumentStore) CheckAndReplace(
	condition *MapOrStructCondition,
	document *Document,
) (bool, error) {
	return documentStore.CheckAndReplaceWithContext(condition, document, nil)
}

// CheckAndReplaceWithContext method atomically evaluates the condition on the given document and if the
// condition holds true for the document then it atomically replaces the document with the given document.
// User defined context is required for this method.
// condition the condition to evaluate on the document.
// document document to replace
func (documentStore *DocumentStore) CheckAndReplaceWithContext(
	condition *MapOrStructCondition,
	document *Document,
	ctx context.Context,
) (bool, error) {
	response, err := documentStore.executeInsertOrReplace(InsertMode_REPLACE, document, condition, ctx)
	if err == nil {
		return true, err
	} else if response.Error.ErrCode == ErrorCode_DOCUMENT_NOT_FOUND {
		return false, nil
	} else {
		return false, err
	}
}

// Util method for all InsertOrReplace variants.
func (documentStore *DocumentStore) executeInsertOrReplace(
	insertMode InsertMode,
	doc *Document,
	condition *MapOrStructCondition,
	userDefinedContext context.Context,
) (*InsertOrReplaceResponse, error) {
	if !doc.HasId() {
		return nil, errors.New("the document must contain the _id field before sending")
	}
	ser, err := json.Marshal(doc)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("couldn't marshal Document: %v\n", err))
	}
	var ctx context.Context
	if userDefinedContext != nil {
		ctx = userDefinedContext
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(),
			time.Duration(documentStore.connection.opts.CallTimeoutSeconds)*time.Second)
		defer cancel()
	}
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	request := &InsertOrReplaceRequest{
		TablePath:       documentStore.storeName,
		InsertMode:      insertMode,
		PayloadEncoding: PayloadEncoding_JSON_ENCODING,
		Data:            &InsertOrReplaceRequest_JsonDocument{JsonDocument: string(ser)},
	}
	if condition != nil {
		conditionString, err := getConditionString(condition)
		if err != nil {
			return nil, err
		}
		request.Condition = &InsertOrReplaceRequest_JsonCondition{JsonCondition: conditionString}
	}
	response, err := documentStore.connection.stub.InsertOrReplace(ctx,
		request,
		grpc.Header(&header),
		grpc.Trailer(&trailer),
	)
	if err != nil {
		return response, errors.New(fmt.Sprintf("Couldn't execute request: %v", err))
	}

	err = checkResponseErrorCode(response.GetError())
	if err != nil {
		return response, err
	}

	documentStore.connection.umd.UpdateToken(header, trailer)
	return response, nil
}

// FindByIdString method executes gRPC FindById request with id field as string
// on server and returns Document if it exists in MapR-DB
func (documentStore *DocumentStore) FindByIdString(id string) (*Document, error) {
	return documentStore.FindByIdStringWithContext(id, nil)
}

// FindByIdStringWithContext method executes gRPC FindById request with id field as string
// on server and returns Document if it exists in MapR-DB
// User defined context is required for this method.
func (documentStore *DocumentStore) FindByIdStringWithContext(id string, ctx context.Context) (*Document, error) {
	if len(id) == 0 {
		return nil, errors.New("_id can't be empty")
	}
	doc := MakeDocumentFromMap(map[string]interface{}{"_id": id})
	return documentStore.findById(doc, nil, nil, ctx)
}

// FindByIdStringWithFieldsAndCondition method returns the document with the specified `_id`
// or nil if the document with that `_id` either doesn't exist
// in this DocumentStore or does not meet the specified condition.
// id  Document id.
// field_paths list of fields that should be returned in the read document.
// condition query condition to test the document
func (documentStore *DocumentStore) FindByIdStringWithFieldsAndCondition(
	id string,
	fieldPaths []string,
	queryCondition *MapOrStructCondition,
) (*Document, error) {
	return documentStore.FindByIdStringWithFieldsAndConditionContext(id, fieldPaths, queryCondition, nil)
}

// FindByIdStringWithFieldsAndConditionContext method returns the document with the specified `_id`
// or nil if the document with that `_id` either doesn't exist
// in this DocumentStore or does not meet the specified condition.
// User defined context is required for this method.
// id  Document id.
// field_paths list of fields that should be returned in the read document.
// condition query condition to test the document
func (documentStore *DocumentStore) FindByIdStringWithFieldsAndConditionContext(
	id string,
	fieldPaths []string,
	queryCondition *MapOrStructCondition,
	ctx context.Context,
) (*Document, error) {
	if len(id) == 0 {
		return nil, errors.New("_id can't be empty")
	}
	doc := MakeDocumentFromMap(map[string]interface{}{"_id": id})
	return documentStore.findById(doc, fieldPaths, queryCondition, ctx)
}

// FindByIdByte method executes gRPC FindById request with id field as array of byte
// on server and returns Document if it exists in MapR-DB
func (documentStore *DocumentStore) FindByIdByte(id []byte) (*Document, error) {
	return documentStore.FindByIdByteWithContext(id, nil)
}

// FindByIdByteWithContext method  executes gRPC FindById request with id field as array of byte
// on server and returns Document if it exists in MapR-DB
// User defined context is required for this method.
func (documentStore *DocumentStore) FindByIdByteWithContext(id []byte, ctx context.Context) (*Document, error) {
	if len(id) == 0 {
		return nil, errors.New("_id can't be empty")
	}
	doc := MakeDocumentFromMap(map[string]interface{}{"_id": id})
	return documentStore.findById(doc, nil, nil, ctx)
}

// FindByIdByteWithFieldsAndCondition method returns the document with the specified `_id`
// or nil if the document with that `_id` either doesn't exist
// in this DocumentStore or does not meet the specified condition.
// id  Document id.
// field_paths list of fields that should be returned in the read document.
// condition query condition to test the document
func (documentStore *DocumentStore) FindByIdByteWithFieldsAndCondition(
	id []byte,
	fieldPaths []string,
	queryCondition *MapOrStructCondition,
) (*Document, error) {
	return documentStore.FindByIdByteWithFieldsAndConditionContext(id, fieldPaths, queryCondition, nil)
}

// FindByIdByteWithFieldsAndConditionContext method returns the document with the specified `_id`
// or nil if the document with that `_id` either doesn't exist
// in this DocumentStore or does not meet the specified condition.
// User defined context is required for this method.
// id  Document id.
// field_paths list of fields that should be returned in the read document.
// condition query condition to test the document
func (documentStore *DocumentStore) FindByIdByteWithFieldsAndConditionContext(
	id []byte,
	fieldPaths []string,
	queryCondition *MapOrStructCondition,
	ctx context.Context,
) (*Document, error) {
	if len(id) == 0 {
		return nil, errors.New("_id can't be empty")
	}
	doc := MakeDocumentFromMap(map[string]interface{}{"_id": id})
	return documentStore.findById(doc, fieldPaths, queryCondition, ctx)
}

// Method executes gRPC FindById request on server and returns Document if it exists in MapR-DB
func (documentStore *DocumentStore) findById(
	doc *Document,
	fieldPaths []string,
	queryCondition *MapOrStructCondition,
	userDefinedContext context.Context,
) (*Document, error) {
	var ctx context.Context
	if userDefinedContext != nil {
		ctx = userDefinedContext
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(),
			time.Duration(documentStore.connection.opts.CallTimeoutSeconds)*time.Second)
		defer cancel()
	}
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	jsonString, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	request := &FindByIdRequest{
		TablePath:       documentStore.storeName,
		PayloadEncoding: PayloadEncoding_JSON_ENCODING,
		Document:        &FindByIdRequest_JsonDocument{JsonDocument: string(jsonString)},
	}
	if fieldPaths != nil {
		request.Projections = fieldPaths
	}

	if queryCondition != nil {
		conditionString, err := getConditionString(queryCondition)
		if err != nil {
			return nil, err
		}
		request.Condition = &FindByIdRequest_JsonCondition{JsonCondition: conditionString}
	}

	response, err := documentStore.connection.stub.FindById(ctx,
		request,
		grpc.Header(&header),
		grpc.Trailer(&trailer),
	)
	if err != nil {
		return nil, err
	}

	deserializedDoc, err := MakeDocument()
	if err != nil {
		return nil, err
	}

	documentStore.connection.umd.UpdateToken(header, trailer)
	res, err := checkIsDocumentExists(response.GetError())

	if err != nil {
		return nil, err
	}

	if res {
		err = json.Unmarshal([]byte(response.GetJsonDocument()), &deserializedDoc)
		if err != nil {
			return nil, err
		}
	}
	return deserializedDoc, nil
}

// Method checks FindByID response error code and return true
// if error code is 0 (NO_ERROR), false if error code 2(DOCUMENT_NOT_FOUND) otherwise error.
func checkIsDocumentExists(rpcError *RpcError) (bool, error) {
	switch rpcError.ErrCode {
	case ErrorCode_NO_ERROR:
		return true, nil
	case ErrorCode_DOCUMENT_NOT_FOUND:
		return false, nil
	default:
		return false, errors.New(fmt.Sprintf("unexpected error code recieved from server\n error: %v\n"+
			" error message : %v\n java stacktrace: %v\n",
			rpcError.ErrCode.String(),
			rpcError.ErrorMessage,
			rpcError.JavaStackTrace))
	}
}

// DeleteDoc method takes Document and executes gRPC Delete request on server and returns true
// if the Document was deleted from MapR-DB table otherwise false.
func (documentStore *DocumentStore) DeleteDoc(doc *Document) (bool, error) {
	return documentStore.delete(doc, nil)
}

// DeleteDocWithContext method takes Document and executes gRPC Delete request on server and returns true
// if the Document was deleted from MapR-DB table otherwise false.
// User defined context is required for this method.
func (documentStore *DocumentStore) DeleteDocWithContext(doc *Document, ctx context.Context) (bool, error) {
	return documentStore.delete(doc, ctx)
}

// DeleteByIdString method takes string _id, create new Document and executes gRPC Delete
// request on server and returns true if the Document was deleted from MapR-DB table otherwise false.
func (documentStore *DocumentStore) DeleteByIdString(id string) (bool, error) {
	return documentStore.DeleteByIdStringWithContext(id, nil)
}

// DeleteByIdStringWithContext method takes string _id, create new Document and executes gRPC Delete
// request on server and returns true if the Document was deleted from MapR-DB table otherwise false.
// User defined context is required for this method.
func (documentStore *DocumentStore) DeleteByIdStringWithContext(id string, ctx context.Context) (bool, error) {
	if len(id) == 0 {
		return false, errors.New("_id can't be empty")
	}
	doc := MakeDocumentFromMap(map[string]interface{}{"_id": id})
	return documentStore.delete(doc, ctx)
}

// DeleteByIdBinary method takes []byte _id, create new Document and executes gRPC Delete request on server
// and returns true if the Document was deleted from MapR-DB table otherwise false.
func (documentStore *DocumentStore) DeleteByIdBinary(id []byte) (bool, error) {
	return documentStore.DeleteByIdBinaryWithContext(id, nil)
}

// DeleteByIdBinaryWithContext method takes []byte _id, create new Document and executes gRPC Delete
// request on server and returns true if the Document was deleted from MapR-DB table otherwise false.
// User defined context is required for this method.
func (documentStore *DocumentStore) DeleteByIdBinaryWithContext(id []byte, ctx context.Context) (bool, error) {
	doc := MakeDocumentFromMap(map[string]interface{}{"_id": id})
	return documentStore.delete(doc, ctx)
}

// CheckAndDelete method atomically evaluates the condition on given document and if the
// condition holds true for the document then it is atomically deleted.
// id string or []byte document _id.
// condition the condition to evaluate on the document.
func (documentStore *DocumentStore) CheckAndDelete(id *BinaryOrStringId, condition *MapOrStructCondition) error {
	return documentStore.CheckAndDeleteWithContext(id, condition, nil)
}

// CheckAndDeleteWithContext method atomically evaluates the condition on given document and if the
// condition holds true for the document then it is atomically deleted.
// User defined context is required for this method.
// id string or []byte document _id.
// condition the condition to evaluate on the document.
func (documentStore *DocumentStore) CheckAndDeleteWithContext(
	id *BinaryOrStringId,
	condition *MapOrStructCondition,
	ctx context.Context,
) error {
	docString, err := getDocumentString(id)
	if err != nil {
		return err
	}
	conditionString, err := getConditionString(condition)
	if err != nil {
		return err
	}
	_, err = documentStore.executeDelete(docString, conditionString, ctx)
	return err
}

func (documentStore *DocumentStore) delete(doc *Document, ctx context.Context) (bool, error) {
	if !doc.HasId() {
		return false, errors.New("document must contain the _id field before send")
	}
	ser, err := json.Marshal(doc)
	if err != nil {
		return false, err
	}
	return documentStore.executeDelete(string(ser), "", ctx)
}

// Method executes gRPC Delete request on server and returns true
// if the document was deleted from MapR-DB table otherwise false
func (documentStore *DocumentStore) executeDelete(
	docString,
	conditionString string,
	userDefinedContext context.Context,
) (bool, error) {
	var ctx context.Context
	if userDefinedContext != nil {
		ctx = userDefinedContext
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(),
			time.Duration(documentStore.connection.opts.CallTimeoutSeconds)*time.Second)
		defer cancel()
	}
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	request := &DeleteRequest{
		TablePath:       documentStore.storeName,
		PayloadEncoding: PayloadEncoding_JSON_ENCODING,
		Document:        &DeleteRequest_JsonDocument{JsonDocument: docString},
	}
	if len(conditionString) != 0 {
		request.Condition = &DeleteRequest_JsonCondition{JsonCondition: conditionString}
	}
	response, err := documentStore.connection.stub.Delete(ctx,
		request,
		grpc.Header(&header),
		grpc.Trailer(&trailer),
	)
	if err != nil {
		return false, err
	}
	documentStore.connection.umd.UpdateToken(header, trailer)
	return checkIsDocumentExists(response.GetError())
}

// FindAll method executes gRPC Find request and returns all content of specific DocumentStore
// findOptions is FindOptions struct with specific query parameters
// returns QueryResult with response content and error
func (documentStore *DocumentStore) FindAll(findOptions *FindOptions) (*QueryResult, error) {
	return documentStore.FindAllWithContext(findOptions, nil)
}

// FindAllWithContext method executes gRPC Find request and returns all content of specific DocumentStore
// User defined context is required for this method.
// findOptions is FindOptions struct with specific query parameters
// returns QueryResult with response content and error
func (documentStore *DocumentStore) FindAllWithContext(
	findOptions *FindOptions,
	ctx context.Context,
) (*QueryResult, error) {
	queryContent := make(map[string]interface{})
	return documentStore.find(&queryContent, findOptions, ctx)
}

// FindQueryString executes gRPC Find request and returns QueryResult and error.
// query is json string representation of OJAI query content
// findOptions is FindOptions struct with specific query parameters
func (documentStore *DocumentStore) FindQueryString(query string, findOptions *FindOptions) (*QueryResult, error) {
	return documentStore.FindQueryStringWithContext(query, findOptions, nil)
}

// FindQueryString executes gRPC Find request and returns QueryResult and error.
// User defined context is required for this method.
// query is json string representation of OJAI query content
// findOptions is FindOptions struct with specific query parameters
func (documentStore *DocumentStore) FindQueryStringWithContext(
	query string,
	findOptions *FindOptions,
	ctx context.Context,
) (*QueryResult, error) {
	queryContent := make(map[string]interface{})
	err := json.Unmarshal([]byte(query), &queryContent)
	if err != nil {
		return nil, err
	}
	return documentStore.find(&queryContent, findOptions, ctx)
}

// FindQueryMap method executes gRPC Find request and returns QueryResult and error.
// query is map[string]interface{} with OJAI query content
// findOptions is FindOptions struct with specific query parameters
func (documentStore *DocumentStore) FindQueryMap(
	query map[string]interface{},
	findOptions *FindOptions,
) (*QueryResult, error) {
	return documentStore.find(&query, findOptions, nil)
}

// FindQueryMapWithContext method executes gRPC Find request and returns QueryResult and error.
// User defined context is required for this method.
// query is map[string]interface{} with OJAI query content
// findOptions is FindOptions struct with specific query parameters
func (documentStore *DocumentStore) FindQueryMapWithContext(
	query map[string]interface{},
	findOptions *FindOptions,
	ctx context.Context,
) (*QueryResult, error) {
	return documentStore.find(&query, findOptions, ctx)
}

// FindQuery method executes gRPC Find request and returns QueryResult and error.
// query is Query struct with OJAI query content
// findOptions is FindOptions struct with specific query parameters
func (documentStore *DocumentStore) FindQuery(query *Query, findOptions *FindOptions) (*QueryResult, error) {
	return documentStore.find(&query.content, findOptions, nil)
}

// FindQueryWithContext method executes gRPC Find request and returns QueryResult and error.
// User defined context is required for this method.
// query is Query struct with OJAI query content
// findOptions is FindOptions struct with specific query parameters
func (documentStore *DocumentStore) FindQueryWithContext(
	query *Query,
	findOptions *FindOptions,
	ctx context.Context,
) (*QueryResult, error) {
	return documentStore.find(&query.content, findOptions, ctx)
}

// find executes gRPC Find request, process response and returns QueryResult and error
func (documentStore *DocumentStore) find(
	queryContent *map[string]interface{},
	findOptions *FindOptions,
	userDefinedContext context.Context,
) (*QueryResult, error) {
	ser, err := json.Marshal(queryContent)
	if err != nil {
		return nil, err
	}
	var ctx context.Context
	if userDefinedContext != nil {
		ctx = userDefinedContext
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(),
			time.Duration(documentStore.connection.opts.CallTimeoutSeconds)*time.Second)
		defer cancel()
	}
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	request := &FindRequest{
		TablePath:        documentStore.storeName,
		PayloadEncoding:  PayloadEncoding_JSON_ENCODING,
		IncludeQueryPlan: findOptions.IncludeQueryPlan,
		Data:             &FindRequest_JsonQuery{JsonQuery: string(ser)},
	}
	responseStream, err := documentStore.connection.stub.Find(ctx,
		request,
		grpc.Header(&header),
		grpc.Trailer(&trailer))
	if err != nil {
		return nil, err
	}
	documentStore.connection.umd.UpdateToken(header, trailer)
	return MakeQueryResult(responseStream, findOptions)
}

// structure that is used in gRPC requests instead of string or []byte _id representation
type BinaryOrStringId struct {
	IsBinary bool
	Binary   []byte
	Str      string
}

// Creates BinaryOrStringId struct from string
func BosiFromString(str string) *BinaryOrStringId {
	return &BinaryOrStringId{Str: str}
}

// Creates BinaryOrStringId struct from []byte
func BosiFromBinary(binary []byte) *BinaryOrStringId {
	return &BinaryOrStringId{IsBinary: true, Binary: binary}
}

// structure that is used in gRPC requests instead of Map or Condition representation
type MapOrStructCondition struct {
	isMap           bool
	MapCondition    map[string]interface{}
	StructCondition *Condition
}

// Creates BapOrStructCondition struct from map[string]interface{}
func MoscFromMap(condition map[string]interface{}) *MapOrStructCondition {
	return &MapOrStructCondition{isMap: true, MapCondition: condition}
}

// Creates BapOrStructCondition struct from Condition
func MoscFromStruct(condition *Condition) *MapOrStructCondition {
	return &MapOrStructCondition{StructCondition: condition}
}

// structure that is used in gRPC requests instead of Map or DocumentMutation representation
type MapOrStructMutation struct {
	IsMap          bool
	MapMutation    map[string]interface{}
	StructMutation *DocumentMutation
}

func MosmFromMap(mutation map[string]interface{}) *MapOrStructMutation {
	return &MapOrStructMutation{IsMap: true, MapMutation: mutation}
}

func MosmFromStruct(mutation *DocumentMutation) *MapOrStructMutation {
	return &MapOrStructMutation{StructMutation: mutation}
}

// Update method applies a mutation on the document identified by the document id.
// All updates specified by the mutation object should be applied atomically,
// and consistently meaning either all of the updates in mutation are applied
// or none of them is applied and a partial update should not be visible to an
// observer.
func (documentStore *DocumentStore) Update(
	id *BinaryOrStringId,
	documentMutation *MapOrStructMutation,
) error {
	_, err := documentStore.update(id, nil, documentMutation, nil)
	return err
}

// UpdateWithContext method applies a mutation on the document identified by the document id.
// All updates specified by the mutation object should be applied atomically,
// and consistently meaning either all of the updates in mutation are applied
// or none of them is applied and a partial update should not be visible to an
// observer.
func (documentStore *DocumentStore) UpdateWithContext(
	id *BinaryOrStringId,
	documentMutation *MapOrStructMutation,
	ctx context.Context,
) error {
	_, err := documentStore.update(id, nil, documentMutation, ctx)
	return err
}

// CheckAndUpdate method atomically evaluates the condition on a given document and if the
// condition holds true for the document then a mutation is applied on the document.
func (documentStore *DocumentStore) CheckAndUpdate(
	id *BinaryOrStringId,
	queryCondition *MapOrStructCondition,
	documentMutation *MapOrStructMutation,
) (bool, error) {
	return documentStore.update(id, queryCondition, documentMutation, nil)
}

// CheckAndUpdateWithContext method atomically evaluates the condition on a given document and if the
// condition holds true for the document then a mutation is applied on the document.
func (documentStore *DocumentStore) CheckAndUpdateWithContext(
	id *BinaryOrStringId,
	queryCondition *MapOrStructCondition,
	documentMutation *MapOrStructMutation,
	ctx context.Context,
) (bool, error) {
	return documentStore.update(id, queryCondition, documentMutation, ctx)
}

// IncrementInt method atomically applies an increment to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the incremental value.
// The operation will fail if the increment is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// inc increment to apply to a field.
func (documentStore *DocumentStore) IncrementInt(id *BinaryOrStringId, fieldPath string, inc int) error {
	return documentStore.IncrementIntWithContext(id, fieldPath, inc, nil)
}

// DecrementInt method atomically applies an decrement to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the decremental value.
// The operation will fail if the decrement is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// dec decrement to apply to a field.
func (documentStore *DocumentStore) DecrementInt(id *BinaryOrStringId, fieldPath string, dec int) error {
	return documentStore.DecrementIntWithContext(id, fieldPath, dec, nil)
}

// IncrementIntWithContext method atomically applies an increment to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the incremental value.
// The operation will fail if the increment is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// inc increment to apply to a field.
func (documentStore *DocumentStore) IncrementIntWithContext(
	id *BinaryOrStringId,
	fieldPath string,
	inc int,
	ctx context.Context,
) error {
	mutation, err := MakeDocumentMutation(IncrementInt(fieldPath, inc))
	if err != nil {
		return err
	}
	_, err = documentStore.update(id, nil, &MapOrStructMutation{StructMutation: mutation}, ctx)
	return err
}

// DecrementIntWithContext method atomically applies an decrement to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the decremental value.
// The operation will fail if the decrement is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// dec decrement to apply to a field.
func (documentStore *DocumentStore) DecrementIntWithContext(
	id *BinaryOrStringId,
	fieldPath string,
	dec int,
	ctx context.Context,
) error {
	mutation, err := MakeDocumentMutation(DecrementInt(fieldPath, dec))
	if err != nil {
		return err
	}
	_, err = documentStore.update(id, nil, &MapOrStructMutation{StructMutation: mutation}, ctx)
	return err
}

// IncrementIntByOne method atomically applies an increment by one to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the incremental value.
// The operation will fail if the increment is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
func (documentStore *DocumentStore) IncrementIntByOne(id *BinaryOrStringId, fieldPath string) error {
	return documentStore.IncrementIntByOneWithContext(id, fieldPath, nil)
}

// DecrementIntByOne method atomically applies an decrement by one to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the decremental value.
// The operation will fail if the decrement is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
func (documentStore *DocumentStore) DecrementIntByOne(id *BinaryOrStringId, fieldPath string) error {
	return documentStore.DecrementIntByOneWithContext(id, fieldPath, nil)
}

// IncrementIntByOneWithContext method atomically applies an increment by one to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the incremental value.
// The operation will fail if the increment is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
func (documentStore *DocumentStore) IncrementIntByOneWithContext(
	id *BinaryOrStringId,
	fieldPath string,
	ctx context.Context,
) error {
	mutation, err := MakeDocumentMutation(IncrementIntByOne(fieldPath))
	if err != nil {
		return err
	}
	_, err = documentStore.update(id, nil, &MapOrStructMutation{StructMutation: mutation}, ctx)
	return err
}

// DecrementIntByOneWithContext method atomically applies an decrement by one to a given
// field (in dot separated notation) of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the decremental value.
// The operation will fail if the decrement is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
func (documentStore *DocumentStore) DecrementIntByOneWithContext(
	id *BinaryOrStringId,
	fieldPath string,
	ctx context.Context,
) error {
	mutation, err := MakeDocumentMutation(DecrementIntByOne(fieldPath))
	if err != nil {
		return err
	}
	_, err = documentStore.update(id, nil, &MapOrStructMutation{StructMutation: mutation}, ctx)
	return err
}

// IncrementFloat64 method atomically applies an increment to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the incremental value.
// The operation will fail if the increment is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// inc increment to apply to a field.
func (documentStore *DocumentStore) IncrementFloat64(id *BinaryOrStringId, fieldPath string, inc float64) error {
	return documentStore.IncrementFloat64WithContext(id, fieldPath, inc, nil)
}

// DecrementFloat64 method atomically applies an decrement to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the decremental value.
// The operation will fail if the decrement is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// dec decrement to apply to a field.
func (documentStore *DocumentStore) DecrementFloat64(id *BinaryOrStringId, fieldPath string, dec float64) error {
	return documentStore.DecrementFloat64WithContext(id, fieldPath, dec, nil)
}

// IncrementFloat64WithContext method atomically applies an increment to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the incremental value.
// The operation will fail if the increment is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// inc increment to apply to a field.
func (documentStore *DocumentStore) IncrementFloat64WithContext(
	id *BinaryOrStringId,
	fieldPath string,
	inc float64,
	ctx context.Context,
) error {
	mutation, err := MakeDocumentMutation(IncrementFloat64(fieldPath, inc))
	if err != nil {
		return err
	}
	_, err = documentStore.update(id, nil, &MapOrStructMutation{StructMutation: mutation}, ctx)
	return err
}

// DecrementFloat64WithContext method atomically applies an decrement to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the decremental value.
// The operation will fail if the decrement is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// dec decrement to apply to a field.
func (documentStore *DocumentStore) DecrementFloat64WithContext(
	id *BinaryOrStringId,
	fieldPath string,
	dec float64,
	ctx context.Context,
) error {
	mutation, err := MakeDocumentMutation(DecrementFloat64(fieldPath, dec))
	if err != nil {
		return err
	}
	_, err = documentStore.update(id, nil, &MapOrStructMutation{StructMutation: mutation}, ctx)
	return err
}

// IncrementFloat64ByOne method atomically applies an increment by one to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the incremental value.
// The operation will fail if the increment is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// inc increment to apply to a field.
func (documentStore *DocumentStore) IncrementFloat64ByOne(id *BinaryOrStringId, fieldPath string) error {
	return documentStore.IncrementFloat64ByOneWithContext(id, fieldPath, nil)
}

// DecrementFloat64ByOne method atomically applies an decrement by one to a given field (in dot separated notation)
// of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the decremental value.
// The operation will fail if the decrement is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
func (documentStore *DocumentStore) DecrementFloat64ByOne(id *BinaryOrStringId, fieldPath string) error {
	return documentStore.DecrementFloat64ByOneWithContext(id, fieldPath, nil)
}

// IncrementFloat64ByOneWithContext method atomically applies an increment by one
// to a given field (in dot separated notation) of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the incremental value.
// The operation will fail if the increment is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
// inc increment to apply to a field.
func (documentStore *DocumentStore) IncrementFloat64ByOneWithContext(
	id *BinaryOrStringId,
	fieldPath string,
	ctx context.Context,
) error {
	mutation, err := MakeDocumentMutation(IncrementFloat64ByOne(fieldPath))
	if err != nil {
		return err
	}
	_, err = documentStore.update(id, nil, &MapOrStructMutation{StructMutation: mutation}, ctx)
	return err
}

// DecrementFloat64ByOneWithContext method atomically applies an decrement by one
// to a given field (in dot separated notation) of the given document id. If the field doesn't exist on the server
// then it will be created with the type of the decremental value.
// The operation will fail if the decrement is applied to a
// field that is of a non-numeric type.
// _id string or byte document id
// fieldPath the field name in dot separated notation
func (documentStore *DocumentStore) DecrementFloat64ByOneWithContext(
	id *BinaryOrStringId,
	fieldPath string,
	ctx context.Context,
) error {
	mutation, err := MakeDocumentMutation(DecrementFloat64ByOne(fieldPath))
	if err != nil {
		return err
	}
	_, err = documentStore.update(id, nil, &MapOrStructMutation{StructMutation: mutation}, ctx)
	return err
}

func getDocumentString(id *BinaryOrStringId) (string, error) {
	if id.IsBinary {
		if len(id.Binary) == 0 {
			return "", errors.New("_id can't be empty")
		}

		doc := MakeDocumentFromMap(map[string]interface{}{"_id": id.Binary})
		ser, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		return string(ser), err
	} else {
		if len(id.Str) == 0 {
			return "", errors.New("_id can't be empty")
		}
		doc := MakeDocumentFromMap(map[string]interface{}{"_id": id.Str})
		ser, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		return string(ser), err
	}
}

func getMutationString(documentMutation *MapOrStructMutation) (string, error) {
	if documentMutation.IsMap {
		doc := &Document{documentMap: documentMutation.MapMutation}
		ser, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		return string(ser), err
	} else {
		doc := &Document{documentMap: documentMutation.StructMutation.mutationMap}
		ser, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		return string(ser), err
	}
}

func getConditionString(queryCondition *MapOrStructCondition) (string, error) {
	if queryCondition.isMap {
		doc := &Document{documentMap: queryCondition.MapCondition}
		ser, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		return string(ser), err
	} else {
		doc := &Document{documentMap: queryCondition.StructCondition.conditionContent}
		ser, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		return string(ser), err
	}
}

func (documentStore *DocumentStore) update(
	id *BinaryOrStringId,
	queryCondition *MapOrStructCondition,
	documentMutation *MapOrStructMutation,
	userDefinedContext context.Context,
) (bool, error) {
	docString, err := getDocumentString(id)
	if err != nil {
		return false, err
	}
	mutationString, err := getMutationString(documentMutation)
	if err != nil {
		return false, err
	}
	var conditionString string
	if queryCondition != nil {
		conditionString, err = getConditionString(queryCondition)
		if err != nil {
			return false, err
		}
	}
	var ctx context.Context
	if userDefinedContext != nil {
		ctx = userDefinedContext
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(),
			time.Duration(documentStore.connection.opts.CallTimeoutSeconds)*time.Second)
		defer cancel()
	}
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	request := &UpdateRequest{
		TablePath:       documentStore.storeName,
		PayloadEncoding: PayloadEncoding_JSON_ENCODING,
		Document:        &UpdateRequest_JsonDocument{JsonDocument: docString},
		Mutation:        &UpdateRequest_JsonMutation{JsonMutation: mutationString},
	}
	if queryCondition != nil {
		request.Condition = &UpdateRequest_JsonCondition{JsonCondition: conditionString}
	}
	response, err := documentStore.connection.stub.Update(ctx,
		request,
		grpc.Header(&header),
		grpc.Trailer(&trailer),
	)
	if err != nil {
		return false, err
	}
	documentStore.connection.umd.UpdateToken(header, trailer)
	return checkIsDocumentExists(response.GetError())
}

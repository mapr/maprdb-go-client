package private_maprdb_go_client

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
)

var re = regexp.MustCompile(`^"|['"](\w+(?:\.\w+)*)['"]|(\w+)`)
var arrRgx = regexp.MustCompile(`\[(\d*?)\]`)

// Set of OJAI keys
var ojaiKeys = map[string]interface{}{
	"$numberLong":  "",
	"$numberFloat": "",
	"$binary":      "",
	"$time":        "",
	"$date":        "",
	"$dateDay":     ""}

type Document struct {
	documentMap map[string]interface{}
}

// Type for Document functional options
type DocumentOperations func(document *Document) (*Document, error)

// MakeDocument function creates and returns new Document
func MakeDocument(operations ...DocumentOperations) (*Document, error) {
	var doc = &Document{documentMap: make(map[string]interface{})}
	var err error = nil

	for _, op := range operations {
		doc, err = op(doc)
		if err != nil {
			return nil, err
		}
	}
	return doc, nil
}

// MakeDocumentFromMap function creates and returns new Document from given map[string]interface{}
func MakeDocumentFromMap(initialData map[string]interface{}) *Document {
	return &Document{initialData}
}

// MakeDocumentFromJson function creates and returns new Document from given JSON string
func MakeDocumentFromJson(jsonDocument string) (*Document, error) {
	document := &Document{documentMap: make(map[string]interface{})}
	err := json.Unmarshal([]byte(jsonDocument), document)
	if err != nil {
		return nil, err
	}
	return document, nil
}

// SetIdString functional option which sets not empty _id string field in the Document
func SetIdString(id string) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if len(id) == 0 {
			return nil, errors.New("_id field can't be empty")
		}
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue("_id", id)).(map[string]interface{})
		return doc, nil
	}
}

// SetIdString method sets not empty _id string field in the Document
func (doc *Document) SetIdString(id string) error {
	if len(id) == 0 {
		return errors.New("_id field can't be empty")
	}
	doc.documentMap = mergeMaps(doc.documentMap, doc.newValue("_id", id)).(map[string]interface{})
	return nil
}

// SetIdBinary functional option which sets not empty _id byte field in theDocument
func SetIdBinary(id []byte) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if len(id) == 0 {
			return nil, errors.New("_id field can't be empty")
		}
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue("_id", id)).(map[string]interface{})
		return doc, nil
	}
}

// SetIdBinary method sets not empty _id byte field in the Document
func (doc *Document) SetIdBinary(id []byte) error {
	if len(id) == 0 {
		return errors.New("_id field can't be empty")
	}
	doc.documentMap = mergeMaps(doc.documentMap, doc.newValue("_id", id)).(map[string]interface{})
	return nil
}

// SetString method sets string value to given field path
func (doc *Document) SetString(fieldPath string, value string) *Document {
	doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	return doc
}

// SetString method sets string value to given field path
func SetString(fieldPath string, value string) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetInt method sets int value to given field path
func (doc *Document) SetInt(fieldPath string, value int) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetInt method sets int value to given field path
func SetInt(fieldPath string, value int) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetBool method sets bool value to given field path
func (doc *Document) SetBool(fieldPath string, value bool) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetBool method sets bool value to given field path
func SetBool(fieldPath string, value bool) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetFloat32 method sets float32 value to given field path
func (doc *Document) SetFloat32(fieldPath string, value float32) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetFloat32 method sets float32 value to given field path
func SetFloat32(fieldPath string, value float32) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetFloat64 method sets float64 value to given field path
func (doc *Document) SetFloat64(fieldPath string, value float64) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetFloat64 method sets float64 value to given field path
func SetFloat64(fieldPath string, value float64) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetSlice method sets list[]interface{}value to given field path
func (doc *Document) SetSlice(fieldPath string, value []interface{}) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetSlice method sets list[]interface{}value to given field path
func SetSlice(fieldPath string, value []interface{}) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetNil method sets nil value to given field path
func (doc *Document) SetNil(fieldPath string) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, nil)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, nil)).(map[string]interface{})
	}
	return doc
}

// SetNil method sets nil value to given field path
func SetNil(fieldPath string) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, nil)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, nil)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetByte method sets []byte value to given field path
func (doc *Document) SetByte(fieldPath string, value []byte) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetByte method sets []byte value to given field path
func SetByte(fieldPath string, value []byte) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetMap method sets map[string]interface{} value to given field path
func (doc *Document) SetMap(fieldPath string, value map[string]interface{}) *Document {
	//if doc.IsPathExists(fieldPath) {
	//	doc.Delete(fieldPath)
	//}
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetMap method sets map[string]interface{} value to given field path
func SetMap(fieldPath string, value map[string]interface{}) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		//if doc.IsPathExists(fieldPath) {
		//	doc.Delete(fieldPath)
		//}
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

func mergeMaps(base map[string]interface{}, override interface{}) interface{} {
	if reflect.TypeOf(base) != reflect.TypeOf(override) {
		return override
	}
	mergedMap := copyMap(base)
	for k, v := range override.(map[string]interface{}) {
		if val, ok := mergedMap[k]; ok {
			if mv, ok := val.(map[string]interface{}); ok {
				mergedMap[k] = mergeMaps(mv, v)
			} else {
				mergedMap[k] = v
			}
		} else {
			mergedMap[k] = v
		}
	}
	return mergedMap
}

// Method checks is path exists in the Document.
func (doc *Document) IsPathExists(fieldPath string) bool {
	res, _ := doc.get(fieldPath)
	return res != nil
}

// Internal get method gets value from given field path or returns nil
func (doc *Document) get(fieldPath string) (interface{}, error) {
	tempMap := doc.documentMap
	for key, value := range doc.documentMap {
		tempMap[key] = value
	}
	parsedPath := doc.parseFieldPath(fieldPath)
	for i := 0; i < len(parsedPath); i++ {
		//for index, element := range parsedPath {
		if val, ok := tempMap[parsedPath[i]]; ok {
			if mv, ok := val.(map[string]interface{}); ok {
				tempMap = mv
			} else if mv, ok := val.([]interface{}); ok {
				if i == len(parsedPath)-1 {
					return mv, nil
				}
				i++
				index, err := strconv.Atoi(parsedPath[i])
				if err != nil {
					return nil, err
				}
				value := mv[index]
				if i == len(parsedPath)-1 {
					return value, nil
				} else {
					if mv, ok := value.(map[string]interface{}); ok {
						tempMap = mv
					} else {
						return nil, fmt.Errorf("invalid field path")
					}
				}
			} else {
				if i == len(parsedPath)-1 {
					return val, nil
				} else {
					return nil, nil
				}
			}
		} else {
			return nil, nil
		}
	}
	return tempMap, nil
}

// The Delete function deletes element from given fieldPath if it exists in document.
func (doc *Document) Delete(fieldPath string) *Document {
	if doc.IsPathExists(fieldPath) {
		tempMap := doc.documentMap
		parsedPath := doc.parseFieldPath(fieldPath)
		if len(parsedPath) == 1 {
			delete(doc.documentMap, parsedPath[len(parsedPath)-1])
		} else {
			for i := 0; i <= len(parsedPath)-2; i++ {
				if mv, ok := tempMap[parsedPath[i]].(map[string]interface{}); ok {
					tempMap = mv
				}
			}
			delete(tempMap, parsedPath[len(parsedPath)-1])
		}
	}
	return doc
}

// Method cleans whole document
func (doc *Document) Clean() *Document {
	doc.documentMap = make(map[string]interface{})
	return doc
}

// SetTime method sets OTIme value to given field path
func SetTime(fieldPath string, value *OTime) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetTime method sets OTIme value to given field path
func (doc *Document) SetTime(fieldPath string, value *OTime) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetDate method sets ODate value to given field path
func SetDate(fieldPath string, value *ODate) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetDate method sets ODate value to given field path
func (doc *Document) SetDate(fieldPath string, value *ODate) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// SetTimestamp method sets OTimestamp value to given field path
func SetTimestamp(fieldPath string, value *OTimestamp) DocumentOperations {
	return func(doc *Document) (*Document, error) {
		if arrRgx.MatchString(fieldPath) {
			doc.setArrayValue(fieldPath, value)
		} else {
			doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
		}
		return doc, nil
	}
}

// SetTimestamp method sets OTimestamp value to given field path
func (doc *Document) SetTimestamp(fieldPath string, value *OTimestamp) *Document {
	if arrRgx.MatchString(fieldPath) {
		doc.setArrayValue(fieldPath, value)
	} else {
		doc.documentMap = mergeMaps(doc.documentMap, doc.newValue(fieldPath, value)).(map[string]interface{})
	}
	return doc
}

// Method check is _id key exists in the document map and returns true or false accordingly
func (doc *Document) HasId() bool {
	if _, ok := doc.documentMap["_id"]; ok {
		return true
	} else {
		return false
	}
}

// Method returns _id of document or string zero value if it doesn't exists.
func (doc *Document) GetIdString() (string, error) {
	return doc.GetString("_id")
}

// Method returns _id of document or []byte zero value if it doesn't exists.
func (doc *Document) GetIdBinary() ([]byte, error) {
	return doc.GetByte("_id")
}

// Method returns string value from given path or string zero value.
func (doc *Document) GetString(fieldPath string) (string, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return "", err
	}
	if res, ok := value.(string); ok {
		return res, nil
	}
	return "", nil
}

// Method returns int value from given path or int zero value.
func (doc *Document) GetInt(fieldPath string) (int, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return 0, err
	}
	if res, ok := value.(int); ok {
		return res, nil
	}
	return 0, nil
}

// Method returns true if value for given path is nil or path doesn't exists.
func (doc *Document) IsNil(fieldPath string) (bool, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return false, err
	}
	return value == nil, nil
}

// Method returns float value from given path or string float value.
func (doc *Document) GetFloat64(fieldPath string) (float64, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return 0, err
	}
	if res, ok := value.(float64); ok {
		return res, nil
	}
	return 0, nil
}

// Method returns float value from given path or float zero value.
func (doc *Document) GetFloat32(fieldPath string) (float32, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return 0, err
	}
	if res, ok := value.(float32); ok {
		return res, nil
	}
	return 0, nil
}

// Method returns boolean value from given path or boolean zero value.
func (doc *Document) GetBool(fieldPath string) (bool, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return false, err
	}
	if res, ok := value.(bool); ok {
		return res, nil
	}
	return false, nil
}

// Method returns []byte value from given path or []byte zero value.
func (doc *Document) GetByte(fieldPath string) ([]byte, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return []byte{}, err
	}
	if mv, ok := value.([]byte); ok {
		return mv, nil
	}
	return []byte{}, nil
}

// Method returns []interface{} value from given path otherwise nil
func (doc *Document) GetSlice(fieldPath string) ([]interface{}, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return nil, err
	}
	if mv, ok := value.([]interface{}); ok {
		return mv, nil
	}
	return nil, nil
}

// Method returns map[string]interface{} value from given path otherwise nil
func (doc *Document) GetMap(fieldPath string) (map[string]interface{}, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return nil, err
	}
	if mv, ok := value.(map[string]interface{}); ok {
		return mv, nil
	}
	return nil, nil
}

// Method returns types.Otime object from given path otherwise nil
func (doc *Document) GetTime(fieldPath string) (OTime, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return OTime{}, err
	}
	if mv, ok := value.(OTime); ok {
		return mv, nil
	}
	return OTime{}, nil
}

// Method returns types.ODate object from given path otherwise nil
func (doc *Document) GetDate(fieldPath string) (ODate, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return ODate{}, err
	}
	if mv, ok := value.(ODate); ok {
		return mv, nil
	}
	return ODate{}, nil
}

// Method returns types.OTimestamp object from given path otherwise OTimestamp zero value
func (doc *Document) GetTimestamp(fieldPath string) (OTimestamp, error) {
	value, err := doc.get(fieldPath)
	if err != nil {
		return OTimestamp{}, err
	}
	if mv, ok := value.(OTimestamp); ok {
		return mv, nil
	}
	return OTimestamp{}, nil
}

// Method returns document content as map[string]interface{}
func (doc *Document) AsMap() map[string]interface{} {
	return doc.documentMap
}

// Method returns document content as string
func (doc *Document) AsString() string {
	buffer := bytes.Buffer{}
	for key, value := range doc.documentMap {
		buffer.WriteString(fmt.Sprintf("%v:%v", key, value))
	}
	return buffer.String()
}

// Method returns document content as JSON string
func (doc *Document) AsJsonString() string {
	jsonString, _ := json.Marshal(doc.documentMap)
	return string(jsonString)
}

// Method gets map[string]interface which was parsed from JSON response,
// removes OJAI tags and convert type.
func (doc *Document) responseParser(value map[string]interface{}) error {
	res, err := doc.parseMap(value)
	if err != nil {
		return err
	}
	if r, ok := res.(map[string]interface{}); ok {
		doc.documentMap = r
	}
	return nil
}

func (doc *Document) parseMap(value map[string]interface{}) (interface{}, error) {
	for k, v := range value {
		if _, ok := ojaiKeys[k]; ok {
			return doc.ojaiTypeTranslator(k, v)
		} else {
			vt := reflect.TypeOf(v)
			if vt == nil {
				translatedValue, err := doc.ojaiTypeTranslator("", v)
				if err != nil {
					return nil, err
				}
				value[k] = translatedValue
			} else {
				switch vt.Kind() {
				case reflect.Map:
					if mv, ok := v.(map[string]interface{}); ok {
						parsedMap, err := doc.parseMap(mv)
						if err != nil {
							return nil, err
						}
						value[k] = parsedMap
					} else {
						return nil, fmt.Errorf("map signature must be map[string]interface{}")
					}
				case reflect.Array, reflect.Slice:
					if mv, ok := v.([]interface{}); ok {
						parsedArray, err := doc.parseArray(mv)
						if err != nil {
							return nil, err
						}
						value[k] = parsedArray
					} else {
						return nil, fmt.Errorf("unsupported value type")
					}
				default:
					translatedValue, err := doc.ojaiTypeTranslator(k, v)
					if err != nil {
						return nil, err
					}
					value[k] = translatedValue
				}
			}
		}
	}
	return value, nil
}

func (doc *Document) parseArray(value []interface{}) ([]interface{}, error) {
	for index, element := range value {
		vt := reflect.TypeOf(element)
		switch vt.Kind() {
		case reflect.Map:
			if mv, ok := element.(map[string]interface{}); ok {
				parsedMap, err := doc.parseMap(mv)
				if err != nil {
					return nil, err
				}
				value[index] = parsedMap
			} else {
				return nil, fmt.Errorf("map signature must be map[string]interface{}")
			}
		case reflect.Array, reflect.Slice:
			if mv, ok := element.([]interface{}); ok {
				parsedArray, err := doc.parseArray(mv)
				if err != nil {
					return nil, err
				}
				value[index] = parsedArray
			} else {
				return nil, fmt.Errorf("unsupported value type")
			}
		default:
			translatedValue, err := doc.ojaiTypeTranslator("", element)
			if err != nil {
				return nil, err
			}
			value[index] = translatedValue
		}
	}
	return value, nil
}

// Unmarshaler interface implementation
func (doc *Document) UnmarshalJSON(b []byte) error {
	var docMap map[string]interface{}
	err := json.Unmarshal(b, &docMap)
	if err != nil {
		return err
	}
	err = doc.responseParser(docMap)
	if err != nil {
		return err
	}
	return nil
}

// method converts types to ojai format
func ojaiTypeConversion(value interface{}) interface{} {
	switch v := value.(type) {
	case int, int64, int32:
		return map[string]interface{}{"$numberLong": v}
	case float64, float32:
		return map[string]interface{}{"$numberFloat": v}
	case []byte:
		return map[string]interface{}{"$binary": b64.StdEncoding.EncodeToString(v)}
	case *OTime:
		return map[string]interface{}{"$time": v.String()}
	case *ODate:
		return map[string]interface{}{"$dateDay": v.String()}
	case *OTimestamp:
		return map[string]interface{}{"$date": v.String()}
	case string:
		return v
	default:
		return v
	}
}

func (doc *Document) ojaiTypeTranslator(key string, value interface{}) (interface{}, error) {
	if len(key) == 0 {
		return value, nil
	} else {
		return doc.parseOJAIValueString(key, value)
	}
}

// Internal method which converts given OJAI type to corresponding Golang type
func (doc *Document) parseOJAIValueString(key string, value interface{}) (interface{}, error) {
	switch key {
	case "$numberLong":
		if mv, ok := value.(float64); ok {
			return int(mv), nil
		}
	case "$numberFloat":
		return value, nil
	case "$binary":
		if mv, ok := value.(string); ok {
			val, _ := b64.StdEncoding.DecodeString(mv)
			return val, nil
		}
	case "$dateDay":
		if mv, ok := value.(string); ok {
			return MakeODateFromString(mv)
		}
	case "$time":
		if mv, ok := value.(string); ok {
			return MakeOTimeFromString(mv)
		}
	case "$date":
		if mv, ok := value.(string); ok {
			val := MakeOTimestampFromString(mv)
			return val, nil
		}
	default:
		return value, nil
	}
	return nil, fmt.Errorf("can't parse given key-value pair. unexpected value type")
}

// Implementation of Marshaler interface for JSON encoding.
func (doc *Document) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	count := 0
	serMap := doc.convertDocumentMap()
	length := len(serMap)
	for key, value := range serMap {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		buffer.WriteString(fmt.Sprintf("\"%s\":%s", key, jsonValue))
		count++
		if count < length {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// Util method convert Document map to new formatted OJAI format map
func (doc *Document) convertDocumentMap() map[string]interface{} {
	serMap := copyMap(doc.documentMap)
	throughMap(serMap)
	return serMap
}

// copyMap copies Document map to new map for future serialization
func copyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{})
	for k, v := range m {
		vt := reflect.TypeOf(v)
		if vt == nil {
			cp[k] = v
		} else {
			switch vt.Kind() {
			case reflect.Map:
				if vm, ok := v.(map[string]interface{}); ok {
					cp[k] = copyMap(vm)
				}
			case reflect.Array, reflect.Slice:
				if vm, ok := v.([]interface{}); ok {
					cp[k] = copyArray(vm)
				} else {
					cp[k] = v
				}
			default:
				cp[k] = v
			}
		}
	}
	return cp
}

// copyArray creates a copy of Document content nested array
func copyArray(a []interface{}) []interface{} {
	cpArr := make([]interface{}, len(a))
	for index, element := range a {
		vt := reflect.TypeOf(element)
		if vt == nil {
			cpArr[index] = element
		} else {
			switch vt.Kind() {
			case reflect.Map:
				if vm, ok := element.(map[string]interface{}); ok {
					cpArr[index] = copyMap(vm)
				}
			case reflect.Array, reflect.Slice:
				if vm, ok := element.([]interface{}); ok {
					cpArr[index] = copyArray(vm)
				} else {
					cpArr[index] = element
				}
			default:
				cpArr[index] = element
			}
		}
	}
	return cpArr
}

// Internal method for parse Document map to OJAI format
func throughMap(docMap map[string]interface{}) (map[string]interface{}, error) {
	for k, v := range docMap {
		vt := reflect.TypeOf(v)
		if vt == nil {
			docMap[k] = ojaiTypeConversion(v)
		} else {
			switch vt.Kind() {
			case reflect.Map:
				if mv, ok := v.(map[string]interface{}); ok {
					parsedMap, err := throughMap(mv)
					if err != nil {
						return nil, err
					}
					docMap[k] = parsedMap
				} else {
					return nil, fmt.Errorf("map signature must be map[string]interface{}")
				}
			case reflect.Array, reflect.Slice:
				if mv, ok := v.([]interface{}); ok {
					parsedArray, err := throughArray(mv)
					if err != nil {
						return nil, err
					}
					docMap[k] = parsedArray
				} else if mv, ok := v.([]byte); ok {
					docMap[k] = ojaiTypeConversion(mv)
				} else {
					return nil, fmt.Errorf("unsupported value type")
				}
			default:
				docMap[k] = ojaiTypeConversion(v)
			}
		}
	}
	return docMap, nil
}

// Internal method for parse Document list to OJAI format
func throughArray(arr []interface{}) ([]interface{}, error) {
	for index, element := range arr {
		vt := reflect.TypeOf(element)
		switch vt.Kind() {
		case reflect.Map:
			if mv, ok := element.(map[string]interface{}); ok {
				parsedMap, err := throughMap(mv)
				if err != nil {
					return nil, err
				}
				arr[index] = parsedMap
			} else {
				return nil, fmt.Errorf("map signature must be map[string]interface{}")
			}
		case reflect.Array, reflect.Slice:
			if mv, ok := element.([]interface{}); ok {
				parsedArray, err := throughArray(mv)
				if err != nil {
					return nil, err
				}
				arr[index] = parsedArray
			} else {
				return nil, fmt.Errorf("unsupported value type")
			}
		default:
			arr[index] = ojaiTypeConversion(element)
		}
	}
	return arr, nil
}

// Internal method responsible for the case when fieldPath contains array indexes
func (doc *Document) setArrayValue(fieldPath string, value interface{}) {
	path := doc.parseFieldPath(fieldPath)
	tempStorageMap := doc.documentMap
	var tempStorageArray []interface{}
	for i := 0; i < len(path); i++ {
		fieldName := path[i]
		data := tempStorageMap[fieldName]
		if mv, ok := data.([]interface{}); ok {
			tempStorageArray = mv
			i++
			index, _ := strconv.Atoi(path[i])
			if i == len(path)-1 {
				tempStorageMap[fieldName] = doc.setToArray(tempStorageArray, index, value)
			} else {
				tempStorageMap = tempStorageArray[index].(map[string]interface{})
			}
		} else if mv, ok := data.(map[string]interface{}); ok {
			tempStorageMap = mv
		} else {
			tempStorageMap[fieldName] = value
		}
		//}
	}
}

// Extend array if index is greater then array size, otherwise just set the data at the given index
func (doc *Document) setToArray(array []interface{}, index int, data interface{}) []interface{} {
	if len(array) <= index {
		for len(array) <= index {
			array = append(array, nil)
		}
	}
	array[index] = data
	return array
}

// Internal method creates new map[string]interfaces from given field path and value.
func (doc *Document) newValue(fieldPath string, value interface{}) map[string]interface{} {
	path := doc.parseFieldPath(fieldPath)
	tempMap := make(map[string]interface{})
	for i := len(path) - 1; i >= 0; i-- {
		if len(tempMap) == 0 {
			tempMap[path[i]] = value
		} else {
			tempMap = map[string]interface{}{path[i]: tempMap}
		}
	}
	return tempMap
}

// Internal method parses string field path to well formatted sequence
func (doc *Document) parseFieldPath(fieldPath string) []string {
	i := 0
	result := re.FindAllStringSubmatch(fieldPath, -1)
	path := make([]string, len(result))
	for _, m := range result {
		if m[1] != "" || m[2] != "" {
			path[i] = fmt.Sprintf(m[1] + m[2])
			i++
		}
	}
	return path
}

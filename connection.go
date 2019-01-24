package private_maprdb_go_client

import (
	"context"
	b64 "encoding/base64"
	"errors"
	"fmt"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Connection struct {
	stub    MapRDbServerClient
	umd     userMetadata
	channel *grpc.ClientConn
	opts    *ConnectionOptions
}

// ConnectionOptions apply to all calls for the connections
// MaxAttempt attempt count
// WaitBetweenSeconds delay between attempts in seconds
// CallTimeoutSeconds maximum call timeout
type ConnectionOptions struct {
	MaxAttempt         int
	WaitBetweenSeconds int
	CallTimeoutSeconds int
}

var prefix = "ojai:mapr@"

// Default connection options
// MaxAttempt 9
// WaitBetweenSeconds 12
// CallTimeoutSeconds 60
var defaultConnectionOpts = &ConnectionOptions{MaxAttempt: 9, WaitBetweenSeconds: 12, CallTimeoutSeconds: 60}

// Method creates channel for secure or insecure connection according to input parameters in connection string.
func createChannel(encodedUMD *string, connectionUrl *string,
	ssl *bool,
	sslCA *string,
	sslTargetNameOverride *string,
	conOpts *ConnectionOptions) (*Connection, error) {
	var opts []grpc.DialOption
	if *ssl {
		transportCredentials, err := credentials.NewClientTLSFromFile(*sslCA, *sslTargetNameOverride)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(transportCredentials))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	conn := &Connection{umd: userMetadata{*encodedUMD, ""}}
	if conOpts == nil || conOpts.MaxAttempt < 1 || conOpts.WaitBetweenSeconds < 1 || conOpts.CallTimeoutSeconds < 1 {
		conn.opts = defaultConnectionOpts
	} else {
		conn.opts = conOpts
	}
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(time.Duration(conn.opts.WaitBetweenSeconds) * time.Second)),
		grpc_retry.WithCodes(
			codes.NotFound,
			codes.Unavailable),
		grpc_retry.WithPerRetryTimeout(time.Duration(conn.opts.WaitBetweenSeconds) * time.Second),
		grpc_retry.WithMax(uint(conn.opts.MaxAttempt)),
	}

	opts = append(
		opts,
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				UnaryClientAuthInterceptor(&conn.umd),
				UnaryClientTokenInterceptor(&conn.umd),
				grpc_retry.UnaryClientInterceptor(retryOpts...),
			)),
		grpc.WithStreamInterceptor(
			grpc_middleware.ChainStreamClient(
				StreamClientAuthInterceptor(&conn.umd),
				StreamClientTokenInterceptor(&conn.umd),
				grpc_retry.StreamClientInterceptor(retryOpts...),
			)),
	)
	channel, err := grpc.Dial(*connectionUrl, opts...)
	if err != nil {
		return nil, err
	}
	conn.channel = channel
	return conn, nil
}

// Method pings gRPC server for ensure that connection is established
func pingRequest(connection *Connection) error {
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(connection.opts.CallTimeoutSeconds)*time.Second)
	defer cancel()
	_, err := connection.stub.Ping(ctx,
		&PingRequest{},
		grpc.Header(&header),
		grpc.Trailer(&trailer),
	)
	if err != nil {
		return err
	}
	connection.umd.UpdateToken(header, trailer)
	return nil
}

// Method executes IsStoreExists method for ensure that store with given
// name is exists and return new DocumentStore if result is positive.
func (connection *Connection) GetStore(storeName string) (*DocumentStore, error) {
	res, err := connection.IsStoreExists(storeName)
	if err != nil {
		return nil, err
	}
	if res {
		return &DocumentStore{connection: connection, storeName: storeName}, nil
	} else {
		return nil, errors.New(fmt.Sprintf("store %v not found", storeName))
	}
}

// Method executes TableExists RPC request with given store name and return true if table is exists or false if not.
func (connection *Connection) IsStoreExists(storeName string) (bool, error) {
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(connection.opts.CallTimeoutSeconds)*time.Second)
	defer cancel()

	response, err := connection.stub.TableExists(ctx,
		&TableExistsRequest{TablePath: storeName},
		grpc.Header(&header),
		grpc.Trailer(&trailer))
	if err != nil {
		return false, errors.New(fmt.Sprintf("couldn't execute request: %v", err))
	}
	connection.umd.UpdateToken(header, trailer)
	return checkExistsErrorCode(response.GetError())
}

// Method executes DeleteTable RPC request with given store name.
func (connection *Connection) DeleteStore(storeName string) error {
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(connection.opts.CallTimeoutSeconds)*time.Second)
	defer cancel()

	response, err := connection.stub.DeleteTable(ctx,
		&DeleteTableRequest{TablePath: storeName},
		grpc.Header(&header),
		grpc.Trailer(&trailer))
	if err != nil {
		return err
	}
	err = checkResponseErrorCode(response.GetError())
	if err != nil {
		return err
	}
	connection.umd.UpdateToken(header, trailer)
	return nil
}

// Creates and returns a new instance of an OJAI Document.
func (connection *Connection) CreateDocumentFromString(jsonString string) (*Document, error) {
	return MakeDocumentFromJson(jsonString)
}

// Creates and returns a new, empty instance of an OJAI Document.
func (connection *Connection) CreateEmptyDocument() (*Document, error) {
	return MakeDocument()
}

// Creates and returns a new instance of an OJAI Document.
func (connection *Connection) CreateDocumentFromMap(documentMap map[string]interface{}) *Document {
	return MakeDocumentFromMap(documentMap)
}

// Method executes CreateTable RPC request with given store name and return new DocumentStore.
func (connection *Connection) CreateStore(storeName string) (*DocumentStore, error) {
	header := make(metadata.MD)
	trailer := make(metadata.MD)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(connection.opts.CallTimeoutSeconds)*time.Second)
	defer cancel()

	response, err := connection.stub.CreateTable(ctx,
		&CreateTableRequest{TablePath: storeName},
		grpc.Header(&header),
		grpc.Trailer(&trailer))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("couldn't execute request: %v", err))
	}
	err = checkResponseErrorCode(response.GetError())
	if err != nil {
		return nil, err
	}
	connection.umd.UpdateToken(header, trailer)
	return connection.GetStore(storeName)
}

// Method checks response error code.
func checkResponseErrorCode(rpcError *RpcError) error {
	switch rpcError.ErrCode {
	case ErrorCode_NO_ERROR:
		return nil
	default:
		return errors.New(fmt.Sprintf("unexpected error code recieved from server.\n error: %v.\n"+
			" error message : %v.\n java stacktrace: %v.\n",
			rpcError.ErrCode.String(),
			rpcError.ErrorMessage,
			rpcError.JavaStackTrace))
	}
}

// Method checks IsTableExists  response error code and return true
// if error code is 0 (NO ERROR), false if error code 2(TABLE NOT FOUND) otherwise error.
func checkExistsErrorCode(rpcError *RpcError) (bool, error) {
	switch rpcError.ErrCode {
	case ErrorCode_NO_ERROR:
		return true, nil
	case ErrorCode_TABLE_NOT_FOUND:
		return false, nil
	default:
		return false, errors.New(fmt.Sprintf("unexpected error code recieved from server.\n error: %v.\n"+
			" error message : %v.\n java stacktrace: %v.\n",
			rpcError.ErrCode.String(),
			rpcError.ErrorMessage,
			rpcError.JavaStackTrace))
	}
}

// Method parses input connection string and returns argument or default values.
func parseConnectionString(connectionString string) (
	connectionUrl string,
	auth string,
	encodedMetadata string,
	ssl bool,
	sslCA string,
	sslTargetNameOverride string,
	err error) {
	u, err := url.Parse(connectionString)
	if err != nil {
		connectionString = prefix + connectionString
		u, err = url.Parse(connectionString)
		if err != nil {
			return
		}
	}
	m, _ := url.ParseQuery(u.RawQuery)
	connectionUrl = findHost(fmt.Sprintf("%v:%v", u.Scheme, u.Opaque))
	auth = getValueOrDefault(m, "auth", "basic")
	user := getValueOrDefault(m, "user", "")
	password := getValueOrDefault(m, "password", "")
	encodedMetadata = b64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v:%v", user, password)))
	ssl = false
	if val, ok := m["ssl"]; ok {
		ssl, err = strconv.ParseBool(val[0])
		if err != nil {
			return
		}
	}
	sslCA = getValueOrDefault(m, "sslCA", "")
	sslTargetNameOverride = getValueOrDefault(m, "sslTargetNameOverride", "")
	//TODO add value validation before return
	return
}

// find host or host:port in connection string opaque value
func findHost(unparsedString string) string {
	parsedString := strings.Split(unparsedString, "@")
	return parsedString[len(parsedString)-1]
}

// method fetches value from url.Values or returns default value
func getValueOrDefault(content url.Values, key string, defaultValue string) string {
	if val, ok := content[key]; ok {
		return val[0]
	} else {
		return defaultValue
	}
}

// Function initialize connection and returns new Connection struct
func MakeConnection(connectionString string) (*Connection, error) {
	return MakeConnectionWithRetryOptions(connectionString, nil)
}

// Function initialize connection with specific retry options and returns new Connection struct
func MakeConnectionWithRetryOptions(
	connectionString string,
	connectionOptions *ConnectionOptions,
) (*Connection, error) {
	connectionUrl, auth, encodedMetadata,
		ssl, sslCA, sslTargetNameOverride, err := parseConnectionString(connectionString)
	if err != nil {
		return nil, err
	}
	if auth != "basic" {
		return nil, errors.New("currently server supports only 'basic' authentication")
	}
	connection, err := createChannel(
		&encodedMetadata,
		&connectionUrl,
		&ssl,
		&sslCA,
		&sslTargetNameOverride,
		connectionOptions)
	if err != nil {
		return nil, err
	}
	connection.stub = NewMapRDbServerClient(connection.channel)
	err = pingRequest(connection)
	if err != nil {
		return nil, err
	}
	return connection, nil
}

// Method Close closes gRPC channel.
func (connection *Connection) Close() {
	defer connection.channel.Close()
}

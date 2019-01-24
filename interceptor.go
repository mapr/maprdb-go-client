package private_maprdb_go_client

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// User metadata struct
// encodedUserMetadata - base64 encoded username:password
// token - unique JWT from server
type userMetadata struct {
	encodedUserMetadata, token string
}

// UpdateToken method updates or set JWT token which must be present in gRPC request
func (userMetadata *userMetadata) UpdateToken(header, trailer metadata.MD) {
	if userMetadata.token == "" {
		if val, ok := header["bearer-token"]; ok {
			userMetadata.token = val[0]
		}
	}
}

// Closure function which returns custom UnaryClientInterceptor for channel
func UnaryClientAuthInterceptor(umd *userMetadata) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if umd.token == "" {
			ctx = metadata.AppendToOutgoingContext(ctx,
				"authorization",
				fmt.Sprintf("basic %v", umd.encodedUserMetadata))
		} else {
			ctx = metadata.AppendToOutgoingContext(ctx,
				"authorization",
				fmt.Sprintf("bearer %v", umd.token))
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		return err
	}
}

// UnaryClientTokenInterceptor responsible for unauthenticated response code in Unary calls.
func UnaryClientTokenInterceptor(umd *userMetadata) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if grpc.Code(err) == codes.Unauthenticated {
			if umd.token != "" {
				umd.token = ""
				ctx = metadata.AppendToOutgoingContext(ctx,
					"authorization",
					fmt.Sprintf("basic %v", umd.encodedUserMetadata))
				err = invoker(ctx, method, req, reply, cc, opts...)
			} else {
				return fmt.Errorf("authentication PAM failed on server. %v", err)
			}
		}
		return err
	}
}

func StreamClientAuthInterceptor(umd *userMetadata) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if umd.token == "" {
			ctx = metadata.AppendToOutgoingContext(ctx,
				"authorization",
				fmt.Sprintf("basic %v", umd.encodedUserMetadata))
		} else {
			ctx = metadata.AppendToOutgoingContext(ctx,
				"authorization",
				fmt.Sprintf("bearer %v", umd.token))
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func StreamClientTokenInterceptor(umd *userMetadata) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		clientStream, err := streamer(ctx, desc, cc, method, opts...)
		if grpc.Code(err) == codes.Unauthenticated {
			if umd.token != "" {
				umd.token = ""
				ctx = metadata.AppendToOutgoingContext(ctx,
					"authorization",
					fmt.Sprintf("basic %v", umd.encodedUserMetadata))
				clientStream, err = streamer(ctx, desc, cc, method, opts...)
			} else {
				return nil, fmt.Errorf("authentication PAM failed on server. %v", err)
			}
		}
		return clientStream, err
	}
}

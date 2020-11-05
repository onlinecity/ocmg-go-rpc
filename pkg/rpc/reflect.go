package rpc

import (
	"reflect"
	"strings"

	"github.com/golang/protobuf/proto"
	pb "github.com/onlinecity/ocmg-api/gen/go/oc/pb/rpc"
	"go.uber.org/zap"
)

func ReflectService(service reflect.Type, name string, prefix uint32) *pb.Service {
	// Build list of all procedures
	procedures := make([]*pb.Procedure, 0, service.NumMethod())
	for i := 0; i < service.NumMethod(); i++ {
		m := service.Method(i)
		t := m.Type
		input := make([]*pb.Argument, 0, t.NumIn())
		for n := 0; n < t.NumIn(); n++ {
			if arg := ReflectArgument(t.In(n)); arg != nil {
				input = append(input, arg)
			}
		}
		output := make([]*pb.Argument, 0, t.NumOut())
		for n := 0; n < t.NumOut(); n++ {
			if arg := ReflectArgument(t.Out(n)); arg != nil {
				output = append(output, arg)
			}
		}
		procedures = append(procedures, &pb.Procedure{
			Name:   m.Name,
			Input:  input,
			Output: output,
		})
	}

	// Finally build service
	return &pb.Service{
		Name:          name,
		ServicePrefix: prefix,
		Procedures:    procedures,
	}
}

func ReflectArgument(t reflect.Type) *pb.Argument {
	// Special case for []byte aka []uint8
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return &pb.Argument{
			Type: pb.Encoding_STRING,
		}
	}

	// Special case for protobufs
	if t.Kind() == reflect.Ptr {
		if _, protomsg := t.MethodByName("ProtoMessage"); protomsg {
			if msg, ok := reflect.New(t.Elem()).Interface().(proto.Message); ok {
				fqname := proto.MessageName(msg)
				if sep := strings.LastIndexByte(fqname, '.'); sep != -1 {
					pbpkg := fqname[0:sep]
					pbname := fqname[sep+1:]
					return &pb.Argument{
						Type:    pb.Encoding_PROTOBUF,
						Message: pbname,
						Package: pbpkg,
					}
				}
			}
		}
	}

	switch t.Kind() {
	case reflect.Slice:
		subtype := ReflectArgument(t.Elem())
		subtype.Repeated = true
		return subtype
	case reflect.Ptr:
		return ReflectArgument(t.Elem())
	case reflect.Interface:
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if t.Implements(errorInterface) {
			return nil
		}
		zap.S().Fatalw("unexpected type", "type", t, "kind", t.Kind())
	case reflect.Bool:
		return &pb.Argument{
			Type: pb.Encoding_BOOL,
		}
	case reflect.Int:
		return &pb.Argument{
			Type: pb.Encoding_INT32,
		}
	case reflect.Int8:
		return &pb.Argument{
			Type: pb.Encoding_INT8,
		}
	case reflect.Int16:
		return &pb.Argument{
			Type: pb.Encoding_INT16,
		}
	case reflect.Int32:
		return &pb.Argument{
			Type: pb.Encoding_INT32,
		}
	case reflect.Int64:
		return &pb.Argument{
			Type: pb.Encoding_INT64,
		}
	case reflect.Uint:
		return &pb.Argument{
			Type: pb.Encoding_UINT32,
		}
	case reflect.Uint8:
		return &pb.Argument{
			Type: pb.Encoding_UINT8,
		}
	case reflect.Uint16:
		return &pb.Argument{
			Type: pb.Encoding_UINT16,
		}
	case reflect.Uint32:
		return &pb.Argument{
			Type: pb.Encoding_UINT32,
		}
	case reflect.Uint64:
		return &pb.Argument{
			Type: pb.Encoding_UINT64,
		}
	case reflect.Float32:
		return &pb.Argument{
			Type: pb.Encoding_FLOAT,
		}
	case reflect.Float64:
		return &pb.Argument{
			Type: pb.Encoding_DOUBLE,
		}
	case reflect.String:
		return &pb.Argument{
			Type: pb.Encoding_STRING,
		}
	default:
		zap.S().Fatalw("unexpected type", "type", t, "kind", t.Kind())
	}
	return nil
}

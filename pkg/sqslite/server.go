package sqslite

import (
	"context"
	"net/http"
)

type Server struct {
	methods map[VerbRoute]http.Handler
}

func (s Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {

}

type VerbRoute struct {
	Verb  string
	Route string
}

type Method[Request, Response any] struct {
	VerbRoute VerbRoute
	Handler   func(context.Context, Request) (Response, error)
}

func (m Method[Request, Response]) ServeHTTP(rw http.ResponseWriter, req *http.Request) {}

const (
	serviceProtocolJSON  = "json"
	serviceProtocolQuery = "query"
)

func getRequestProtocol(req *http.Request) string {
	contentType := req.Header.Get("Content-Type")
	if contentType == "application/x-amz-json-1.0" {
		return serviceProtocolJSON
	}
	return serviceProtocolQuery
}

type Serializer interface {
	SerializeError(any)
	SerializeResponse(any)
}

func createSerializer(serviceProtocol string)

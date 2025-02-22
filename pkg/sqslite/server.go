package sqslite

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type Server struct {
	mux *httprouter.Router
}

func (s *Server) Initialize() {
	s.mux = httprouter.New()
	s.mux.GET("/_aws/sqs/messages", s.handleMethod(nil))
	s.mux.POST("/_aws/sqs/messages", s.handleMethod(nil))

	s.mux.GET("/_aws/sqs/messages/:region/:account_id/:queue_name", s.handleMethod(nil))
}

func (s Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	s.mux.ServeHTTP(rw, req)
}

type Handler func(req *http.Request, s serializer)

func (s Server) handleMethod(method Handler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		s := newSerializer(getRequestProtocol(r))
		method(r, s)
	}
}

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

type serializer interface {
	SerializeError(any)
	SerializeResponse(any)
}

func newSerializer(serviceProtocol string) serializer {
	if serviceProtocol == serviceProtocolJSON {
		return new(jsonSerializer)
	}
	return new(querySerializer)
}

type jsonSerializer struct {
	serializer
}

type querySerializer struct {
	serializer
}

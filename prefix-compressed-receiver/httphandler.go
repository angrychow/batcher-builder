package prefix_compressed_receiver // import "go.opentelemetry.io/collector/receiver/otlpreceiver"

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"regexp"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"angrychow/otel/prefix-compressed-receiver/internal/logs"
	"angrychow/otel/prefix-compressed-receiver/internal/metrics"
	"angrychow/otel/prefix-compressed-receiver/internal/trace"
)

// Pre-computed status with code=Internal to be used in case of a marshaling error.
var fallbackMsg = []byte(`{"code": 13, "message": "failed to marshal error message"}`)

const fallbackContentType = "application/json"

func revertSpan(iter map[string]interface{}) []map[string]interface{} {
	ret := make([]map[string]interface{}, 0)
	if iter["Son"] == nil {
		return nil
	}
	_, ok := iter["Son"].([]interface{})
	if !ok {
		return nil
	}
	// revertTraces(iter[0]["Son"].([]map[string]interface{}))
	for _, item := range iter["Son"].([]interface{}) {
		// revertTraces(item[])
		temps := revertSpan(item.(map[string]interface{}))
		if temps == nil {
			ret = append(ret, item.(map[string]interface{}))
		} else {
			for _, temp := range temps {
				temp[item.(map[string]interface{})["AttrName"].(string)] = item.(map[string]interface{})["AttrValue"]
				ret = append(ret, temp)
			}
		}

	}
	// fmt.Println(ret)
	return ret
}

type Attribute_ struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func revertTraces(iters map[string]interface{}) {
	// iter_ := iter
	pattern := "^attr_"
	regExp, _ := regexp.Compile(pattern)
	if iters["resourceSpans"] == nil {
		return
	}

	for _, resourceSpan := range iters["resourceSpans"].([]interface{}) {
		iter := resourceSpan.(map[string]interface{})
		resources, ok := iter["resource"].(map[string]interface{})
		if ok {
			attributes, ok := resources["attributes"].([]interface{})
			if ok {
				for _, attribute_ := range attributes {
					fmt.Printf("Success! \n")
					fmt.Println(attribute_)
					attribute := attribute_.(map[string]interface{})
					attribute["value"] = (attribute["value"].(map[string]interface{}))["Value"]
				}
			}
		}
		if iter["scopeSpans"] == nil {
			return
		}
		scopeSpans, ok := iter["scopeSpans"].([]interface{})
		if !ok {
			fmt.Println("not array")
			return
		}
		for _, scopeSpan := range scopeSpans {
			if scopeSpan.(map[string]interface{})["spans"] == nil {
				return
			}
			var spans_ = make([]interface{}, 0)
			for _, span := range scopeSpan.(map[string]interface{})["spans"].([]interface{}) {
				for _, item := range revertSpan(span.(map[string]interface{})) {
					attributes := make([]Attribute_, 0)
					for key := range item {
						if regExp.MatchString(key) {
							attributes = append(attributes, Attribute_{
								Key: key[5:],
								Value: (func() interface{} {
									if item[key] == "NONE" {
										return nil
									}
									str, ok := item[key].(string)
									if ok {
										return str
									}
									t, ok := item[key].(map[string]interface{})
									if !ok {
										return nil
									}
									return t["Value"]

								})(),
							})
							delete(item, key)
						}
					}
					if len(attributes) > 0 {
						item["attributes"] = attributes
					}
					spans_ = append(spans_, item)
				}
			}
			scopeSpan.(map[string]interface{})["spans"] = spans_
		}
	}
}

func handleTraces(resp http.ResponseWriter, req *http.Request, tracesReceiver *trace.Receiver) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	body, ok := readAndCloseBody(resp, req, enc)
	if !ok {
		return
	}

	// fmt.Println(string(body))
	// fmt.Printf("\n\n\n\n\n\n")
	var body_ map[string]interface{}
	err := json.Unmarshal(body, &body_)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}
	revertTraces(body_)
	body, err = json.Marshal(body_)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}
	fmt.Println(string(body))
	otlpReq, err := enc.unmarshalTracesRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := tracesReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalTracesResponse(otlpResp)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

func handleMetrics(resp http.ResponseWriter, req *http.Request, metricsReceiver *metrics.Receiver) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	body, ok := readAndCloseBody(resp, req, enc)
	if !ok {
		return
	}

	otlpReq, err := enc.unmarshalMetricsRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := metricsReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalMetricsResponse(otlpResp)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

func handleLogs(resp http.ResponseWriter, req *http.Request, logsReceiver *logs.Receiver) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	body, ok := readAndCloseBody(resp, req, enc)
	if !ok {
		return
	}

	otlpReq, err := enc.unmarshalLogsRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := logsReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalLogsResponse(otlpResp)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

func readContentType(resp http.ResponseWriter, req *http.Request) (encoder, bool) {
	if req.Method != http.MethodPost {
		handleUnmatchedMethod(resp)
		return nil, false
	}

	switch getMimeTypeFromContentType(req.Header.Get("Content-Type")) {
	case pbContentType:
		return pbEncoder, true
	case jsonContentType:
		return jsEncoder, true
	default:
		handleUnmatchedContentType(resp)
		return nil, false
	}
}

func readAndCloseBody(resp http.ResponseWriter, req *http.Request, enc encoder) ([]byte, bool) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return nil, false
	}
	if err = req.Body.Close(); err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return nil, false
	}
	return body, true
}

// writeError encodes the HTTP error inside a rpc.Status message as required by the OTLP protocol.
func writeError(w http.ResponseWriter, encoder encoder, err error, statusCode int) {
	s, ok := status.FromError(err)
	if !ok {
		s = errorMsgToStatus(err.Error(), statusCode)
	}
	writeStatusResponse(w, encoder, statusCode, s.Proto())
}

// errorHandler encodes the HTTP error message inside a rpc.Status message as required
// by the OTLP protocol.
func errorHandler(w http.ResponseWriter, r *http.Request, errMsg string, statusCode int) {
	s := errorMsgToStatus(errMsg, statusCode)
	switch getMimeTypeFromContentType(r.Header.Get("Content-Type")) {
	case pbContentType:
		writeStatusResponse(w, pbEncoder, statusCode, s.Proto())
		return
	case jsonContentType:
		writeStatusResponse(w, jsEncoder, statusCode, s.Proto())
		return
	}
	writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
}

func writeStatusResponse(w http.ResponseWriter, enc encoder, statusCode int, rsp *spb.Status) {
	msg, err := enc.marshalStatus(rsp)
	if err != nil {
		writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
		return
	}

	writeResponse(w, enc.contentType(), statusCode, msg)
}

func writeResponse(w http.ResponseWriter, contentType string, statusCode int, msg []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	// Nothing we can do with the error if we cannot write to the response.
	_, _ = w.Write(msg)
}

func errorMsgToStatus(errMsg string, statusCode int) *status.Status {
	if statusCode == http.StatusBadRequest {
		return status.New(codes.InvalidArgument, errMsg)
	}
	return status.New(codes.Unknown, errMsg)
}

func getMimeTypeFromContentType(contentType string) string {
	mediatype, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return mediatype
}

func handleUnmatchedMethod(resp http.ResponseWriter) {
	status := http.StatusMethodNotAllowed
	writeResponse(resp, "text/plain", status, []byte(fmt.Sprintf("%v method not allowed, supported: [POST]", status)))
}

func handleUnmatchedContentType(resp http.ResponseWriter) {
	status := http.StatusUnsupportedMediaType
	writeResponse(resp, "text/plain", status, []byte(fmt.Sprintf("%v unsupported media type, supported: [%s, %s]", status, jsonContentType, pbContentType)))
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ptraceotlp // import "go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

import (
	goJson "encoding/json"
	"fmt"

	"go.opentelemetry.io/collector/pdata/internal"
	otlpcollectortrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/trace/v1"
	"go.opentelemetry.io/collector/pdata/internal/otlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var jsonUnmarshaler = &ptrace.JSONUnmarshaler{}

// ExportRequest represents the request for gRPC/HTTP client/server.
// It's a wrapper for ptrace.Traces data.
type ExportRequest struct {
	orig  *otlpcollectortrace.ExportTraceServiceRequest
	state *internal.State
}

// NewExportRequest returns an empty ExportRequest.
func NewExportRequest() ExportRequest {
	state := internal.StateMutable
	return ExportRequest{
		orig:  &otlpcollectortrace.ExportTraceServiceRequest{},
		state: &state,
	}
}

// NewExportRequestFromTraces returns a ExportRequest from ptrace.Traces.
// Because ExportRequest is a wrapper for ptrace.Traces,
// any changes to the provided Traces struct will be reflected in the ExportRequest and vice versa.
func NewExportRequestFromTraces(td ptrace.Traces) ExportRequest {
	return ExportRequest{
		orig:  internal.GetOrigTraces(internal.Traces(td)),
		state: internal.GetTracesState(internal.Traces(td)),
	}
}

// MarshalProto marshals ExportRequest into proto bytes.
func (ms ExportRequest) MarshalProto() ([]byte, error) {
	return ms.orig.Marshal()
}

// UnmarshalProto unmarshalls ExportRequest from proto bytes.
func (ms ExportRequest) UnmarshalProto(data []byte) error {
	if err := ms.orig.Unmarshal(data); err != nil {
		return err
	}
	otlp.MigrateTraces(ms.orig.ResourceSpans)
	return nil
}

type TrieSpan struct {
	AttrName  string
	AttrValue interface{}
	Son       []interface{}
}

type ScopeSpan struct {
	SchemaUrl string        `json:"schemaUrl,omitempty"`
	Scope     interface{}   `json:"scope,omitempty"`
	Spans     []interface{} `json:"spans,omitempty"`
}

type ExportData struct {
	SchemaUrl  string       `json:"schemaUrl,omitempty"`
	Resource   interface{}  `json:"resource,omitempty"`
	ScopeSpans []*ScopeSpan `json:"scopeSpans,omitempty"`
}

// MarshalJSON marshals ExportRequest into JSON bytes.
func (ms ExportRequest) MarshalJSON() ([]byte, error) {
	// var buf bytes.Buffer
	var attrList = make(map[string][]string, 0)
	// var nameList = make([]string, 0)
	var attrExist = make(map[string]map[string]bool)
	// var nameExist = make(map[string]bool)
	// ms.orig.ResourceSpans[0].ScopeSpans[0].Spans[0]
	// ms.orig.ResourceSpans[0].ScopeSpans[0].Spans[0].
	data := struct {
		ResourceSpans []ExportData `json:"resourceSpans"`
	}{
		ResourceSpans: make([]ExportData, 0),
	}

	for _, rspan := range ms.orig.ResourceSpans {
		rspanNew := ExportData{
			SchemaUrl:  rspan.SchemaUrl,
			Resource:   rspan.Resource,
			ScopeSpans: make([]*ScopeSpan, 0),
		}

		for _, sspan := range rspan.ScopeSpans {
			sspanNew := &ScopeSpan{
				SchemaUrl: sspan.SchemaUrl,
				Scope:     sspan.Scope,
				Spans:     make([]interface{}, 0),
			}

			for _, span := range sspan.Spans {
				spanBytes, err := goJson.Marshal(span)
				if err != nil {
					fmt.Println("JSON encoding error:", err)
					continue
				}

				var spanMap map[string]interface{}
				err = goJson.Unmarshal(spanBytes, &spanMap)
				if err != nil {
					fmt.Println("JSON decoding error:", err)
					continue
				}

				if span.Attributes != nil {
					for _, attribute := range span.Attributes {
						spanMap["attr_"+attribute.Key] = attribute.Value
						if attrExist[span.Name] == nil {
							attrExist[span.Name] = make(map[string]bool)
						}
						if !attrExist[span.Name]["attr_"+attribute.Key] {
							attrExist[span.Name]["attr_"+attribute.Key] = true
							if attrList[span.Name] == nil {
								attrList[span.Name] = make([]string, 0)
							}
							attrList[span.Name] = append(attrList[span.Name], "attr_"+attribute.Key)
						}
						// attribute.Key
					}
					delete(spanMap, "attributes")
				}

				sspanNew.Spans = append(sspanNew.Spans, spanMap)
			}

			rspanNew.ScopeSpans = append(rspanNew.ScopeSpans, sspanNew)
		}

		data.ResourceSpans = append(data.ResourceSpans, rspanNew)
	}

	for _, rspan := range data.ResourceSpans {
		for _, sspan := range rspan.ScopeSpans {
			newSpans := make([]*TrieSpan, 0)
			for _, span := range sspan.Spans {
				temp := span.(map[string]interface{})
				var iter *TrieSpan = nil
				for _, trieSon := range newSpans {
					if trieSon.AttrValue == temp["name"] {
						iter = trieSon
						break
					}
				}
				if iter == nil {
					iter = &TrieSpan{
						AttrName:  "name",
						AttrValue: temp["name"],
						Son:       make([]interface{}, 0),
					}
					newSpans = append(newSpans, iter)
				}
				for index, attrname := range attrList[temp["name"].(string)] {
					// toBePush := make([]TrieSpan, 0)
					var next *TrieSpan = nil
					val_, _ := goJson.Marshal(temp[attrname])
					val := string(val_)
					for _, son := range iter.Son {
						if temp[attrname] == nil {
							if son.(*TrieSpan).AttrValue == "NONE" {
								next = son.(*TrieSpan)
								break
							}
						} else {
							// if goJson.Marshal(son.(*TrieSpan).AttrValue) ==
							val_, _ := goJson.Marshal(son.(*TrieSpan).AttrValue)
							valIter := string(val_)
							fmt.Println(valIter, val)
							if valIter == val {
								next = son.(*TrieSpan)
								break
							}
						}
					}
					if next == nil {
						next = &TrieSpan{
							AttrName: attrname,
							AttrValue: (func() interface{} {
								if temp[attrname] == nil {
									return "NONE"
								} else {
									return temp[attrname]
								}
							})(),
							Son: make([]interface{}, 0),
						}
						iter.Son = append(iter.Son, next)
					}
					iter = next
					if index == len(attrList[temp["name"].(string)])-1 {
						toBePush := make(map[string]interface{})
						for key := range temp {
							if key == "name" || attrExist[temp["name"].(string)][key] {
								continue
							}
							toBePush[key] = temp[key]
						}
						iter.Son = append(iter.Son, toBePush)
						continue
					}
				}
			}
			fmt.Println(newSpans)
			// sspan.Spans = []interface{}
			sspan.Spans = make([]interface{}, 0)
			for _, v := range newSpans {
				sspan.Spans = append(sspan.Spans, v)
			}
		}
	}

	v, _ := goJson.Marshal(data)
	fmt.Println(string(v))
	// var t map[string]interface{}
	// _ = goJson.Unmarshal(v, &t)
	// revertTraces(t)
	// v, _ = goJson.Marshal((t))
	// fmt.Println(string(v))
	// fmt.Println("\n\n\n\n ------- \n\n\n\n")
	// fmt.Println("\n\n\n\n ------- \n\n\n\n")
	origMarshalData, _ := goJson.Marshal(ms.orig)
	fmt.Println(string(origMarshalData))
	fmt.Printf("compression rate: %f \n", float32(len(v))/float32(len(origMarshalData)))

	// if err := json.Marshal(&buf, ms.orig); err != nil {
	// 	return nil, err
	// }
	return v, nil
}

// UnmarshalJSON unmarshalls ExportRequest from JSON bytes.
func (ms ExportRequest) UnmarshalJSON(data []byte) error {
	td, err := jsonUnmarshaler.UnmarshalTraces(data)
	if err != nil {
		return err
	}
	*ms.orig = *internal.GetOrigTraces(internal.Traces(td))
	return nil
}

func (ms ExportRequest) Traces() ptrace.Traces {
	return ptrace.Traces(internal.NewTraces(ms.orig, ms.state))
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ptraceotlp // import "go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

import (
	goJson "encoding/json"
	"fmt"
	"math/rand"
	"strconv"

	"go.opentelemetry.io/collector/pdata/internal"
	otlpcollectortrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/trace/v1"
	"go.opentelemetry.io/collector/pdata/internal/otlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var jsonUnmarshaler = &ptrace.JSONUnmarshaler{}
var attrNameDictionary = make(map[string]string)
var dictCounter = 0

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
	AN    string
	AV    interface{}
	Son   []interface{}
	Count int `json:"-"`
}

type ScopeSpan struct {
	SchemaUrl string        `json:"schemaUrl,omitempty"`
	Scope     interface{}   `json:"scope,omitempty"`
	TOffset   uint64        `json:"tOffset,omitempty"`
	Spans     []interface{} `json:"spans,omitempty"`
}

type ExportData struct {
	SchemaUrl  string       `json:"schemaUrl,omitempty"`
	Resource   interface{}  `json:"resource,omitempty"`
	ScopeSpans []*ScopeSpan `json:"scopeSpans,omitempty"`
}

type UpdatesEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var trieSpanProto []*TrieSpan = nil
var attrList = make(map[string][]string, 0)
var attrExist = make(map[string]map[string]bool)
var recordsList = make(map[string]int, 0) // accumulating calculate
var totalRecord = 0

// MarshalJSON marshals ExportRequest into JSON bytes.
func (ms ExportRequest) MarshalJSON() ([]byte, []UpdatesEntry, error) {

	isUpdateDictionary := false

	updatesEntry := make([]UpdatesEntry, 0)
	// var buf bytes.Buffer

	// var nameList = make([]string, 0)
	// var nameExist = make(map[string]bool)
	// ms.orig.ResourceSpans[0].ScopeSpans[0].Spans[0]
	// ms.orig.ResourceSpans[0].ScopeSpans[0].Spans[0].
	data := struct {
		ResourceSpans []ExportData `json:"resourceSpans"`
	}{
		ResourceSpans: make([]ExportData, 0),
	}

	// the following step is to flat the attributes object into attr_name format

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
			var minTime uint64 = 1<<63 - 1

			for _, span := range sspan.Spans {
				totalRecord++
				recordsList[span.Name]++
				spanBytes, err := goJson.Marshal(span)
				if err != nil {
					fmt.Println("JSON encoding error:", err)
					continue
				}
				var spanMap map[string]interface{}
				err = goJson.Unmarshal(spanBytes, &spanMap)
				spanMap["stun"] = span.StartTimeUnixNano
				spanMap["etun"] = span.EndTimeUnixNano
				delete(spanMap, "start_time_unix_nano")
				delete(spanMap, "end_time_unix_nano")
				if err != nil {
					fmt.Println("JSON decoding error:", err)
					continue
				}
				if span.Attributes != nil {
					for _, attribute := range span.Attributes {

						if _, exists := attrNameDictionary[attribute.Key]; !exists {
							isUpdateDictionary = true
							attrNameDictionary[attribute.Key] = strconv.Itoa(dictCounter)
							updatesEntry = append(updatesEntry, UpdatesEntry{
								Key:   attribute.Key,
								Value: strconv.Itoa(dictCounter),
							})
							dictCounter++
						}

						spanMap["attr_"+string(attrNameDictionary[attribute.Key])] = attribute.Value
						if attrExist[span.Name] == nil {
							attrExist[span.Name] = make(map[string]bool)
						}
						if !attrExist[span.Name]["attr_"+string(attrNameDictionary[attribute.Key])] {
							attrExist[span.Name]["attr_"+string(attrNameDictionary[attribute.Key])] = true
							if attrList[span.Name] == nil {
								attrList[span.Name] = make([]string, 0)
							}
							attrList[span.Name] = append(attrList[span.Name], "attr_"+string(attrNameDictionary[attribute.Key]))
						}
						// attribute.Key
					}
					delete(spanMap, "attributes")
				}
				minTime = min(span.StartTimeUnixNano, minTime)
				sspanNew.Spans = append(sspanNew.Spans, spanMap)
			}
			for _, span_ := range sspanNew.Spans {
				span := span_.(map[string]interface{})
				span["stun"] = span["stun"].(uint64) - minTime
				span["etun"] = span["etun"].(uint64) - minTime
			}
			sspanNew.TOffset = minTime
			rspanNew.ScopeSpans = append(rspanNew.ScopeSpans, sspanNew)
		}

		data.ResourceSpans = append(data.ResourceSpans, rspanNew)
	}

	// the following step is to turn span into trie format

	for _, rspan := range data.ResourceSpans {
		for _, sspan := range rspan.ScopeSpans {
			if trieSpanProto == nil {
				trieSpanProto = make([]*TrieSpan, 0)
			}
			newSpans := make([]*TrieSpan, 0)
			for _, span := range sspan.Spans {
				abnormalDetect := false
				temp := span.(map[string]interface{})
				var iter *TrieSpan = nil
				var iterProto *TrieSpan = nil
				for _, trieSon := range newSpans { // find next hop
					if trieSon.AV == temp["name"] {
						iter = trieSon
						break
					}
				}
				for _, spanProto := range trieSpanProto { // do the same thing in trieSpanProto
					if spanProto.AV == temp["name"] {
						iterProto = spanProto
						spanProto.Count += 1
						break
					}
				}
				if iter == nil { // if didn't find, create it
					iter = &TrieSpan{
						AN:  "name",
						AV:  temp["name"],
						Son: make([]interface{}, 0),
					}
					newSpans = append(newSpans, iter)
				}
				if iterProto == nil { // if didn't find, create it, do it in trieSpanProto too.
					iterProto = &TrieSpan{
						AN:    "name",
						AV:    temp["name"],
						Son:   make([]interface{}, 0),
						Count: 1,
					}
					trieSpanProto = append(trieSpanProto, iterProto)
				}
				if recordsList[temp["name"].(string)] > 0 && totalRecord/len(attrList)/10 >= recordsList[temp["name"].(string)] { // rare name
					abnormalDetect = true
				}
				if len(attrList[temp["name"].(string)]) != 0 {
					for index, attrname := range attrList[temp["name"].(string)] {
						// toBePush := make([]TrieSpan, 0)
						var next *TrieSpan = nil
						var nextProto *TrieSpan = nil
						val_, _ := goJson.Marshal(temp[attrname])
						val := string(val_)
						for _, son := range iter.Son {
							if temp[attrname] == nil {
								if son.(*TrieSpan).AV == "NONE" {
									next = son.(*TrieSpan)
									break
								}
							} else {
								// if goJson.Marshal(son.(*TrieSpan).AV) ==
								val_, _ := goJson.Marshal(son.(*TrieSpan).AV)
								valIter := string(val_)
								// fmt.Println(valIter, val)
								if valIter == val {
									next = son.(*TrieSpan)
									break
								}
							}
						}
						for _, son := range iterProto.Son {
							if temp[attrname] == nil {
								if son.(*TrieSpan).AV == "NONE" {
									nextProto = son.(*TrieSpan)
									break
								}
							} else {
								// if goJson.Marshal(son.(*TrieSpan).AV) ==
								// fmt.Println(son)
								val_, _ := goJson.Marshal(son.(*TrieSpan).AV)
								valIter := string(val_)
								// fmt.Println(valIter, val)
								if valIter == val {
									nextProto = son.(*TrieSpan)
									nextProto.Count += 1
									break
								}
							}
						}
						if next == nil {
							next = &TrieSpan{
								AN: attrname,
								AV: (func() interface{} {
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
						if nextProto == nil {
							nextProto = &TrieSpan{
								AN: attrname,
								AV: (func() interface{} {
									if temp[attrname] == nil {
										return "NONE"
									} else {
										return temp[attrname]
									}
								})(),
								Son:   make([]interface{}, 0),
								Count: 1,
							}
							iterProto.Son = append(iterProto.Son, nextProto)
						}
						// INDICATE ABNORMAL RATE !!!!!!!
						if len(iterProto.Son) > 0 && recordsList[temp["name"].(string)]/len(iterProto.Son)/10 >= nextProto.Count {
							abnormalDetect = true
						}

						iter = next
						iterProto = nextProto

						if index == len(attrList[temp["name"].(string)])-1 {
							rand := rand.Int() % 2 // sample rate : 50%
							if rand != 0 && !abnormalDetect {
								continue
							}
							toBePush := make(map[string]interface{})
							for key := range temp {
								if key == "name" || attrExist[temp["name"].(string)][key] {
									continue
								}
								toBePush[key] = temp[key]
							}
							iter.Son = append(iter.Son, toBePush)
							if abnormalDetect {
								fmt.Println("abnormal detect")
								fmt.Println(temp)
							}
							continue
						}
					}
				} else { // no attributes
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
			// fmt.Println(newSpans)
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
	if isUpdateDictionary {
		return v, updatesEntry, nil
	} else {
		return v, nil, nil
	}

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

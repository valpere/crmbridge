// Package fakenp is an in-memory Nova Poshta tracking endpoint for demos and
// tests. It answers TrackingDocument/getStatusDocuments in the documented
// shape; POST /_set {"number","code"} moves a parcel to a status code.
package fakenp

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
)

var texts = map[int]string{
	1: "Відправник самостійно створив цю накладну, але ще не надав до відправки",
	3: "Номер не знайдено", 4: "Відправлення у місті відправника", 5: "Відправлення прямує до міста одержувача",
	6: "Відправлення у місті одержувача", 7: "Прибув на відділення", 8: "Прибув на поштомат",
	9: "Відправлення отримано", 102: "Відмова одержувача",
}

type Server struct {
	mu       sync.Mutex
	codes    map[string]int
	failNext int
	Requests int
	Numbers  int // total document numbers asked about
}

func New() *Server { return &Server{codes: map[string]int{}} }

func (s *Server) Set(number string, code int) { s.mu.Lock(); s.codes[number] = code; s.mu.Unlock() }
func (s *Server) FailNext(n int)              { s.mu.Lock(); s.failNext = n; s.mu.Unlock() }
func (s *Server) Stats() (requests, numbers int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Requests, s.Numbers
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v2.0/json/", s.track)
	mux.HandleFunc("POST /_set", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Number string `json:"number"`
			Code   int    `json:"code"`
		}
		if json.NewDecoder(r.Body).Decode(&v) != nil || v.Number == "" {
			http.Error(w, "number and code required", http.StatusBadRequest)
			return
		}
		s.Set(v.Number, v.Code)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /_fail", func(w http.ResponseWriter, r *http.Request) {
		var v struct{ N int }
		_ = json.NewDecoder(r.Body).Decode(&v)
		s.FailNext(v.N)
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (s *Server) track(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ModelName        string `json:"modelName"`
		CalledMethod     string `json:"calledMethod"`
		MethodProperties struct {
			Documents []struct {
				DocumentNumber string `json:"DocumentNumber"`
			} `json:"Documents"`
		} `json:"methodProperties"`
	}
	w.Header().Set("Content-Type", "application/json")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Requests++
	if s.failNext > 0 {
		s.failNext--
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if json.NewDecoder(r.Body).Decode(&in) != nil || in.ModelName != "TrackingDocument" || in.CalledMethod != "getStatusDocuments" {
		_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "data": []any{}, "errors": []string{"unknown model or method"}})
		return
	}
	if n := len(in.MethodProperties.Documents); n == 0 || n > 100 {
		_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "data": []any{}, "errors": []string{"between 1 and 100 documents are required"}})
		return
	}
	data := []map[string]any{}
	for _, d := range in.MethodProperties.Documents {
		s.Numbers++
		code, ok := s.codes[d.DocumentNumber]
		if !ok {
			code = 3
		}
		t := texts[code]
		if t == "" {
			t = "Статус " + strconv.Itoa(code)
		}
		data = append(data, map[string]any{"Number": d.DocumentNumber, "StatusCode": strconv.Itoa(code), "Status": t})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data, "errors": []string{}, "warnings": []string{}, "info": []string{}})
}

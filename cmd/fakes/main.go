// Command fakes runs the SalesDrive and Nova Poshta stand-ins for local demos.
//
//	SalesDrive on :9101  (X-Api-Key: demo-key; POST /_fail {"n":3}, POST /_lose {"n":1},
//	                      POST /_ttn {"id":1001,"ttn":"..."}, GET /_orders)
//	Nova Poshta on :9102 (POST /_set {"number":"...","code":7})
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/valpere/crmbridge/internal/fakenp"
	"github.com/valpere/crmbridge/internal/fakesalesdrive"
)

func main() {
	sdAddr := flag.String("salesdrive", ":9101", "SalesDrive stand-in address")
	npAddr := flag.String("novaposhta", ":9102", "Nova Poshta stand-in address")
	key := flag.String("api-key", "demo-key", "SalesDrive API key")
	hook := flag.String("webhook", "http://localhost:8788/webhooks/salesdrive?token=demo-secret", "where the SalesDrive stand-in sends its webhooks (empty disables)")
	flag.Parse()

	sd := fakesalesdrive.New(*key)
	sd.WebhookURL = *hook
	go func() { log.Fatal(http.ListenAndServe(*sdAddr, sd.Handler())) }()
	log.Printf("fake SalesDrive on %s, fake Nova Poshta on %s", *sdAddr, *npAddr)
	log.Fatal(http.ListenAndServe(*npAddr, fakenp.New().Handler()))
}

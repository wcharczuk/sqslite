package sqslite

type Policy struct {
	Version    string            `json:"Version"`
	Statements []PolicyStatement `json:"Statement"`
}

type PolicyStatement struct {
	SID       string   `json:"Sid"`
	Effect    string   `json:"Effect"`
	Action    []string `json:"Action"`
	Resource  any      `json:"Resource"`
	Condition any      `json:"Condition"`
}

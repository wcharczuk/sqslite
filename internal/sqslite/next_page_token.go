package sqslite

import (
	"encoding/hex"
	"encoding/json"
)

func parseNextPageToken(token string) (output nextPageToken) {
	d, _ := hex.DecodeString(token)
	_ = json.Unmarshal(d, &output)
	return
}

type nextPageToken struct {
	Offset int
}

func (npt nextPageToken) String() string {
	data, _ := json.Marshal(npt)
	return hex.EncodeToString(data)
}

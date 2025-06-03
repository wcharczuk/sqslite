package integration

import (
	"fmt"
	"regexp"

	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

func normalizeRequest(req spy.Request) spy.Request {
	req.ResponseBody = normalizeQueueURLs(req.ResponseBody)
	req.ResponseBody = normalizeQueueARNs(req.ResponseBody)
	return req
}

func normalizeQueueURLs(corpus string) string {
	return regexpQueueURL.ReplaceAllString(corpus, fmt.Sprintf("http://%s/%s/$4", sqslite.DefaultHost, sqslite.DefaultAccountID))
}

var regexpQueueURL = regexp.MustCompile(`(http|https):\/\/([0-9,\.\:]+)\/([0-9,a-z,A-Z,_,-]+)\/([0-9,a-z,A-Z,_,-]+)`)

func normalizeQueueARNs(corpus string) string {
	return regexpQueueARN.ReplaceAllString(corpus, fmt.Sprintf("arn:aws:sqs:$1:%s:$3", sqslite.DefaultAccountID))
}

var regexpQueueARN = regexp.MustCompile(`arn:aws:sqs:([0-9,a-z,-]+):([0-9,a-z,A-Z]+):([0-9,a-z,A-Z,_,-]+)`)

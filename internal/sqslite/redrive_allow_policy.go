package sqslite

import "slices"

// RedrivePolicy is the json data in the [types.QueueAttributeNameRedrivePolicy] attribute field.
type RedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

type RedrivePermission string

const (
	RedrivePermissionAllowAll RedrivePermission = "allowAll"
	RedrivePermissionDenyAll  RedrivePermission = "denyAll "
	RedrivePermissionByQueue  RedrivePermission = "byQueue"
)

// RedriveAllowPolicy is the json data in the [types.QueueAttributeNameRedrivePolicy] attribute field.
type RedriveAllowPolicy struct {
	RedrivePermission RedrivePermission `json:"redrivePermission"`
	SourceQueueARNs   []string          `json:"sourceQueueArns"`
}

// AllowSource returns if a given destination redrive allow policy
// allows a given source arn.
func (r RedriveAllowPolicy) AllowSource(sourceARN string) bool {
	if r.RedrivePermission == RedrivePermissionAllowAll {
		return true
	}
	if r.RedrivePermission == RedrivePermissionDenyAll {
		return false
	}
	if r.RedrivePermission == RedrivePermissionByQueue {
		return slices.ContainsFunc(r.SourceQueueARNs, func(arn string) bool {
			return sourceARN == arn
		})
	}
	return false /*fail closed*/
}

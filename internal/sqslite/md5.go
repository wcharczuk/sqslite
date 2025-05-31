package sqslite

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"maps"
	"slices"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func md5sum(values ...string) string {
	hf := md5.New()
	for _, v := range values {
		hf.Write([]byte(v))
	}
	return hex.EncodeToString(hf.Sum(nil))
}

// md5OfMessageAttributes performs a checksum on the message attribute values.
//
// This is required to function when we send messages to the queue.
func md5OfMessageAttributes(attributes map[string]types.MessageAttributeValue) string {
	keys := slices.Collect(maps.Keys(attributes))
	sort.Strings(keys)
	var buffer []byte
	for _, key := range keys {
		attribute := attributes[key]
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(key)))
		buffer = append(buffer, []byte(key)...)
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(*attribute.DataType)))
		buffer = append(buffer, []byte(*attribute.DataType)...)
		if attribute.StringValue != nil && *attribute.StringValue != "" {
			buffer = append(buffer, byte(1))
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(*attribute.StringValue)))
			buffer = append(buffer, []byte(*attribute.StringValue)...)
		} else {
			buffer = append(buffer, byte(2))
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(attribute.BinaryValue)))
			buffer = append(buffer, attribute.BinaryValue...)
		}
	}
	return hex.EncodeToString(md5.New().Sum(buffer))
}

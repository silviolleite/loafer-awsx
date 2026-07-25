package producer

import "fmt"

// topicARNPattern is the canonical SNS topic ARN layout:
// arn:aws:sns:<region>:<accountID>:<topicName>.
const topicARNPattern = "arn:aws:sns:%s:%s:%s"

// BuildTopicARN constructs a full SNS topic ARN from its components. The result
// follows the pattern arn:aws:sns:<region>:<accountID>:<topicName>. The inputs
// are not validated; callers are responsible for providing non-empty values.
func BuildTopicARN(region, accountID, topicName string) string {
	return fmt.Sprintf(topicARNPattern, region, accountID, topicName)
}

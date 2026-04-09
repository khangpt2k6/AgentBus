package agentstream

import "strings"

// DecideRetryRoute determines whether a failed event should be retried
// on the source topic or moved to DLQ based on max attempts.
func DecideRetryRoute(currentAttempt, maxAttempts int) (toDLQ bool, nextAttempt int) {
	if currentAttempt < 1 {
		currentAttempt = 1
	}
	nextAttempt = currentAttempt + 1
	if maxAttempts <= 0 {
		return false, nextAttempt
	}
	return nextAttempt > maxAttempts, nextAttempt
}

func ResolveDLQTopic(primaryTopic, dlqTopic string) string {
	if strings.TrimSpace(dlqTopic) != "" {
		return strings.TrimSpace(dlqTopic)
	}
	return primaryTopic + ".dlq"
}

func IsDLQTopic(topic string) bool {
	return strings.HasSuffix(strings.TrimSpace(topic), ".dlq")
}

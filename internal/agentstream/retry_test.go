package agentstream

import "testing"

func TestDecideRetryRoute(t *testing.T) {
	tests := []struct {
		name           string
		currentAttempt int
		maxAttempts    int
		wantDLQ        bool
		wantNext       int
	}{
		{
			name:           "requeue when below max",
			currentAttempt: 1,
			maxAttempts:    3,
			wantDLQ:        false,
			wantNext:       2,
		},
		{
			name:           "send to dlq when exceeding max",
			currentAttempt: 3,
			maxAttempts:    3,
			wantDLQ:        true,
			wantNext:       4,
		},
		{
			name:           "unbounded retries when max disabled",
			currentAttempt: 4,
			maxAttempts:    0,
			wantDLQ:        false,
			wantNext:       5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotDLQ, gotNext := DecideRetryRoute(tc.currentAttempt, tc.maxAttempts)
			if gotDLQ != tc.wantDLQ || gotNext != tc.wantNext {
				t.Fatalf("DecideRetryRoute(%d,%d)=(%v,%d) want (%v,%d)", tc.currentAttempt, tc.maxAttempts, gotDLQ, gotNext, tc.wantDLQ, tc.wantNext)
			}
		})
	}
}

func TestResolveDLQTopic(t *testing.T) {
	if got := ResolveDLQTopic("agent-events", ""); got != "agent-events.dlq" {
		t.Fatalf("default dlq topic=%q", got)
	}
	if got := ResolveDLQTopic("agent-events", "custom-dlq"); got != "custom-dlq" {
		t.Fatalf("custom dlq topic=%q", got)
	}
}

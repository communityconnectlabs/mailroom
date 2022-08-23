package amazonconnect_test

import (
	"github.com/nyaruka/gocommon/httpx"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"github.com/nyaruka/mailroom/services/tickets/amazonconnect"
	"time"
)

const (
	endpointURL = "https://aws.lambda.com"
	authToken   = "12345"
)

func TestCreateMessage(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		endpointURL + "/connect-agent": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{ "error": "error calling Amazon Connect" }`),
			httpx.NewMockResponse(200, nil, `{ "ticket": "12345", "contactId": "12345", "participantId": "12345", "userId": "12345", "datetime": "2022-09-20 00:20:00" }`),
		},
	}))

	client := amazonconnect.NewClient(http.DefaultClient, nil, authToken, endpointURL)
	chatMessage := &amazonconnect.CreateChatMessageParams{
		Message:    "Testing",
		Timestamp:  time.Now().Format(time.RFC3339),
		Timezone:   "UTC",
		Identifier: "+19999999999",
		Ticket:     "12345",
	}

	_, _, err := client.CreateMessage(chatMessage)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateMessage(chatMessage)
	assert.Error(t, err)

	_, trace, err := client.CreateMessage(chatMessage)
	assert.NoError(t, err)
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 123\r\n\r\n", string(trace.ResponseTrace))
}

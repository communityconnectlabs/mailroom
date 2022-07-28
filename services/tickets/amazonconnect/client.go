package amazonconnect

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"github.com/google/go-querystring/query"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
)

type baseClient struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	authToken   string
	endpointURL string
}

func newBaseClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, authToken string, endpointURL string) baseClient {
	return baseClient{
		httpClient:  httpClient,
		httpRetries: httpRetries,
		authToken:   authToken,
		endpointURL: endpointURL,
	}
}

type errorResponse struct {
	Code     int32  `json:"code,omitempty"`
	Message  string `json:"message,omitempty"`
	MoreInfo string `json:"more_info,omitempty"`
	Status   int32  `json:"status,omitempty"`
}

func (c *baseClient) request(method, endpoint string, payload url.Values, response interface{}) (*httpx.Trace, error) {
	fullUrl := fmt.Sprintf("%s/%s", c.endpointURL, endpoint)
	headers := map[string]string{
		"Authorization": fmt.Sprintf("%s", c.authToken),
		"Content-Type":  "application/json",
	}
	var body io.Reader

	if payload != nil {
		data, err := jsonx.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}

	req, err := httpx.NewRequest(method, fullUrl, body, headers)
	if err != nil {
		return nil, err
	}

	trace, err := httpx.DoTrace(c.httpClient, req, c.httpRetries, nil, -1)
	if err != nil {
		return trace, err
	}

	if trace.Response.StatusCode >= 400 {
		response := &errorResponse{}
		err := jsonx.Unmarshal(trace.ResponseBody, response)
		if err != nil {
			return trace, err
		}
		return trace, errors.New(response.Message)
	}

	if response != nil {
		return trace, jsonx.Unmarshal(trace.ResponseBody, response)
	}
	return trace, nil
}

func (c *baseClient) post(endpoint string, payload url.Values, response interface{}) (*httpx.Trace, error) {
	return c.request("POST", endpoint, payload, response)
}

func (c *baseClient) get(endpoint string, payload url.Values, response interface{}) (*httpx.Trace, error) {
	return c.request("GET", endpoint, payload, response)
}

type Client struct {
	baseClient
}

// NewClient returns a new twilio api client.
func NewClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, authToken string, endpointUrl string) *Client {
	return &Client{
		baseClient: newBaseClient(httpClient, httpRetries, authToken, endpointUrl),
	}
}

// CreateMessage create a message in chat channel.
func (c *Client) CreateMessage(message *CreateChatMessageParams) (*CreateChatMessageResult, *httpx.Trace, error) {
	response := &CreateChatMessageResult{}
	data, err := query.Values(message)
	if err != nil {
		return nil, nil, err
	}
	data = removeEmpties(data)
	trace, err := c.post("connect-agent", data, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

// CompleteTask updates an Amazon Connect Task as completed
func (c *Client) CompleteTask(taskSid string) {}

type CreateChatMessageParams struct {
	Messages   []ChatMessage `json:"messages,omitempty"`
	Identifier string        `json:"identifier,omitempty"`
	Ticket     string        `json:"ticket:omitempty"`
}

type CreateChatMessageResult struct {
	ContactID string `json:"contact_id:omitempty"`
}

type ChatMessage struct {
	SegmentId string `json:"segmentId"`
	Text      string `json:"text"`
	Timestamp string `json:"timestamp"`
	Timezone  string `json:"timezone"`
}

// removeEmpties remove empty values from url.Values
func removeEmpties(uv url.Values) url.Values {
	for k, v := range uv {
		if len(v) == 0 || len(v[0]) == 0 {
			delete(uv, k)
		}
	}
	return uv
}

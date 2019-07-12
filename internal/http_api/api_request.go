package http_api

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// A custom http.Transport with support for deadline timeouts
func NewDeadlineTransport(connectTimeout time.Duration, requestTimeout time.Duration) *http.Transport {
	// arbitrary values copied from http.DefaultTransport
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout, //设置建立tcp连接的时间
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ResponseHeaderTimeout: requestTimeout, //设置读取response header的时间（发送请求后，读完返回包头的时间，不包括读返回body的时间），默认是0，无限等待
		MaxIdleConns:          100, //连接池对所有host的最大连接数量，默认无穷大
		IdleConnTimeout:       90 * time.Second, //控制连接池中一个连接可以idle多长时间。
		TLSHandshakeTimeout:   10 * time.Second, //限制 TLS握手的时间
	}
	return transport
}

type Client struct {
	c *http.Client
}

func NewClient(tlsConfig *tls.Config, connectTimeout time.Duration, requestTimeout time.Duration) *Client {
	transport := NewDeadlineTransport(connectTimeout, requestTimeout)
	transport.TLSClientConfig = tlsConfig
	return &Client{
		c: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,//这个超时时间管的太宽了。其覆盖了一个请求的全部过程，从连接建立一直到获得响应结果。
		},
	}
}

// GETV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func (c *Client) GETV1(endpoint string, v interface{}) error {
retry:
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}
	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}

	return nil
}

// PostV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func (c *Client) POSTV1(endpoint string) error {
retry:
	req, err := http.NewRequest("POST", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	return nil
}

func httpsEndpoint(endpoint string, body []byte) (string, error) {
	var forbiddenResp struct {
		HTTPSPort int `json:"https_port"`
	}
	err := json.Unmarshal(body, &forbiddenResp)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", err
	}

	u.Scheme = "https"
	u.Host = net.JoinHostPort(host, strconv.Itoa(forbiddenResp.HTTPSPort))
	return u.String(), nil
}

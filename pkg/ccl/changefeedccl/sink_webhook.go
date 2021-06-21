// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	applicationTypeJSON = `application/json`
	authorizationHeader = `Authorization`
	workerPoolSize      = 17
)

func isWebhookSink(u *url.URL) bool {
	switch u.Scheme {
	// allow HTTP here but throw an error later to make it clear HTTPS is required
	case changefeedbase.SinkSchemeWebhookHTTP, changefeedbase.SinkSchemeWebhookHTTPS:
		return true
	default:
		return false
	}
}

type webhookSinkPayload struct {
	payload []json.RawMessage `json:"payload"`
}

func encodePayloadWebhook(value []byte) ([]byte, error) {
	var payload json.RawMessage
	if err := json.Unmarshal(value, &payload); err != nil {
		return nil, err
	}
	// the 'payload' field has an array as a value to support
	// batched rows in the future
	batch := []json.RawMessage{payload}
	body := webhookSinkPayload{
		payload: batch,
	}
	j, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return j, err
}

type workerPayload struct {
	msg         string
	signalFlush bool
}

type webhookSinkResponse struct {
	url  sinkURL
	body string
	err  error
}

type webhookSink struct {
	ctx        context.Context
	url        sinkURL
	authHeader string
	client     *httputil.Client
	cancelFunc func()
	mu         struct {
		syncutil.Mutex
		inflight    int64
		workerChans []chan *workerPayload
		resolvedCh  chan *workerPayload
		flushCh     chan *webhookSinkResponse
		wg          *sync.WaitGroup
	}
}

func makeWebhookSink(ctx context.Context, u sinkURL, opts map[string]string) (Sink, error) {
	if u.Scheme != changefeedbase.SinkSchemeWebhookHTTPS {
		return nil, errors.Errorf(`this sink requires %s`, changefeedbase.SinkSchemeHTTPS)
	}
	u.Scheme = strings.TrimPrefix(u.Scheme, `webhook-`)

	ctx, cancel := context.WithCancel(ctx)

	sink := &webhookSink{
		ctx:        ctx,
		cancelFunc: cancel,
	}
	sink.mu.wg = new(sync.WaitGroup)

	switch changefeedbase.FormatType(opts[changefeedbase.OptFormat]) {
	case changefeedbase.OptFormatJSON:
	// only JSON supported at this time for webhook sink
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, opts[changefeedbase.OptFormat])
	}

	switch changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) {
	case changefeedbase.OptEnvelopeWrapped:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, opts[changefeedbase.OptEnvelope])
	}

	if _, ok := opts[changefeedbase.OptKeyInValue]; !ok {
		return nil, errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptKeyInValue)
	}

	if _, ok := opts[changefeedbase.OptTopicInValue]; !ok {
		return nil, errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptTopicInValue)
	}

	var connTimeout time.Duration
	sink.authHeader = opts[changefeedbase.OptWebhookAuthHeader]
	if timeout, ok := opts[changefeedbase.OptWebhookClientTimeout]; ok {
		var err error
		connTimeout, err = time.ParseDuration(timeout)
		if err != nil {
			return nil, err
		}
	} else {
		connTimeout = 3 * time.Second
	}

	var err error
	sink.client, err = makeWebhookClient(u, connTimeout)
	if err != nil {
		return nil, err
	}

	// remove known query params from sink URL before setting in sink config
	sinkURLParsed, err := url.Parse(u.String())
	if err != nil {
		return nil, err
	}
	params := sinkURLParsed.Query()
	params.Del(changefeedbase.SinkParamSkipTLSVerify)
	params.Del(changefeedbase.SinkParamCACert)
	sinkURLParsed.RawQuery = params.Encode()
	sink.url = sinkURL{URL: sinkURLParsed}

	sink.setupWorkers()
	sink.mu.flushCh = make(chan *webhookSinkResponse)

	return sink, nil
}

func makeWebhookClient(u sinkURL, timeout time.Duration) (*httputil.Client, error) {
	var client *httputil.Client

	client = &httputil.Client{
		Client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{Timeout: timeout}).DialContext,
			},
		},
	}

	dialConfig := struct {
		tlsSkipVerify bool
		caCert        []byte
		clientCert    []byte
		clientKey     []byte
	}{}

	transport := client.Transport.(*http.Transport)

	if _, err := u.consumeBool(changefeedbase.SinkParamSkipTLSVerify, &dialConfig.tlsSkipVerify); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamCACert, &dialConfig.caCert); err != nil {
		return nil, err
	}

	transport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: dialConfig.tlsSkipVerify,
	}

	if dialConfig.caCert != nil {
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "could not load system root CA pool")
		}
		if caCertPool == nil {
			caCertPool = x509.NewCertPool()
		}
		if !caCertPool.AppendCertsFromPEM(dialConfig.caCert) {
			return nil, errors.Errorf("failed to parse certificate data:%s", string(dialConfig.caCert))
		}
		transport.TLSClientConfig.RootCAs = caCertPool
	}

	return client, nil
}

func (s *webhookSink) setupWorkers() {
	s.mu.workerChans = make([]chan *workerPayload, workerPoolSize)
	for index := 0; index < workerPoolSize; index++ {
		s.mu.workerChans[index] = make(chan *workerPayload)
		s.mu.wg.Add(1)
		go s.workerLoop(s.mu.workerChans[index], s.mu.wg)
	}
	s.mu.resolvedCh = make(chan *workerPayload)
	s.mu.wg.Add(1)
	go s.workerLoop(s.mu.resolvedCh, s.mu.wg)
}

func (s *webhookSink) workerLoop(worker chan *workerPayload, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			close(worker)
			return
		case req := <-worker:
			message := s.sendMessage(s.ctx, req.msg)
			// don't expect flush for resolved timestamps
			if req.signalFlush {
				wg.Add(1)
				go func() {
					defer wg.Done()
					select {
					case <-s.ctx.Done():
					case s.mu.flushCh <- message:
					}
				}()
			}
		}
	}
}

// Dial is a no-op for this sink since we don't necessarily have
// a "health check" endpoint to use.
func (s *webhookSink) Dial() error {
	return nil
}

func (s *webhookSink) sendMessage(ctx context.Context, reqBody string) *webhookSinkResponse {
	resWrapper := &webhookSinkResponse{url: s.url}

	var req *http.Request
	req, resWrapper.err = http.NewRequestWithContext(ctx, http.MethodPost, s.url.String(), strings.NewReader(reqBody))
	if resWrapper.err != nil {
		return resWrapper
	}
	req.Header.Set("Content-Type", applicationTypeJSON)
	if s.authHeader != "" {
		req.Header.Set(authorizationHeader, s.authHeader)
	}

	var res *http.Response
	res, resWrapper.err = s.client.Do(req)
	if resWrapper.err != nil {
		log.Errorf(ctx, "%v", resWrapper.err)
		return resWrapper
	}
	defer res.Body.Close()

	var resBody []byte
	resBody, resWrapper.err = ioutil.ReadAll(res.Body)
	if resWrapper.err != nil {
		return resWrapper
	}

	if !(res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices) {
		resWrapper.err = fmt.Errorf("%s: %s", res.Status, string(resBody))
	} else {
		resWrapper.body = string(resBody)
	}

	return resWrapper
}

func workerIndex(key []byte) uint32 {
	hash := crc32.ChecksumIEEE(key)
	return hash % workerPoolSize
}

func (s *webhookSink) EmitRow(
	ctx context.Context, topic TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	j, err := encodePayloadWebhook(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.mu.inflight++
	s.mu.Unlock()

	index := workerIndex(key)
	payload := &workerPayload{
		msg:         string(j),
		signalFlush: true,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.mu.workerChans[index] <- payload:
	}
	return nil
}

func (s *webhookSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	j, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}

	payload := &workerPayload{
		msg:         string(j),
		signalFlush: false,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.mu.resolvedCh <- payload:
	}
	return nil
}

func (s *webhookSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var flushErr error
	for s.mu.inflight > 0 {
		log.Infof(ctx, "flush waiting for %d inflight messages", s.mu.inflight)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-s.mu.flushCh:
			if flushErr == nil {
				if res.err != nil {
					log.Errorf(ctx, "%v", errors.Wrapf(res.err, "encountered error while flushing webhook sink"))
					flushErr = res.err
				}
				log.Infof(ctx, "dialed webhook sink: %s and received response: %s", res.url, res.body)
			}
			s.mu.inflight--
		}
	}
	return flushErr
}

func (s *webhookSink) Close() error {
	// Flush to make sure we're not sending to closed channels
	err := s.Flush(s.ctx)
	if err != nil {
		return err
	}
	s.client.CloseIdleConnections()
	s.cancelFunc()
	s.mu.wg.Wait()
	close(s.mu.flushCh)
	return nil
}

// redactWebhookAuthHeader redacts sensitive information from `auth`, which should
// be the value of the HTTP header `Authorization:`. The entire header will be
// redacted here.
func redactWebhookAuthHeader(auth string) string {
	return "redacted"
}

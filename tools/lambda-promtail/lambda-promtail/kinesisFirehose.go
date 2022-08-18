package main

import (
	"context"
	"time"
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/prometheus/common/model"
)

type KinesisFirehoseHttpEndpointRequest struct {
	RequestID string `json:"requestId"`
	Timestamp int64  `json:"timestamp"`
	Records   []struct {
		Data string `json:"data"`
	} `json:"records"`
}

type KinesisFirehoseHttpEndpointResponse struct {
	RequestID string `json:"requestId"`
	Timestamp int64  `json:"timestamp"`
	ErrorMessage string `json:"errorMessage"`
}

func parseKinesisFirehoseHttpEndpointRequest(ctx context.Context, b *batch, ev *events.APIGatewayV2HTTPRequest) (*string,error) {

	body := KinesisFirehoseHttpEndpointRequest{}
    json.Unmarshal([]byte(ev.Body), &body)


	timestamp := time.UnixMilli(body.Timestamp)

	for _, record := range body.Records {
		data, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			return nil,err
		}

		labels := model.LabelSet{
			model.LabelName("__aws_log_type"):                 model.LabelValue("kinesis_delivery_stream"),
			model.LabelName("__aws_kinesis_firehose_source_arn"): model.LabelValue(ev.Headers["x-amz-firehose-source-arn"]),
			model.LabelName("__aws_kinesis_firehose_request_id"): model.LabelValue(body.RequestID),
		}

		labels = applyExtraLabels(labels)

		b.add(ctx, entry{labels, logproto.Entry{
			Line:      string(data),
			Timestamp: timestamp,
		}})
	}

	kinesisFirehoseHttpEndpointResponse, err := json.Marshal(KinesisFirehoseHttpEndpointResponse{
		body.RequestID,body.Timestamp,"",
	})
	if err != nil {
		return nil, err
	}

	kinesisFirehoseHttpEndpointResponseString := string(kinesisFirehoseHttpEndpointResponse)
	return &kinesisFirehoseHttpEndpointResponseString,nil
}

func processKinesisFirehoseHttpEndpointRequest(ctx context.Context, ev *events.APIGatewayV2HTTPRequest) (*string, error) {
	batch, _ := newBatch(ctx)
	body,err := parseKinesisFirehoseHttpEndpointRequest(ctx, batch, ev)
	if err != nil {
		return nil,err
	}

	err = sendToPromtail(ctx, batch)
	if err != nil {
		return nil,err
	}


	return body, nil
}

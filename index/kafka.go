/*
 * Copyright 2022 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package index

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaIndexOption func(options *kafka.ReaderConfig)

func WithBrokers(brokers []string) KafkaIndexOption {
	return func(options *kafka.ReaderConfig) {
		options.Brokers = brokers
	}
}

func WithGroupID(groupID string) KafkaIndexOption {
	return func(options *kafka.ReaderConfig) {
		options.GroupID = groupID
	}
}

func WithTopic(topic string) KafkaIndexOption {
	return func(options *kafka.ReaderConfig) {
		options.Topic = topic
	}
}

func WithMinBytes(minBytes int) KafkaIndexOption {
	return func(options *kafka.ReaderConfig) {
		options.MinBytes = minBytes
	}
}

func WithMaxBytes(maxBytes int) KafkaIndexOption {
	return func(options *kafka.ReaderConfig) {
		options.MaxBytes = maxBytes
	}
}

func WithMaxWait(maxWait time.Duration) KafkaIndexOption {
	return func(options *kafka.ReaderConfig) {
		options.MaxWait = maxWait
	}
}

type KafkaIndexer struct {
	kafka.ReaderConfig
	queue Queue
}

func (k KafkaIndexer) Run(ctx context.Context) (err error) {
	defer k.queue.Close()
	defer func() {
		r := recover()
		switch v := r.(type) {
		case error:
			err = v
		}
	}()

	r := kafka.NewReader(k.ReaderConfig)
	defer r.Close()

	for {
		msg, err := r.ReadMessage(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		k.queue.Add(string(msg.Value))
	}
}

func NewKafkaIndexer(queue Queue, options ...KafkaIndexOption) KafkaIndexer {
	readerConfig := new(kafka.ReaderConfig)
	for _, apply := range options {
		apply(readerConfig)
	}
	return KafkaIndexer{
		ReaderConfig: *readerConfig,
		queue:        queue,
	}
}

package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	q, err := ch.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	if err := ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, q, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %w", err)
	}

	deliveries, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("couldn't deliver message: %w", err)
	}
	go func() {
		for delivery := range deliveries {
			var data T
			err := json.Unmarshal(delivery.Body, &data)
			if err != nil {
				fmt.Printf("couln't get the data: %v", err)
				continue
			}
			handler(data)
			if err := delivery.Ack(false); err != nil {
				fmt.Printf("couldn't remove message: %v", err)
			}
		}
	}()

	return nil
}

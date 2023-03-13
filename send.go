package krpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageConstraint is a generic interface that all messages must implement. This
// is used to dynamically create a queue name based on the message type.
type MessageConstraint interface {
	Channel() string
}

// KnownQueues is a map of all known queues. This is used to cache queues and
// improve performance.
var KnownQueues = map[string]amqp.Queue{}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

// Send is a generic function that sends a message to a queue. If a callback is
// provided, the response will be passed to the callback function.
func Send[T MessageConstraint](message T, callback ...func(response interface{}, err error)) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		return err
	}

	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Println("Error closing connection: ", err)
		}
	}(conn)

	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			log.Println("Error closing channel: ", err)
		}
	}(ch)

	var queue amqp.Queue
	ReplyTo := ""

	if q, ok := KnownQueues[message.Channel()]; ok {
		queue = q
		ReplyTo = q.Name
	} else {
		queue, err = ch.QueueDeclare(
			message.Channel(), // name
			false,             // durable
			false,             // delete when unused
			false,             // exclusive
			false,             // no-wait
			nil,               // arguments
		)

		if err != nil {
			return err
		}

		KnownQueues[message.Channel()] = queue
	}

	var replyQueue *amqp.Queue
	if len(callback) > 0 {
		q, err := ch.QueueDeclare(
			fmt.Sprintf("%s-%s", message.Channel(), randomString(30)), // name
			false, // durable
			true,  // delete when unused
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)

		if err != nil {
			return err
		}

		replyQueue = &q

		ReplyTo = replyQueue.Name
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Marshal message to JSON
	marshalled, err := json.Marshal(message)

	if err != nil {
		return err
	}

	var msgs <-chan amqp.Delivery

	if len(callback) > 0 {
		msgs, err = ch.Consume(
			ReplyTo, // queue
			"",      // consumer
			true,    // auto-ack
			true,    // exclusive
			false,   // no-local
			false,   // no-wait
			nil,     // args
		)

		if err != nil {
			return err
		}
	}

	err = ch.PublishWithContext(ctx,
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        marshalled,
			ReplyTo:     ReplyTo,
		})

	if err != nil {
		return err
	}

	if len(callback) > 0 {
		for d := range msgs {
			// Unmarshal message from JSON
			var message interface{}
			err = json.Unmarshal(d.Body, &message)

			if err != nil {
				callback[0](message, err)
			}

			callback[0](message, nil)
		}
	}

	return nil
}

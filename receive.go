package krpc

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// Receive is a generic function that receives messages from a queue. Each message
// is passed to the callback function, which can return a response. If no response
// is needed, you can use "error" as the return type. Then no response will be
// sent and only an error will be printed.
func Receive[T MessageConstraint, R interface{}](callback func(message T) R) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		panic(err)
	}

	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Println("Error closing connection: ", err)
		}
	}(conn)

	ch, err := conn.Channel()

	if err != nil {
		panic(err)
	}

	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			log.Println("Error closing channel: ", err)
		}
	}(ch)

	// Dynamically create a queue name based on the message type
	channelName := (*new(T)).Channel()

	q, err := ch.QueueDeclare(
		channelName, // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	var forever chan struct{}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for d := range msgs {
			// Unmarshal message from JSON
			var message T
			err := json.Unmarshal(d.Body, &message)

			if err != nil {
				log.Println("Error unmarshalling message: ", err)

				err := d.Reject(false)
				if err != nil {
					log.Println("Error rejecting message: ", err)
				}

				continue
			}

			response := callback(message)

			if d.ReplyTo != "" {
				marshalledResponse, err := json.Marshal(response)

				if err != nil {
					log.Println("Error marshalling response: ", err)
				}

				err = ch.PublishWithContext(ctx,
					"",        // exchange
					d.ReplyTo, // routing key
					false,     // mandatory
					false,     // immediate
					amqp.Publishing{
						Body: marshalledResponse,
					})

				if err != nil {
					log.Println("Error replying to message: ", err)
				}
			}
		}
	}()

	log.Printf(" [*] Listening for messages on channel: %s", channelName)
	<-forever
}

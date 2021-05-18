package main

import (
	"bytes"
	"fmt"
	"github.com/disintegration/imaging"
	"github.com/streadway/amqp"
	"log"
	"os"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	args := os.Args
	if len(args) < 3 {
		log.Fatalln("Usage: server user password")
	}

	user := args[1]
	password := args[2]

	url := fmt.Sprintf("amqp://%s:%s@localhost:5672/image-resizer", user, password)
	fmt.Printf("URL: %s\n", url)

	fmt.Println("Connecting to RabbitMQ broker...")
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	fmt.Println("Connected to RabbitMQ broker")
	defer conn.Close()

	fmt.Println("Opening channel...")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	fmt.Println("Opened channel")
	defer ch.Close()

	fmt.Println("Declaring queue...")
	q, err := ch.QueueDeclare(
		"image-resizer", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")
	fmt.Println("Declared queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	fmt.Println("Registering consumer...")
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	fmt.Println("Registered consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			r := bytes.NewReader(d.Body)
			src, err := imaging.Decode(r)
			failOnError(err, "Failed to decode image")

			// Resize the to width = 200px preserving the aspect ratio.
			resized := imaging.Resize(src, 200, 0, imaging.Lanczos)

			b := bytes.NewBuffer([]byte{})
			imaging.Encode(b, resized, imaging.JPEG)

			response := b.Bytes()

			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          response,
				})
			failOnError(err, "Failed to publish a message")

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting RPC requests")
	<-forever
}

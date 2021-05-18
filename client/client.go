package main

import (
	"bytes"
	"fmt"
	"github.com/disintegration/imaging"
	"github.com/streadway/amqp"
	"image"
	"log"
	"math/rand"
	"os"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

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

func imageResizeRPC(img image.Image) (err error) {
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

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	fmt.Println("Declaring queue...")
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	fmt.Println("Declared queue")

	corrId := randomString(32)

	 b := bytes.NewBuffer([]byte{})
	 imaging.Encode(b, img, imaging.JPEG)

	err = ch.Publish(
		"",          // exchange
		"image-resizer", // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          b.Bytes(),
		})
	failOnError(err, "Failed to publish a message")

	for d := range msgs {
		if corrId == d.CorrelationId {
			r := bytes.NewReader(d.Body)
			resized, err := imaging.Decode(r)
			failOnError(err, "Failed to decode image")

			// Save the resulting image as JPEG.
			err = imaging.Save(resized, "resized.jpg")
			if err != nil {
				log.Fatalf("failed to save image: %v", err)
			}
			fmt.Println("Saved resized image")
			break
		}
	}

	return
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	// Open a test image.
	src, err := imaging.Open("pasqueflower.jpg")
	if err != nil {
		log.Fatalf("failed to open image: %v", err)
	}

	fmt.Println("Requesting image resize...")
	err = imageResizeRPC(src)
	failOnError(err, "Failed to handle RPC request")
}
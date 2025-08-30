package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	// Ограничиваем количество одновременных соединений
	listenerCounter := make(chan struct{}, 1000)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		listenerCounter <- struct{}{}

		go func(conn net.Conn) {
			defer conn.Close()
			defer func() {
				<-listenerCounter
			}()

			for {
				err = parseContent(conn)
				if err != nil {
					conn.Write([]byte(err.Error()))
					if err == io.EOF {
						// os.Exit(1)
					}
				}
			}
		}(conn)
	}

	// time.Sleep(60 * time.Second)
}

func readLine(reader *bufio.Reader) (string, error) {
	content, isPrefix, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	if isPrefix {
		// прочитана не вся строка
		return "", fmt.Errorf("wrong string format")
	}

	return string(content), nil
}

func parseContent(conn net.Conn) error {
	reader := bufio.NewReader(conn)

	firstByte, err := reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return err
		}
		return err
	}
	if firstByte != '*' {
		// это не массив
		return fmt.Errorf("waiting array")
	}

	// проверяем длину переданную
	count, err := readLine(reader)
	if err != nil {
		return err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return err
	}
	if countInt < 1 {
		// не та длина
		return fmt.Errorf("wrong length")
	}

	// пропускаем строку
	_, err = readLine(reader)
	if err != nil {
		return err
	}

	command, err := readLine(reader)
	if err != nil {
		return err
	}
	stringCommand := string(command)
	switch stringCommand {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		_, err = readLine(reader)
		if err != nil {
			return err
		}
		text, err := readLine(reader)
		if err != nil {
			return err
		}
		fmt.Println(len(text))
		// fmt.Println(string(text))
		// if len(words) < 3 {
		// 	// нечего выводить
		// 	return fmt.Errorf("need words")
		// }
		// message := "$2hhhhhh"
		message := "$" + strconv.Itoa(len(text)) + "\r\n" + string(text) + "\r\n"
		// conn.Write([]byte(message))
		conn.Write([]byte(message))
	default:
		conn.Write([]byte("unknown command"))
	}
	return nil
}

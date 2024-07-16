package main

import (
	"fmt"
	"io"
	"myRedis/aof"
	"myRedis/handler"
	"myRedis/resp"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Entry Point of Server")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Error in listner:", err)
	}

	aof, err := aof.NewAOF("database.aof")
	if err != nil {
		fmt.Println("Error in AOF setup:", err)
		os.Exit(1)
	}
	defer aof.Close()
	aof.Read(func(val resp.Value) {
		command := strings.ToUpper(val.Array[0].Bulk)

		args := val.Array[1:]

		handler, ok := handler.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}
		handler(args)
	})

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error in Accepting connection:", err)
	}

	defer conn.Close()

	for {
		respI := resp.NewResp(conn)
		value, err := respI.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error in Reading command:", err)
			os.Exit(1)
		}

		if value.Typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.Array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)

		writer := resp.NewWriter(conn)

		args := value.Array[1:]

		handler, ok := handler.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(resp.Value{Typ: "string", Str: ""})
			continue
		}
		result := handler(args)

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		writer.Write(result)
	}
}

package resp

import (
	"fmt"
	"io"
	"strconv"
)

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(val Value) error {
	bytes := val.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		fmt.Println("Error in Writing Bytes:", err)
		return err
	}

	return nil
}

func (v Value) Marshal() []byte {
	switch v.Typ {
	case "string":
		{
			return v.marshalString()
		}
	case "bulk":
		{
			return v.marshalBulk()
		}
	case "array":
		{
			return v.marshalArray()
		}
	case "null":
		return v.marshalNull()
	case "integer":
		return v.marshalInteger()
	default:
		{
			fmt.Println("Error: Unknown Type")
			return nil
		}
	}
}

func (v Value) marshalString() []byte {
	var bytes []byte

	bytes = append(bytes, STRING)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte

	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n')

	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalArray() []byte {
	var bytes []byte
	arrayLength := len(v.Array)

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(arrayLength)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < arrayLength; i++ {
		element := v.Array[i].Marshal()
		bytes = append(bytes, element...)
	}

	return bytes
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

func (v Value) marshalInteger() []byte {
	var bytes []byte

	bytes = append(bytes, INTEGER)
	bytes = append(bytes, byte(v.Int))
	bytes = append(bytes, '\r', '\n')

	return bytes
}

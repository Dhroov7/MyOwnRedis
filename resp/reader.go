package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type Resp struct {
	reader *bufio.Reader
}

type Value struct {
	Typ   string
	Array []Value
	Bulk  string
	Str   string
	Int   int
}

const (
	ARRAY   = '*'
	BULK    = '$'
	STRING  = '+'
	INTEGER = ':'
)

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) Read() (Value, error) {
	typ, err := r.reader.ReadByte()
	if err != nil {
		fmt.Println("Invalid type, expecting string only")
		return Value{}, err
	}

	switch typ {
	case ARRAY:
		{
			return r.readArray()
		}
	case BULK:
		{
			return r.readBulk()
		}
	default:
		fmt.Printf("Unknown type: %v", string(typ))
		return Value{}, nil
	}

}

func (r *Resp) readBulk() (Value, error) {
	val := Value{}
	val.Typ = "bulk"

	len, err := r.readInteger()
	if err != nil {
		fmt.Println("Error in reading Integer:", err)
		return Value{}, err
	}

	buff := make([]byte, len)
	r.reader.Read(buff)
	val.Bulk = string(buff)

	//Read the trailing EOF command
	r.readLine()

	return val, nil
}

func (r *Resp) readInteger() (int, error) {
	len, err := r.readLine()

	if err != nil {
		fmt.Println("Error in reading Integer:", err)
		return 0, err
	}

	len64, err := strconv.ParseInt(string(len), 10, 64)

	if err != nil {
		fmt.Println("Error in Parsing Integer Byte:", err)
		return 0, err
	}

	return int(len64), nil
}

func (r *Resp) readLine() (line []byte, err error) {
	for {
		byteVal, err := r.reader.ReadByte()
		if err != nil {
			return nil, err
		}

		line = append(line, byteVal)

		//It means we have reached to the end of line
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	// We only want to return the actual value and not the EOF command
	return line[:len(line)-2], nil
}

func (r *Resp) readArray() (Value, error) {
	val := Value{}
	val.Typ = "array"

	len, err := r.readInteger()

	if err != nil {
		fmt.Println("Error in reading Integer:", err)
		return Value{}, err
	}

	val.Array = make([]Value, 0)

	for i := 0; i < len; i++ {
		newValue, err := r.Read()

		if err != nil {
			fmt.Println("Error in reading Bytes:", err)
			return Value{}, err
		}

		val.Array = append(val.Array, newValue)
	}

	return val, nil
}

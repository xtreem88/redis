package parser

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

func ParseArray(reader *bufio.Reader) ([]string, int, error) {
	s, err := reader.ReadString('\n')
	cmdSize := len(s) + 1
	if err != nil {
		return nil, 0, err
	}
	arrLength, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return nil, 0, err
	}
	arr := make([]string, 0, arrLength)
	for range arrLength {
		element, l, err := parseArrayElement(reader)
		cmdSize += l
		if err != nil {
			return nil, 0, err
		}
		arr = append(arr, element)
	}
	return arr, cmdSize, nil
}
func parseArrayElement(reader *bufio.Reader) (string, int, error) {
	_, err := reader.ReadByte()
	if err != nil {
		return "", 0, err
	}
	s, err := reader.ReadString('\n')
	if err != nil {
		return "", 0, err
	}
	cmdSize := len(s) + 1
	length, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return "", 0, err
	}
	element := make([]byte, length)
	l, err := io.ReadFull(reader, element)
	cmdSize += l
	if err != nil {
		return "", 0, err
	}
	l, err = reader.Discard(2)
	cmdSize += l
	if err != nil {
		return "", 0, err
	}
	return string(element), cmdSize, nil
}

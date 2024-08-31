package parser

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

func ParseArray(reader *bufio.Reader) ([]string, error) {
	s, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	arrLength, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return nil, err
	}
	arr := make([]string, 0, arrLength)
	for range arrLength {
		element, err := parseArrayElement(reader)
		if err != nil {
			return nil, err
		}
		arr = append(arr, element)
	}
	return arr, nil
}
func parseArrayElement(reader *bufio.Reader) (string, error) {
	_, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	s, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	length, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return "", err
	}
	element := make([]byte, length)
	_, err = io.ReadFull(reader, element)
	if err != nil {
		return "", err
	}
	_, err = reader.Discard(2)
	if err != nil {
		return "", err
	}
	return string(element), nil
}

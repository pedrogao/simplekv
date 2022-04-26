package simplekv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type LineReader struct {
	file  *os.File
	inner *bufio.Reader
}

func NewLineReader(path string, offset int64) (*LineReader, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("open file err: %s", err)
	}
	if offset > 0 {
		_, err = file.Seek(offset, 0)
		if err != nil {
			return nil, fmt.Errorf("seek segment file err: %s", err)
		}
	}
	return &LineReader{
		file:  file,
		inner: bufio.NewReader(file),
	}, nil
}

func (r *LineReader) ReadLine() (string, error) {
	line, err := r.inner.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", err
		}
		return "", fmt.Errorf("read line err: %s", err)
	}
	line = strings.TrimSuffix(line, "\n")
	return line, nil
}

func (r *LineReader) ReadLineKV() (string, string, error) {
	line, err := r.ReadLine()
	if err != nil {
		return "", "", err
	}
	parts := strings.Split(line, ",")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("kv line not valid: %s", line)
	}
	return parts[0], parts[1], nil
}

func (r *LineReader) Close() error {
	return r.file.Close()
}

func readFileLines(path string) (lines []string) {
	f, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		lines = append(lines, line)
	}
	return
}

func exists(path string) bool {
	if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

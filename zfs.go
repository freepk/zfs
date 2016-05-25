package zfs

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os/exec"
	"strings"
)

func zfs(stdout io.Writer, stdin io.Reader, args ...string) error {
	var stderr bytes.Buffer
	cmd := exec.Command("zfs", args...)
	if stdout != nil {
		cmd.Stdout = stdout
	}
	if stdin != nil {
		cmd.Stdin = stdin
	}
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return errors.New(stderr.String())
	}
	return nil
}

func Create(name string) error {
	return zfs(nil, nil, "create", name)
}

func Destroy(name string, recursively, force bool) error {
	args := []string{"destroy"}
	if recursively {
		args = append(args, "-r")
	}
	if force {
		args = append(args, "-f")
	}
	args = append(args, name)
	return zfs(nil, nil, args...)
}

func Snapshot(name string) error {
	return zfs(nil, nil, "snapshot", name)
}

func Send(name string, w io.Writer) error {
	return zfs(w, nil, "send", name)
}

func SendDelta(name0, name1 string, intermediary bool, w io.Writer) error {
	args := []string{"send"}
	if intermediary {
		args = append(args, "-I")
	} else {
		args = append(args, "-i")
	}
	args = append(args, name0, name1)
	return zfs(w, nil, args...)
}

func recvSnapName(r io.Reader) string {
	s := bufio.NewScanner(r)
	n := ""
	for s.Scan() {
		l := s.Text()
		w := strings.Split(l, "@")
		p := len(w) - 1
		n = w[p]
		s.Scan()
	}
	return n
}

func Recv(name string, force bool, r io.Reader) (string, error) {
	buf := &bytes.Buffer{}
	args := []string{"recv"}
	if force {
		args = append(args, "-F")
	}
	args = append(args, "-v")
	args = append(args, name)
	err := zfs(buf, r, args...)
	if err != nil {
		return "", err
	}
	return recvSnapName(buf), nil
}

func Rollback(name string, recent bool) error {
	args := []string{"rollback"}
	if recent {
		args = append(args, "-r")
	}
	args = append(args, name)
	return zfs(nil, nil, args...)
}

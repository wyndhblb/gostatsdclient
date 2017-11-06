package gostatsdclient

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func newLocalListenerUDP(t *testing.T) (*net.UDPConn, *net.UDPAddr) {
	udpAddr, err := net.ResolveUDPAddr("udp", ":62198")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	return ln, udpAddr
}

func TestCount(t *testing.T) {
	ln, udpAddr := newLocalListenerUDP(t)
	defer ln.Close()

	prefix := "myproject."

	client := NewStatsdClient(udpAddr.String(), prefix, "none")

	ch := make(chan string, 0)

	s := map[string]int64{
		"a:b:c": 5,
		"d:e:f": 2,
		"x:b:c": 5,
		"g.h.i": 1,
	}

	expected := make(map[string]int64)
	for k, v := range s {
		expected[k] = v
	}

	// also test %HOST% replacement
	s["zz.%HOST%"] = 1
	hostname, err := os.Hostname()
	expected["zz."+hostname] = 1

	go doListenUDP(ln, ch, len(s))

	err = client.CreateSocket()
	if nil != err {
		t.Fatal(err)
	}
	defer client.Close()

	for k, v := range s {
		client.Total(k, v)
	}

	actual := make(map[string]int64)

	re := regexp.MustCompile(`^(.*)\:(\d+)\|(\w).*$`)

	for i := len(s); i > 0; i-- {
		x := <-ch
		x = strings.TrimSpace(x)
		//fmt.Println(x)
		if !strings.HasPrefix(x, prefix) {
			t.Errorf("Metric without expected prefix: expected '%s', actual '%s'", prefix, x)
		}
		vv := re.FindStringSubmatch(x)
		if vv[3] != "c" {
			t.Errorf("Metric without expected suffix: expected 'c', actual '%s' (string: %s)", vv[3], x)
		}
		v, err := strconv.ParseInt(vv[2], 10, 64)
		if err != nil {
			t.Error(err)
		}
		actual[vv[1][len(prefix):]] = v
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("did not receive all metrics: Expected: %T %v, Actual: %T %v ", expected, expected, actual, actual)
	}
}


func TestBufferedCount(t *testing.T) {
	ln, udpAddr := newLocalListenerUDP(t)
	defer ln.Close()

	prefix := "myproject."

	client := NewStatsdClientBuffered(udpAddr.String(), prefix, "none", 20)

	ch := make(chan string, 0)

	s := map[string]int64{
		"a:b:c": 5,
		"d:e:f": 2,
		"x:b:c": 5,
		"g.h.i": 1,
		"g.h.j": 1,
		"g.h.k": 1,
		"g.h.l": 1,
		"g.h.m": 1,
		"g.h.n": 1,
		"g.h.o": 1,
	}

	expected := make(map[string]int64)
	for k, v := range s {
		expected[k] = v
	}

	// also test %HOST% replacement
	s["zz.%HOST%"] = 1
	hostname, err := os.Hostname()
	expected["zz."+hostname] = 1

	go doListenUDP(ln, ch, len(s))

	err = client.CreateSocket()
	if nil != err {
		t.Fatal(err)
	}

	defer client.Close()

	for k, v := range s {
		client.Incr(k, v)
	}

	// pause for flush
	//time.Sleep(time.Second)

	actual := make(map[string]int64)

	re := regexp.MustCompile(`^(.*)\:(\d+)\|(\w).*$`)

	strs := make([]string, 0)

	for i := len(s); i > 0; i-- {
		x := <-ch
		x = strings.TrimSpace(x)
		strs = append(strs, x)
	}
	if len(strs) != len(expected){
		t.Fatalf("Did not get all the metrics: got %d wanted %d", len(strs), len(expected))

	}
	for _, x := range strs{
		//t.Log(x)
		if !strings.HasPrefix(x, prefix) {
			t.Errorf("Metric without expected prefix: expected '%s', actual '%s'", prefix, x)
		}
		vv := re.FindStringSubmatch(x)
		if vv[3] != "c" {
			t.Errorf("Metric without expected suffix: expected 'c', actual '%s' (string: %s)", vv[3], x)
		}
		v, err := strconv.ParseInt(vv[2], 10, 64)
		if err != nil {
			t.Error(err)
		}
		actual[vv[1][len(prefix):]] = v
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("did not receive all metrics: Expected: %T %v, Actual: %T %v ", expected, expected, actual, actual)
	}

}

func doListenUDP(conn *net.UDPConn, ch chan string, n int) {
	for n > 0 {
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c *net.UDPConn, ch chan string) {
			buffer := make([]byte, 1024)
			size, err := c.Read(buffer)
			buffer = buffer[:size]
			// size, address, err := sock.ReadFrom(buffer) <- This starts printing empty and nil values below immediatly
			if err != nil {
				fmt.Println(string(buffer), size, err)
				panic(err)
			}
			for _, s := range strings.Split(strings.TrimSpace(string(buffer)), "\n") {
				if len(s) == 0{
					continue
				}
				ch <- s
			}
		}(conn, ch)
		n--
	}
}

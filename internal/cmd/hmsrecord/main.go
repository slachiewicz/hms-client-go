// Command hmsrecord is a TCP proxy that sits between metastore clients and
// a real Hive Metastore and records the name of every Thrift call that
// passes through it. It exists to learn which RPCs an engine (Spark,
// Trino, Flink) actually issues, so a lightweight metastore knows which
// ones it must implement.
//
// Bytes are forwarded unchanged in both directions. Calls are parsed from
// the client-to-server stream as unframed TBinaryProtocol, the metastore's
// default; a connection that turns out to be anything else (SASL, framed,
// HTTP) is still proxied, but its calls are not recorded, and a line saying
// so is written instead.
//
//	hmsrecord -listen 0.0.0.0:9083 -upstream 127.0.0.1:9084 -out calls.jsonl
//
// Each recorded call is one JSON line. On SIGINT or SIGTERM the per-method
// counts are printed to stdout, most frequent first.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// call is one recorded RPC.
type call struct {
	Time   time.Time `json:"time"`
	Client string    `json:"client"`
	Method string    `json:"method"`
	Type   string    `json:"type"`
	SeqID  int32     `json:"seq_id"`
	// Note is set instead of Method when a connection could not be parsed
	// as TBinaryProtocol.
	Note string `json:"note,omitempty"`
}

// recorder collects calls and appends each to out as a JSON line.
type recorder struct {
	mu     sync.Mutex
	enc    *json.Encoder
	counts map[string]int
}

func newRecorder(out io.Writer) *recorder {
	return &recorder{enc: json.NewEncoder(out), counts: map[string]int{}}
}

func (r *recorder) record(c call) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.Method != "" {
		r.counts[c.Method]++
	}
	if err := r.enc.Encode(c); err != nil {
		log.Printf("hmsrecord: write: %v", err)
	}
}

// summary returns "count method" lines, most frequent first, then by name.
func (r *recorder) summary() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.counts))
	for n := range r.counts {
		names = append(names, n)
	}
	slices.SortFunc(names, func(a, b string) int {
		if d := r.counts[b] - r.counts[a]; d != 0 {
			return d
		}
		if a < b {
			return -1
		}
		return 1
	})
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = fmt.Sprintf("%6d %s", r.counts[n], n)
	}
	return out
}

// serve accepts connections on ln and proxies each to upstream until ln is
// closed.
func serve(ln net.Listener, upstream string, rec *recorder) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go proxy(c, upstream, rec)
	}
}

// proxy forwards c to upstream in both directions, recording the calls c
// sends.
func proxy(c net.Conn, upstream string, rec *recorder) {
	defer func() { _ = c.Close() }()
	up, err := net.Dial("tcp", upstream)
	if err != nil {
		log.Printf("hmsrecord: dial %s: %v", upstream, err)
		return
	}
	defer func() { _ = up.Close() }()

	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(c, up)
		// The server hung up: unblock the client-side read below.
		_ = c.Close()
		close(done)
	}()
	record(c, up, c.RemoteAddr().String(), rec)
	// The client hung up: tell the server, then wait for its side to drain.
	if tc, ok := up.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
	<-done
}

// record copies client to server, parsing each message on the way. Every
// byte is written to server as it is read, before it is parsed, so parsing
// never holds data back. When parsing fails the rest is copied verbatim.
func record(client io.Reader, server io.Writer, addr string, rec *recorder) {
	in := &countingReader{r: io.TeeReader(client, server)}
	// Strict read: Java clients always send the versioned header, and
	// requiring it rejects a non-Thrift stream on its first four bytes.
	cfg := &thrift.TConfiguration{MaxMessageSize: math.MaxInt32, TBinaryStrictRead: new(true)}
	proto := thrift.NewTBinaryProtocolConf(thrift.NewStreamTransportR(in), cfg)
	ctx := context.Background()
	for {
		in.n = 0
		name, typ, seq, err := proto.ReadMessageBegin(ctx)
		if err == nil {
			err = proto.Skip(ctx, thrift.STRUCT)
		}
		if err == nil {
			err = proto.ReadMessageEnd(ctx)
		}
		if err != nil {
			// EOF before the first byte of a message is the client
			// hanging up; anywhere else the stream is not what we parse.
			if in.n > 0 || !isEOF(err) {
				rec.record(call{Time: time.Now(), Client: addr, Note: "not TBinaryProtocol, proxying unrecorded: " + err.Error()})
				_, _ = io.Copy(server, client)
			}
			return
		}
		rec.record(call{Time: time.Now(), Client: addr, Method: name, Type: messageType(typ), SeqID: seq})
	}
}

// countingReader counts the bytes read through it since n was last reset.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// isEOF reports whether err means the client closed the connection
// between messages.
func isEOF(err error) bool {
	var te thrift.TTransportException
	if errors.As(err, &te) && te.TypeId() == thrift.END_OF_FILE {
		return true
	}
	return errors.Is(err, io.EOF)
}

func messageType(t thrift.TMessageType) string {
	switch t {
	case thrift.CALL:
		return "call"
	case thrift.ONEWAY:
		return "oneway"
	case thrift.REPLY:
		return "reply"
	case thrift.EXCEPTION:
		return "exception"
	default:
		return fmt.Sprintf("type-%d", t)
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:9083", "address to accept metastore clients on")
	upstream := flag.String("upstream", "127.0.0.1:9084", "address of the real metastore")
	outPath := flag.String("out", "calls.jsonl", "file to append recorded calls to, one JSON object per line")
	flag.Parse()

	f, err := os.OpenFile(*outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		log.Fatalf("hmsrecord: %v", err)
	}
	defer func() { _ = f.Close() }()
	rec := newRecorder(f)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("hmsrecord: %v", err)
	}
	log.Printf("hmsrecord: %s -> %s, recording to %s", ln.Addr(), *upstream, *outPath)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		_ = ln.Close()
	}()
	_ = serve(ln, *upstream, rec)
	for _, line := range rec.summary() {
		fmt.Println(line)
	}
}

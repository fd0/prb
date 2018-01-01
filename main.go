package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var (
	workers           int
	reportingInterval time.Duration
	outputFile        string
)

func init() {
	flag.IntVar(&workers, "workers", 2, "set number of workers reading files concurrently")
	flag.DurationVar(&reportingInterval, "interval", 10*time.Second, "set reporting interval")
	flag.StringVar(&outputFile, "output", "benchmarks.csv", "set output file")
	flag.Parse()
}

// Stats collect statistics about what has been seen.
type Stats struct {
	files, dirs int
	bytes       int64
}

// Add adds all the stats from other.
func (s *Stats) Add(other Stats) {
	s.files += other.files
	s.dirs += other.dirs
	s.bytes += other.bytes
}

func readFile(wg *sync.WaitGroup, ch chan string, stats chan<- Stats) {
	defer wg.Done()
	last := time.Now()
	for filename := range ch {
		if time.Since(last) > reportingInterval {
			fmt.Fprintf(os.Stderr, "read %v\n", filename)
			last = time.Now()
		}

		f, err := os.Open(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to read %v: %v\n", filename, err)
			continue
		}

		n, err := io.Copy(ioutil.Discard, f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error reading %v: %v\n", filename, err)
			_ = f.Close()
			continue
		}

		err = f.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error closing %v: %v\n", filename, err)
			continue
		}

		stats <- Stats{
			files: 1,
			bytes: n,
		}
	}
}

func walk(dir string, ch chan string, stats chan<- Stats) {
	defer close(ch)
	dirs := 0
	err := filepath.Walk(dir, func(item string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fi.Mode().IsRegular() {
			ch <- item
		}

		if fi.IsDir() {
			dirs++
		}

		return nil
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "error walking %v: %v\n", dir, err)
	}

	stats <- Stats{dirs: dirs}
}

func traverse(workers int, dir string) (stats Stats) {
	var wg sync.WaitGroup
	var ch = make(chan string, 100)
	var statsCh = make(chan Stats, 100)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go readFile(&wg, ch, statsCh)
	}

	go walk(dir, ch, statsCh)

	var statsWg sync.WaitGroup
	statsWg.Add(1)
	go func() {
		defer statsWg.Done()
		for s := range statsCh {
			stats.Add(s)
		}
	}()

	wg.Wait()
	close(statsCh)

	statsWg.Wait()

	return stats
}

func formatBytes(c uint64) string {
	b := float64(c)

	switch {
	case c > 1<<40:
		return fmt.Sprintf("%.3f TiB", b/(1<<40))
	case c > 1<<30:
		return fmt.Sprintf("%.3f GiB", b/(1<<30))
	case c > 1<<20:
		return fmt.Sprintf("%.3f MiB", b/(1<<20))
	case c > 1<<10:
		return fmt.Sprintf("%.3f KiB", b/(1<<10))
	default:
		return fmt.Sprintf("%dB", c)
	}
}

func main() {
	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "usage: parallel-read-benchmark DIR\n")
		os.Exit(1)
	}

	dir := flag.Args()[0]
	fmt.Fprintf(os.Stderr, "traversing %v with %v workers\n", dir, workers)

	f, err := os.OpenFile(outputFile, syscall.O_APPEND, 0644)
	if os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "creating output file %v\n", outputFile)
		err = ioutil.WriteFile(outputFile, []byte("workers\tfiles\tdirs\tbytes\ttime (seconds)\tbandwidth (per second)\n"), 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating output file %v: %v\n", outputFile, err)
			os.Exit(2)
		}

		f, err = os.OpenFile(outputFile, syscall.O_APPEND, 0644)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening output file %v: %v\n", outputFile, err)
	}

	start := time.Now()
	stats := traverse(workers, dir)
	sec := float64(time.Since(start)) / float64(time.Second)
	bps := float64(stats.bytes) / sec

	fmt.Fprintf(os.Stderr, "%v files, %v dirs, %v, %vs, %v/s\n",
		stats.files, stats.dirs, formatBytes(uint64(stats.bytes)), sec, formatBytes(uint64(bps)))

	fmt.Fprintf(f, "%v\t%v\t%v\t%v\t%v\t%v\n",
		workers, stats.files, stats.dirs, stats.bytes, sec, uint64(bps))

	err = f.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error closing file %v: %v\n", outputFile, err)
	}
}

// This package converts a genetic map and contigs associated with it into a pseumolecule
// It uses the contig type from the contig package to create sorted and
package main

import (
	"ContigMapping"
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

func ReadCmdLine() (*os.File, *os.File, string, int) {
	var mapFile, markerFile, outfile string
	var t int
	flag.StringVar(&mapFile, "map", "", "Name of the file with the genetic map")
	flag.StringVar(&markerFile, "markers", "", "Name of the file with the marker information")
	flag.StringVar(&outfile, "out", "", "Name of the output file")
	flag.IntVar(&t, "threads", 1, "Number of threads/cores to use")
	flag.Parse()
	mapHandle, maperr := os.Open(mapFile)
	markerHandle, markererr := os.Open(markerFile)
	if maperr != nil || markererr != nil {
		log.Fatal(maperr, markererr)
	}
	return mapHandle, markerHandle, outfile, t
}

func parseGenMap(MChan chan map[string]*ContigMapping.Marker, lgChan chan map[string]*ContigMapping.ContigMap, file *os.File) {
	defer file.Close()
	fmt.Println("Reading and parsing map...")
	LGMap := make(map[string]*ContigMapping.ContigMap)
	LG := ContigMapping.NewContigMap()
	var skipRegex = regexp.MustCompile(`^;`)
	var GP = regexp.MustCompile(`^group`)
	scanner := bufio.NewScanner(file)
L:
	for {
		ok := scanner.Scan()
		switch {
		case !ok:
			LGMap[LG.Name] = LG
			break L
		case skipRegex.MatchString(scanner.Text()) || scanner.Text() == "":
			continue
		case GP.MatchString(scanner.Text()):
			if LG.Name != "" {
				LGMap[LG.Name] = LG
				LG = ContigMapping.NewContigMap()
			}
			g := strings.Split(scanner.Text(), " ")
			LG.Name = g[1]
		default:
			markers := <-MChan
			values := strings.Split(scanner.Text(), "\t")
			p, _ := strconv.ParseFloat(values[1], 64)
			po, _ := strconv.ParseFloat(fmt.Sprintf("%.03f", p), 64)
			pos := uint64(po * 1000)
			if m, ok := markers[values[0]]; ok {
				m.GenPos = pos
				m.LG = LG.Name
				LG.AddMarkers(m)
			} else {
				M := &ContigMapping.Marker{Name: values[0], GenPos: pos, LG: LG.Name}
				markers[values[0]] = M
				LG.AddMarkers(M)
			}
			MChan <- markers
		}
	}
	lgChan <- LGMap
	fmt.Println("Finished with map")
}

func parseMarkerInfo(MChan chan map[string]*ContigMapping.Marker, cChan chan map[string]*ContigMapping.Contig, file *os.File) {
	defer file.Close()
	fmt.Println("Reading and parsing marker info...")
	CMap := make(map[string]*ContigMapping.Contig)
	var mok, cok bool
	m := &ContigMapping.Marker{}
	c := ContigMapping.NewContig()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		values := strings.Split(scanner.Text(), "\t")
		p := strings.Split(values[2], ".")
		pos, _ := strconv.ParseUint(p[0], 10, 64)
		weight, _ := strconv.ParseUint(values[3], 10, 64)
		markers := <-MChan
		m, mok = markers[values[0]]
		c, cok = CMap[values[1]]
		if mok {
			m.Contig = values[1]
			m.Weight = weight
			m.ConPos = pos
		} else {
			m = &ContigMapping.Marker{Name: values[0], Contig: values[1], Weight: weight, ConPos: pos}
			markers[values[0]] = m
		}
		if cok {
			c.AddMarkers(m)
		} else {
			c = ContigMapping.NewContig()
			c.Name = values[1]
			c.AddMarkers(m)
			CMap[values[1]] = c
		}
		MChan <- markers
	}
	cChan <- CMap
	fmt.Println("Finished reading marker info")
}

func CompleteContigs(c *ContigMapping.Contig, lgChan chan map[string]*ContigMapping.ContigMap, erChan chan *os.File, wg *sync.WaitGroup) {
	er := c.Autocomplete()
	erOut := <-erChan
	fmt.Fprintln(erOut, er)
	erChan <- erOut
	lgMap := <-lgChan
	if LG, ok := lgMap[c.LG]; ok && c.Placeable {
		LG.AddContigs(c)
	}
	lgChan <- lgMap
	wg.Done()
}

func WriteContigMaps(LG *ContigMapping.ContigMap, erChan chan *os.File, wg *sync.WaitGroup) {
	records := LG.WriteMap()
	o := <-erChan
	fmt.Fprintln(o, records)
	erChan <- o
	wg.Done()

}

func main() {
	var wg sync.WaitGroup
	MMap := make(map[string]*ContigMapping.Marker)
	mChan := make(chan map[string]*ContigMapping.Marker, 1)
	lgChan := make(chan map[string]*ContigMapping.ContigMap, 1)
	cChan := make(chan map[string]*ContigMapping.Contig, 1)
	erChan := make(chan *os.File, 1)
	mapHandle, markerHandle, outfile, t := ReadCmdLine()
	runtime.GOMAXPROCS(t)
	mChan <- MMap
	go parseGenMap(mChan, lgChan, mapHandle)
	go parseMarkerInfo(mChan, cChan, markerHandle)
	cMap := <-cChan
	lgMap := <-lgChan
	MMap = <-mChan
	lgChan <- lgMap
	erChan <- os.Stderr
	fmt.Println("Completing contigs...")
	for _, c := range cMap {
		wg.Add(1)
		go CompleteContigs(c, lgChan, erChan, &wg)
	}
	wg.Wait()
	lgMap = <-lgChan
	fmt.Println("Done")
	fmt.Println("Writing the maps...")
	out, e := os.Create(outfile)
	if e != nil {
		panic(e)
	}

	// Clean the channel from os.Stderr writer
	<-erChan

	// Load the channel with the output file
	erChan <- out
	for _, LG := range lgMap {
		wg.Add(1)
		go WriteContigMaps(LG, erChan, &wg)
	}
	wg.Wait()
	out.Close()
	fmt.Println("Done")
	fmt.Println("All done. Check the log for errors.")
}

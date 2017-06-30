package ContigMapping

import (
	"fmt"
	"sort"
	"strconv"
)

// Type definitions

// Struct with data about each Marker: genetic position, contig position and number of missing data
type Marker struct {
	Name   string
	ConPos uint64
	GenPos uint64
	Weight uint64
	LG     string
	Contig string
}

// Struct with data about each contig. It has a name and a map with all the Marker objects in it
type Contig struct {
	Markers     *map[string]*Marker
	Name        string
	GenPos      uint64
	AvgWeight   uint64
	Orientation string
	Range       [2]*Marker
	LG          string
	Placeable   bool
}

//Struct data about a map of contigs
type ContigMap struct {
	Contigs  *map[string]*Contig
	Markers  *map[string]*Marker
	Filtered bool
	Name     string
}

// Printing methods

func (m *Marker) String() string {
	out := "{ Name: " + m.Name
	out += ", ConPos: " + strconv.FormatUint(m.ConPos, 10)
	out += ", GenPos: " + strconv.FormatUint(m.GenPos, 10)
	out += ", Weight: " + strconv.FormatUint(m.Weight, 10)
	out += ", LG: " + m.LG + ", Contig: " + m.Contig + " }\n"
	return out
}

func (c *Contig) String() string {
	out := "{ Name: " + c.Name
	out += ", Placeable: " + strconv.FormatBool(c.Placeable)
	out += ", Markers: " + strconv.Itoa(len(*c.Markers))
	out += ", GenPos: " + strconv.FormatUint(c.GenPos, 10)
	out += ", AvgWeight: " + strconv.FormatUint(c.AvgWeight, 10)
	out += ", Orientation: " + c.Orientation + ", LG: " + c.LG
	out += ", Range: [ " + strconv.FormatUint(c.Range[0].GenPos, 10) + ", " + strconv.FormatUint(c.Range[1].GenPos, 10) + " ] }\n"
	return out
}

func (CM *ContigMap) String() string {
	out := "{ Name: " + CM.Name
	out += ", Filtered: " + strconv.FormatBool(CM.Filtered)
	out += ", Markers: " + strconv.Itoa(len(*CM.Markers))
	out += ", Contigs: " + strconv.Itoa(len(*CM.Contigs)) + " }\n"
	return out
}

//Methods to implemeent sort interface on list of uint64
type Uintarr []uint64

func (a Uintarr) Len() int {
	return len(a)
}

func (a Uintarr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a Uintarr) Less(i, j int) bool {
	return a[i] < a[j]
}

// Methods to sort a list of *Marker
type ByGenPos []*Marker
type ByConPos []*Marker

// Methods to implement the sort.interface
func (a ByConPos) Len() int {
	return len(a)
}

func (a ByConPos) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByConPos) Less(i, j int) bool {
	return a[i].ConPos < a[j].ConPos
}

func (a ByGenPos) Len() int {
	return len(a)
}

func (a ByGenPos) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByGenPos) Less(i, j int) bool {
	return a[i].GenPos < a[j].GenPos
}

// Methods to sort a list of *Contig
type lessFunc func(c1, c2 *Contig) bool

type multiSorter struct {
	contigs []*Contig
	less    []lessFunc
}

// Implementing the sort interface for the multiSorter
func (ms *multiSorter) Sort(contigs []*Contig) {
	ms.contigs = contigs
	sort.Sort(ms)
}

func OrderedBy(less ...lessFunc) *multiSorter {
	return &multiSorter{less: less}
}

func (ms *multiSorter) Len() int {
	return len(ms.contigs)
}

func (ms *multiSorter) Swap(i, j int) {
	ms.contigs[i], ms.contigs[j] = ms.contigs[j], ms.contigs[i]
}

func (ms *multiSorter) Less(i, j int) bool {
	p, q := ms.contigs[i], ms.contigs[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		switch {
		case less(p, q):
			// p < q, so we have a decision.
			return true
		case less(q, p):
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	return ms.less[k](p, q)
}

// Methods on the ContigMap struct

// Constructor

func NewContigMap() *ContigMap {
	markers := make(map[string]*Marker)
	contigs := make(map[string]*Contig)
	CM := ContigMap{Markers: &markers, Contigs: &contigs}
	return &CM
}

// Build and set the Map field in the ContigMap struct
func (CM *ContigMap) buildMap() *map[uint64][]*Contig {
	M := make(map[uint64][]*Contig)
	C := CM.Contigs
	if !CM.Filtered {
		for _, c := range *C {
			min := c.Range[0].GenPos
			M[min] = append(M[min], c)
		}
		// Next line was only for debugging
		//fmt.Println(CM.Name, len(M))
	} else {
		for _, c := range *C {
			k := c.GenPos
			M[k] = append(M[k], c)
		}
	}
	return &M
}

// Method to remove contigs that are apparently missplaced
func (CM *ContigMap) filterContigs() (out int) {
	decWeight := func(c1, c2 *Contig) bool {
		return c1.AvgWeight > c2.AvgWeight
	}

	maxRange := func(c1, c2 *Contig) bool {
		return c1.Range[1].GenPos > c1.Range[1].GenPos
	}

	name := func(c1, c2 *Contig) bool {
		return c1.Name < c2.Name
	}

	draftMap := CM.buildMap()
	var sortedKeys []uint64
	contig := &Contig{AvgWeight: 0, GenPos: 0, Range: [2]*Marker{&Marker{GenPos: 0}, &Marker{GenPos: 0}}}

	// fill the array of keys to be sorted
	for i, _ := range *draftMap {
		sortedKeys = append(sortedKeys, i)
	}
	sort.Sort(Uintarr(sortedKeys))

	//Loop through the sorted array.
	for _, i := range sortedKeys {

		// Get the list of contig pointers for from the draftmap at the current position
		// We keep only one representative Contig for each position, the one with the largest weight value and widest range.
		contigList := (*draftMap)[i]

		// We sort the list of contigs in decreasing order of average weight and the decreasing range
		// This helps us reduce the complexity of the decision tree
		OrderedBy(decWeight, maxRange, name).Sort(contigList)
		tmpContig := &Contig{Name: "dummy", AvgWeight: 0, Range: [2]*Marker{&Marker{GenPos: i}, &Marker{GenPos: i}}}

		// Iterate through the sorted list
		for _, c := range contigList {
			switch {
			case !c.Placeable || c.LG != CM.Name:
				delete(*CM.Contigs, c.Name)
				out++
				continue

			// Keep contigs that have all markers in the same genetic position
			case c.Range[1].GenPos == c.Range[0].GenPos:
				continue

			// First one with range > 0
			case tmpContig.Name == "dummy":
				tmpContig = c

				//Next line was only for debugging
				//fmt.Println("contigs in: ", CM.Name, i, "\t-->\t", len(contigList), ":\n", contigList, "\n", c.Name, "\n", "\nRepresentative:\n", tmpContig.Name, "\n")

				continue

			// If there is a representative contig already
			case tmpContig.AvgWeight == c.AvgWeight && tmpContig.Range[1].GenPos == c.Range[1].GenPos:
				delete(*CM.Contigs, tmpContig.Name)
				delete(*CM.Contigs, c.Name)
				tmpContig = &Contig{Name: "dummy", AvgWeight: 0, Range: [2]*Marker{&Marker{GenPos: i}, &Marker{GenPos: i}}}
				out += 2
				continue

			// remove everything else
			default:
				delete(*CM.Contigs, c.Name)
				out++
				continue
			}
		}

		// Now compare the representative Contig from the current position with the representative of the previous position.
		// If the current representative Contig is contained in the range of the previous representative contig, keep the one with most weight
		switch {

		// There is no current representative Contig, continue
		case tmpContig.Name == "dummy":
			continue

		// The current contig is not contained in the previous one
		case tmpContig.Range[1].GenPos > contig.Range[1].GenPos:
			contig = tmpContig
			continue

		// If contained, keep the one with more weight
		case tmpContig.AvgWeight < contig.AvgWeight:
			delete(*CM.Contigs, tmpContig.Name)
			if tmpContig.Name != "dummy" {
				out++
			}
			continue
		case tmpContig.AvgWeight > contig.AvgWeight:
			delete(*CM.Contigs, contig.Name)
			if tmpContig.Name != "dummy" {
				out++
			}
			contig = tmpContig
			continue

		// If equal weight, remove both
		case tmpContig.AvgWeight == contig.AvgWeight:
			delete(*CM.Contigs, tmpContig.Name)
			delete(*CM.Contigs, contig.Name)
			if tmpContig.Name != "dummy" {
				out += 2
			} else {
				out++
			}
			contig = tmpContig
			continue
		default:
			fmt.Println("Error", contig, tmpContig)
		}
	}
	CM.Filtered = true
	return out
}

// Method to write the final ContigMap
func (CM *ContigMap) WriteMap() (out string) {
	deleted := CM.filterContigs()
	out = "### LG: " + CM.Name + "\n### Deleted Sequences: " + strconv.Itoa(deleted) + "\n"
	finalMap := CM.buildMap()
	var keys []uint64
	for i, _ := range *finalMap {
		keys = append(keys, i)
	}
	sort.Sort(Uintarr(keys))
	for _, k := range keys {
		var s string = ""
		for _, c := range (*finalMap)[k] {
			s = ""
			s += c.Name
			s += "\t"
			s += strconv.FormatUint(c.GenPos, 10)
			s += "\t"
			s += c.Orientation
			out += s + "\n"
		}
	}
	return out
}

// Method to add marker pointers to the contig map
func (CM *ContigMap) AddMarkers(markers ...*Marker) {
	M := CM.Markers
	for _, m := range markers {
		(*M)[m.Name] = m
	}
}

//Method to add contigs to the contig map
func (CM *ContigMap) AddContigs(contigs ...*Contig) {
	C := CM.Contigs
	for _, c := range contigs {
		(*C)[c.Name] = c
	}
}

// Methods on the Contig struct

// Constructor
func NewContig() *Contig {
	markers := make(map[string]*Marker)
	c := Contig{Markers: &markers, Placeable: true}
	return &c
}

// This function allows to add 0 or more markers to the contig struct
func (c *Contig) AddMarkers(markers ...*Marker) {
	M := c.Markers
	for _, m := range markers {
		(*M)[m.Name] = m
	}
}

// This method checks if all he top markers of a contig belong to the same linkage group
func (c *Contig) AssignLG() (lg string) {
	if !c.Placeable {
		c.LG = "-"
		return "-"
	}
	topMarkers := c.Top()
	for _, m := range topMarkers {
		switch {
		case lg == "":
			lg = m.LG
		case m.LG != lg:
			c.Placeable = false
			c.LG = "-"
			return "-"
		case m.LG == lg:
			continue
		}
	}
	c.Placeable = true
	c.LG = lg
	return lg
}

// Calculate average weight of the markers in a contig
func (c *Contig) CalculateAvgWeight() uint64 {
	if c.LG == "" {
		c.AssignLG()
	}
	if !c.Placeable {
		return 0.0
	}
	var sum uint64 = 0
	var tot uint64 = 0
	for _, m := range *c.Markers {
		if m.LG == c.LG {
			sum += m.Weight
			tot++
		} else {
			continue
		}
	}
	c.AvgWeight = sum / tot
	return sum / tot
}

// Given a contig, return a weighted mean position in the genetic map
// the maximum weight is for Markers with 0 missing data (most accurate)
func (c *Contig) CalculateMapPos() uint64 {
	if c.LG == "" {
		c.AssignLG()
	}
	if !c.Placeable {
		return 0.0
	}
	var weight uint64 = 0
	var sum uint64 = 0
	for _, m := range *c.Markers {
		if m.LG == c.LG {
			sum += m.Weight * m.GenPos
			weight += m.Weight
		} else {
			continue
		}
	}
	c.GenPos = sum / weight
	return (sum / weight)
}

// Given a contig return the position in the contig where the weighted genetic position would be placed
func (c *Contig) CentrePos() uint64 {
	if c.LG == "" {
		c.AssignLG()
	}
	if !c.Placeable {
		return 0.0
	}
	var sum uint64 = 0
	var tot uint64 = 0
	for _, m := range *(*c).Markers {
		if m.LG == c.LG {
			sum += m.ConPos
			tot++
		} else {
			continue
		}
	}
	return sum / tot
}

// Given a contig return a slice of *Marker with the best weighted markers of the contig
func (c *Contig) Top() (out []*Marker) {
	var maxWeight uint64 = 0
	if c.LG == "" {
		for _, m := range *c.Markers {
			switch {
			case m.Weight > maxWeight:
				out = nil
				out = append(out, m)
				maxWeight = m.Weight
			case m.Weight == maxWeight:
				out = append(out, m)
			default:
				continue
			}
		}
		return out
	} else {
		for _, m := range *c.Markers {
			switch {
			case m.LG != c.LG:
				continue
			case m.Weight > maxWeight:
				out = nil
				out = append(out, m)
				maxWeight = m.Weight
			case m.Weight == maxWeight:
				out = append(out, m)
			default:
				continue
			}
		}
		return out
	}
}

// Given a contig it returns two values: a string "+",  "-" or "" and a boolean
// true boolean means the Markers in the contig were coherent (colinear and contiguous in the genetic map and in the contig)
// false boolean means the Marker has unsolvable conflicts and cannot be placed accurately
// A string "" means that there was not enough information to orientate the contig, but it was possible lo place it in the map.
func (c *Contig) Orient() (string, bool) {

	// Checl if it has already been flagged as unplaceable
	if !c.Placeable {
		return "", false
	}

	// Check if LG has been assigned
	if c.LG == "" {
		c.AssignLG()
	}

	// Check if GenPos has been assigned
	if c.GenPos == 0 {
		c.CalculateMapPos()
	}

	// Get the top markers to compare
	topMarkers := c.Top()

	// Check that top markers actually have different positions
	topPos := make(map[uint64]int)
	for _, m := range topMarkers {
		topPos[m.GenPos] = 1
	}

	// If the top markers are the only markers in the contig and they have the same genetic position
	if len(topMarkers) == len(*c.Markers) && len(topPos) == 1 {
		return "", false
	}

	// If there is only one topMarker or if the topmarkers are in the same genetic position
	if len(topMarkers) == 1 || len(topPos) == 1 {

		// create new weighted Marker to get the right orientation
		p := Marker{ConPos: c.CentrePos(), GenPos: c.GenPos}
		topMarkers = append(topMarkers, &p)
	}
	out, ok := OrientMarkers(topMarkers...)
	c.Orientation = out
	c.Placeable = ok
	return out, ok
}

// Define the limits of the contig in the genetic map. Set the Range field of the Contig struct
func (c *Contig) CalculateRange() {
	if c.Placeable == false {
		return
	}
	if c.LG == "" {
		c.AssignLG()
	}
	// Check if GenPos has been assigned
	if c.GenPos == 0 {
		c.CalculateMapPos()
	}
	topPos := make(map[uint64]int)
	for _, m := range *c.Markers {
		topPos[m.GenPos]++
	}
	if len(topPos) == 1 {
		p := Marker{GenPos: c.GenPos, Weight: c.MaxWeight()}
		c.Range[0] = &p
		c.Range[1] = &p
		return
	}
	topMarkers := c.Top()
	if len(topMarkers) == 1 {
		p := Marker{GenPos: c.GenPos, Weight: c.MaxWeight()}
		topMarkers = append(topMarkers, &p)
	}
	sort.Sort(ByGenPos(topMarkers))
	c.Range[0] = topMarkers[0]
	c.Range[1] = topMarkers[len(topMarkers)-1]
}

// Get the Maximum weight of all the markers in a contig
func (c *Contig) MaxWeight() (out uint64) {
	markers := c.Markers
	for _, m := range *markers {
		if m.Weight > out {
			out = (*m).Weight
		}
	}
	return out
}

// Fill all the fields in the contig struct
func (c *Contig) Autocomplete() (out string) {
	out = "Processing contig " + c.Name
	out += "\n\tAssinging LG = " + c.AssignLG()
	out += "\n\tCalculating Map Position = " + strconv.FormatUint(c.CalculateMapPos(), 10)
	out += "\n\tCalculating Avg weight = " + strconv.FormatUint(c.CalculateAvgWeight(), 10)
	st, ok := c.Orient()
	out += "\n\tOrienting contig = " + st + " " + strconv.FormatBool(ok)
	c.CalculateRange()
	out += "\n\tPlaceable = " + strconv.FormatBool(c.Placeable)
	return out
}

// General functions using the structs declared here.

// This function is used only by the OrientMarkers function. It returns "+", "-" or "" depending on the difference x-y
func sign(x, y uint64) string {
	switch {
	case x < y:
		return "-"
	case x > y:
		return "+"
	case x == y:
		return ""
	}
	return ""
}

// Takes as many *Markers as available and compares them. It returns a sign "+/-" or an empty string and a bool if the markers cannot be oriented.
func OrientMarkers(data ...*Marker) (out string, ok bool) {

	// Sort the data slice
	sort.Sort(ByConPos(data))

	// Iterate through the markers
	for i := 0; i < len(data)-1; i++ {

		// First get the pointers to the markers and assign a relation between them "" or "+" or "-"
		m1, m2 := data[i], data[i+1]
		strand := ""
		switch {

		// Cannot orient if there is no difference between the genetic positions of the markers.
		case sign(m1.GenPos, m2.GenPos) == "":
			continue
		case sign(m1.GenPos, m2.GenPos) == sign(m1.ConPos, m2.ConPos):
			strand = "+"
		case sign(m1.GenPos, m2.GenPos) != sign(m1.ConPos, m2.ConPos):
			strand = "-"
		}

		// Now compare to previous relations, if any, and check if they remain the same
		switch {
		case out == "":
			out = strand
			continue
		case strand != out:
			return "", false
		case strand == out:
			continue
		}
	}
	return out, true
}

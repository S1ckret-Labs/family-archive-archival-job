package main

import (
	"fmt"
	"github.com/tidwall/btree"
	"gopkg.in/guregu/null.v4"
	"strconv"
	"time"
)

type Object interface {
	Key() string
}

type Archive struct {
	key           string
	sizeBytes     int64
	objectsInside int64
}

func (a Archive) String() string {
	return fmt.Sprintf("Archive[key: %s, size: %08d, inside: %03d]", a.key, a.sizeBytes, a.objectsInside)
}

func (a Archive) Key() string {
	return a.key
}

type Dir struct {
	key           string
	level         int64
	sizeBytes     int64
	objectsInside int64
}

func (d Dir) String() string {
	return fmt.Sprintf("Dir[key: %s, level: %d, size: %d, inside: %d]", d.key, d.level, d.sizeBytes, d.objectsInside)
}

func (d Dir) Key() string {
	return d.key
}

type File struct {
	key        string
	sizeBytes  int64
	takenAtSec null.Int
}

func (f File) String() string {
	return fmt.Sprintf("File[key: %s, size: %d, takenAtSec: %d]", f.key, f.sizeBytes, f.takenAtSec.Int64)
}

func (f File) Key() string {
	return f.key
}

type Node struct {
	object   Object
	children *btree.Map[string, *Node]
}

func (n *Node) hasChildOneLevel(key string) *Node {
	v, _ := n.children.Get(key)
	return v
}

func (n *Node) insert(obj Object) *Node {
	newNode := Node{object: obj, children: btree.NewMap[string, *Node](32)}
	n.children.Set(obj.Key(), &newNode)
	return &newNode
}

type ObjectTree struct {
	root *Node
}

func newObjectTree() ObjectTree {
	rootNode := Node{
		children: btree.NewMap[string, *Node](32),
		object: Dir{
			key:           "/",
			level:         rootLevel,
			sizeBytes:     0,
			objectsInside: 0,
		},
	}
	return ObjectTree{root: &rootNode}
}

func newObjectTreeFromRequests(requests []UploadRequest) ObjectTree {
	tree := newObjectTree()

	for _, req := range requests {
		if req.TakenAtSec.Valid {
			t := time.Unix(req.TakenAtSec.Int64, 0)
			year, month, day := t.Date()
			tree.insert(year, int(month), day, File{
				key:        req.ObjectKey,
				sizeBytes:  req.SizeBytes,
				takenAtSec: req.TakenAtSec,
			})
		} else {
			tree.insertNoMetadata(File{
				key:        req.ObjectKey,
				sizeBytes:  req.SizeBytes,
				takenAtSec: req.TakenAtSec,
			})

		}
	}
	return tree
}

const rootLevel = 0
const yearLevel = 1
const monthLevel = 2
const dayLevel = 3

// insert adds file as a leaf node to the t. Also creates parent dirs for the file if needed.
func (t ObjectTree) insert(year int, month int, day int, file File) ObjectTree {
	yearStr := strconv.Itoa(year)
	monthStr := strconv.Itoa(month)
	dayStr := strconv.Itoa(day)

	yearNode := t.root.hasChildOneLevel(yearStr)
	if yearNode == nil {
		yearNode = t.root.insert(Dir{key: yearStr, level: yearLevel})
	}

	monthNode := yearNode.hasChildOneLevel(monthStr)
	if monthNode == nil {
		monthNode = yearNode.insert(Dir{key: monthStr, level: monthLevel})
	}

	dayNode := monthNode.hasChildOneLevel(dayStr)
	if dayNode == nil {
		dayNode = monthNode.insert(Dir{key: dayStr, level: dayLevel})
	}

	fileNode := dayNode.hasChildOneLevel(file.key)
	if fileNode == nil {
		fileNode = dayNode.insert(file)
	}

	return t
}

// insertNoMetadata adds file as a leaf node to dir called 'No metadata'.
// Also creates parent dirs for the file if needed.
func (t ObjectTree) insertNoMetadata(file File) ObjectTree {
	const mixedDirKey = "No metadata"
	mixedNode := t.root.hasChildOneLevel(mixedDirKey)
	if mixedNode == nil {
		mixedNode = t.root.insert(Dir{key: mixedDirKey})
	}

	fileNode := mixedNode.hasChildOneLevel(file.key)
	if fileNode == nil {
		fileNode = mixedNode.insert(file)
	}

	return t
}

// TraverseTreePostOrder traverses the tree just for debug purposes
func TraverseTreePostOrder(t ObjectTree) {
	if t.root == nil {
		return
	}

	t.root.children.Scan(func(_ string, node *Node) bool {
		TraverseTreePostOrder(ObjectTree{root: node})
		return true
	})

	if t.root.object != nil {
		//file, ok := t.root.object.(File)
		//if ok {
		//	fmt.Printf("%s\n", file)
		//}
		dir, ok := t.root.object.(Dir)
		if ok {
			fmt.Printf("%s\n", dir)
		}
		archive, ok := t.root.object.(Archive)
		if ok {
			fmt.Printf("%s\n", archive)
		}
	}
}

func CollectFolderSizeAndObjectInPlace(t ObjectTree) (int64, int64) {
	return _collectFolderSizeAndObjectInPlace(t.root)
}

// _collectFolderSizeAndObjectInPlace returns (bytes, objectsInside)
func _collectFolderSizeAndObjectInPlace(curr *Node) (int64, int64) {
	if curr == nil {
		// Skip current node
		return 0, 0
	}

	obj := curr.object
	if obj == nil {
		// Skip current node
		return 0, 0
	}
	fmt.Println("[Folder data collection] Going deep into:", obj)

	// Dive into the depth of a tree
	var currBytes int64 = 0
	var currObjects int64 = 0
	curr.children.Scan(func(_ string, child *Node) bool {
		bytes, objects := _collectFolderSizeAndObjectInPlace(child)
		currBytes += bytes
		currObjects += objects
		return true
	})

	//fmt.Println("[Folder data collection] Evaluating:", obj)

	file, ok := obj.(File)
	if ok {
		// Count this file
		return file.sizeBytes, 1
	}
	dir, ok := obj.(Dir)
	if ok {
		// Do not count dir as object. We care only about files
		// Propagate state up to folder
		dir.sizeBytes += currBytes
		dir.objectsInside += currObjects
		// Write to Node
		curr.object = dir
		fmt.Println("[Folder data collection] Updated folder:", curr.object)

		return currBytes, currObjects
	}
	return 0, 0
}

type GroupingState struct {
	bytesCounter   int64
	objectsCounter int64
	dirsToArchive  []*Node
	previousNode   *Node
	currYear       string
	currMonth      string
}

func (s *GroupingState) Reset() {
	s.dirsToArchive = []*Node{}
	s.bytesCounter = 0
	s.objectsCounter = 0
	s.previousNode = nil
}

func ArchiveDaysInplace(t ObjectTree) {
	state := GroupingState{
		bytesCounter:   0,
		objectsCounter: 0,
		dirsToArchive:  nil,
		previousNode:   nil,
		currYear:       "year",
		currMonth:      "month",
	}

	tryToArchive(nil, t.root, &state)
	// TODO: If state is not empty (meaning it contains dirsToArchive) and the size is not enough for individual archive
	// TODO: then Add to previous archive
	// TODO: (Remember about year and month snappines)
}

func tryToArchive(parent *Node, curr *Node, state *GroupingState) {
	if curr == nil {
		// Skip current node
		return
	}

	obj := curr.object
	if obj == nil {
		// Skip current node
		return
	}
	fmt.Printf("[Archival] Going deep into: %v\n", obj)

	dir, ok := obj.(Dir)
	if !ok || dir.level > dayLevel {
		// Skip current node
		return
	}

	// Check for year and month snappienes. An archive must not span across multiple months or years.
	if len(state.dirsToArchive) != 0 {
		yearIsDifferent := dir.level == yearLevel && state.currYear != dir.Key()
		monthIsDifferent := dir.level == monthLevel && state.currMonth != dir.Key()
		if yearIsDifferent || monthIsDifferent {
			// Current year or month differs from previous.
			// That means that we entered a new year or month.
			// We need to archive collected dirs to avoid spreading the archive across months or years.
			fmt.Printf("[Archival] Oh, snap! We need to archive now because we entered a new month or year! Previous year = %s, month = %s. Current dirLevel = %d, dirKey = %s\n", state.currYear, state.currMonth, dir.level, dir.Key())
			archiveName := createArchiveName(state.currYear, state.currMonth, state.dirsToArchive)
			archiveNode := createArchiveNode(archiveName, state.bytesCounter, state.objectsCounter, state.dirsToArchive)
			replaceDirsWithArchive(state.previousNode, archiveNode, state.dirsToArchive)
			// Reset state
			state.Reset()
		}
	}

	// Remember year, month for archive name
	if dir.level == yearLevel {
		state.currYear = dir.Key()
	} else if dir.level == monthLevel {
		state.currMonth = dir.Key()
	}

	// Remember the node
	state.previousNode = parent

	// Dive into the depth of a tree
	// We don't want to dive deeper if a dir has level = day level
	if dir.level < dayLevel {
		curr.children.Scan(func(_ string, child *Node) bool {
			tryToArchive(curr, child, state)
			return true
		})
	}

	// Only archive dirs with level = dayLevel
	if dir.level != dayLevel {
		// Skip current node
		return
	}

	// Count this dir
	state.bytesCounter += dir.sizeBytes
	state.objectsCounter += dir.objectsInside
	state.dirsToArchive = append(state.dirsToArchive, curr)

	const sizeBytes500Mb = 500 * 1024 * 1024
	canArchive := state.bytesCounter > sizeBytes500Mb && state.objectsCounter > 20

	fmt.Printf("[Archival] Can archive %v objects with total size of %v? %v\n", state.objectsCounter, state.bytesCounter, canArchive)
	if !canArchive {
		// Skip current node
		return
	}

	// Archive dirs now
	archiveName := createArchiveName(state.currYear, state.currMonth, state.dirsToArchive)
	archiveNode := createArchiveNode(archiveName, state.bytesCounter, state.objectsCounter, state.dirsToArchive)
	replaceDirsWithArchive(parent, archiveNode, state.dirsToArchive)

	// Reset state
	state.Reset()
}

func replaceDirsWithArchive(parent *Node, archiveNode Node, dirsToArchive []*Node) {
	parent.children.Set(archiveNode.object.Key(), &archiveNode)
	for _, dir := range dirsToArchive {
		parent.children.Delete(dir.object.Key())
	}
}

func createArchiveNode(archiveName string, sizeBytes, objectsNum int64, dirsToArchive []*Node) Node {
	archive := Archive{
		key:           archiveName,
		sizeBytes:     sizeBytes,
		objectsInside: objectsNum,
	}

	// Collect all day level dirs children together
	archiveChildren := btree.NewMap[string, *Node](32)
	for _, dir := range dirsToArchive {
		dir.children.Scan(func(key string, node *Node) bool {
			archiveChildren.Set(key, node)
			return true
		})
	}

	return Node{
		object:   archive,
		children: archiveChildren,
	}
}

func createArchiveName(year, month string, dirsToBeArchived []*Node) string {
	body := ""
	length := len(dirsToBeArchived)
	if length == 1 {
		// I assume that only dirs are supplied
		// TODO: Change *Node to *Dir parameter
		body = fmt.Sprintf("%02s", dirsToBeArchived[0].object.Key())
	}
	if length > 1 {
		// I assume that days are sequential (i.e. no gaps between 07 and 15 day e.g.)
		// TODO: Check for this later
		body = fmt.Sprintf("%02s-%02s", dirsToBeArchived[0].object.Key(), dirsToBeArchived[length-1].object.Key())
	}
	return fmt.Sprintf("%04s.%02s.%s.zip", year, month, body)
}

func BuildObjectTree(requests []UploadRequest) ObjectTree {
	tree := newObjectTreeFromRequests(requests)

	bytes, objects := CollectFolderSizeAndObjectInPlace(tree)
	// TODO: Update the total counter in database
	fmt.Printf("There are new %d objects with total size of %d\n", objects, bytes)

	ArchiveDaysInplace(tree)

	return tree
}

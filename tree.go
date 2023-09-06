package main

import (
	"gopkg.in/guregu/null.v4"
	"strconv"
	"time"
)

type Object interface {
	Key() string
}

type Dir struct {
	key           string
	level         int64
	sizeBytes     int64
	objectsInside int64
}

func (d Dir) Key() string {
	return d.key
}

type File struct {
	key        string
	sizeBytes  int64
	takenAtSec null.Int
}

func (f File) Key() string {
	return f.key
}

type Node struct {
	object   Object
	children []*Node
}

func (n *Node) hasChild(key string) *Node {
	for _, c := range n.children {
		if c.object.Key() == key {
			return c
		}
	}
	return nil
}

func (n *Node) insert(obj Object) *Node {
	newNode := Node{object: obj}
	n.children = append(n.children, &newNode)
	return &newNode
}

type ObjectTree struct {
	root *Node
}

func NewObjectTree() ObjectTree {
	rootNode := Node{}
	return ObjectTree{root: &rootNode}
}

// insert adds file as a leaf node to the t. Also creates parent dirs for the file if needed.
func (t ObjectTree) insert(year int, month int, day int, file File) ObjectTree {
	yearStr := strconv.Itoa(year)
	monthStr := strconv.Itoa(month)
	dayStr := strconv.Itoa(day)

	yearNode := t.root.hasChild(yearStr)
	if yearNode == nil {
		yearNode = t.root.insert(Dir{key: yearStr, level: 1})
	}

	monthNode := yearNode.hasChild(monthStr)
	if monthNode == nil {
		monthNode = yearNode.insert(Dir{key: monthStr, level: 2})
	}

	dayNode := monthNode.hasChild(dayStr)
	if dayNode == nil {
		dayNode = monthNode.insert(Dir{key: dayStr, level: 3})
	}

	fileNode := dayNode.hasChild(file.key)
	if fileNode == nil {
		fileNode = dayNode.insert(file)
	}

	return t
}

// insertNoMetadata adds file as a leaf node to dir called 'No metadata'.
// Also creates parent dirs for the file if needed.
func (t ObjectTree) insertNoMetadata(file File) ObjectTree {
	const mixedDirKey = "No metadata"
	mixedNode := t.root.hasChild(mixedDirKey)
	if mixedNode == nil {
		mixedNode = t.root.insert(Dir{key: mixedDirKey})
	}

	fileNode := mixedNode.hasChild(file.key)
	if fileNode == nil {
		fileNode = mixedNode.insert(file)
	}

	return t
}

func TraverseTreePostOrder(t ObjectTree) {
	if t.root == nil {
		return
	}

	for _, node := range t.root.children {
		TraverseTreePostOrder(ObjectTree{root: node})
	}

	if t.root.object != nil {
		file, ok := t.root.object.(File)
		if ok {
			println("I am a file!", file.key, file.sizeBytes)
		}
		dir, ok := t.root.object.(Dir)
		if ok {
			println("I am a dir!", dir.key, dir.level)
		}
	}
}

func (t ObjectTree) groupDaysIntoArchives() {

}

func BuildObjectTree(requests []UploadRequest) ObjectTree {
	tree := NewObjectTree()

	for _, r := range requests {
		if r.TakenAtSec.Valid {
			t := time.Unix(r.TakenAtSec.Int64, 0)
			year, month, day := t.Date()
			tree.insert(year, int(month), day, File{
				key:        r.ObjectKey,
				sizeBytes:  r.SizeBytes,
				takenAtSec: r.TakenAtSec,
			})
		} else {
			tree.insertNoMetadata(File{
				key:        r.ObjectKey,
				sizeBytes:  r.SizeBytes,
				takenAtSec: r.TakenAtSec,
			})

		}
	}

	return tree

}

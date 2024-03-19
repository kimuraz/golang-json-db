package utils

import "fmt"

// Binary tree implementation

type BTreeNode struct {
	Key   string
	Value map[string]bool // IDs
	Left  *BTreeNode
	Right *BTreeNode
}

type BTree struct {
	Root *BTreeNode
}

func NewBTree() *BTree {
	return &BTree{}
}

func (b *BTree) String() string {
	return b.Root.string()
}

func (n *BTreeNode) string() string {
	if n == nil {
		return ""
	}
	var left, right string
	if n.Left != nil {
		left = n.Left.string()
	}
	if n.Right != nil {
		right = n.Right.string()
	}
	return fmt.Sprintf("%s: %v\n%s%s", n.Key, n.Value, left, right)
}

func (b *BTree) Insert(key string, value string) {
	if b.Root == nil {
		b.Root = &BTreeNode{Key: key, Value: map[string]bool{value: true}}
	} else {
		b.Root.insert(key, value)
	}
}

func (n *BTreeNode) insert(key string, value string) {
	if key == n.Key {
		n.Value[value] = true
	} else if key < n.Key {
		if n.Left == nil {
			n.Left = &BTreeNode{Key: key, Value: map[string]bool{value: true}}
		} else {
			n.Left.insert(key, value)
		}
	} else {
		if n.Right == nil {
			n.Right = &BTreeNode{Key: key, Value: map[string]bool{value: true}}
		} else {
			n.Right.insert(key, value)
		}
	}
}

func (b *BTree) Search(key string) (map[string]bool, bool) {
	return b.Root.search(key)
}

func (n *BTreeNode) search(key string) (map[string]bool, bool) {
	if n == nil {
		return nil, false
	}
	if key == n.Key {
		return n.Value, true
	}
	if key < n.Key {
		return n.Left.search(key)
	}
	return n.Right.search(key)
}

func (b *BTree) IsEmpty() bool {
	return b.Root == nil
}

func (b *BTree) RemoveID(id string) {
	b.Root.removeID(id)
}

func (n *BTreeNode) removeID(id string) {
	if n == nil {
		return
	}
	if n.Value[id] {
		delete(n.Value, id)
	}
	n.Left.removeID(id)
	n.Right.removeID(id)
}

func (b *BTree) CountNodes() int {
	return b.Root.countNodes()
}

func (n *BTreeNode) countNodes() int {
	if n == nil {
		return 0
	}
	return 1 + n.Left.countNodes() + n.Right.countNodes()
}

func (b *BTree) PrintTree() {
	if b.Root == nil {
		fmt.Println("Tree is empty")
	} else {
		b.Root.printInOrder("")
	}
	fmt.Printf("Nodes: %d\n", b.CountNodes())
}

func (n *BTreeNode) printInOrder(prefix string) {
	if n == nil {
		return
	}
	n.Left.printInOrder(prefix + "  ")
	fmt.Printf("%s%s: %v\n", prefix, n.Key, n.Value)
	n.Right.printInOrder(prefix + "  ")
}

package main

import (
	"gopkg.in/guregu/null.v4"
	"testing"
	"time"
)

func TestTraverseTreePostOrder(t *testing.T) {
	tree := BuildObjectTree([]UploadRequest{
		{
			ObjectKey:  "img-2023-06-30",
			SizeBytes:  1500,
			TakenAtSec: null.NewInt(time.Now().Unix(), true),
		},
		{
			ObjectKey:  "img-2023-06-31",
			SizeBytes:  1500,
			TakenAtSec: null.NewInt(time.Now().Unix()+24*60*60, true),
		},
		{
			ObjectKey:  "img-2023-06-31-1",
			SizeBytes:  1500,
			TakenAtSec: null.NewInt(time.Now().Unix()+24*60*60+5*60, true),
		},
		{
			ObjectKey:  "img-2023-06-31-2",
			SizeBytes:  1500,
			TakenAtSec: null.NewInt(time.Now().Unix()+24*60*60+10*60, true),
		},
		{
			ObjectKey:  "img-2024-04-04",
			SizeBytes:  1500,
			TakenAtSec: null.NewInt(0, false),
		},
	})

	TraverseTreePostOrder(tree)
}

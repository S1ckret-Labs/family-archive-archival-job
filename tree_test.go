package main

import (
	"fmt"
	"gopkg.in/guregu/null.v4"
	"testing"
	"time"
)

const kb = 1024
const mb = 1024 * kb
const gb = 1024 * mb

type ObjectsOnDay struct {
	objectNum int
	year      int
	month     int
	day       int
}

func NewOOD(objectNum, year, month, day int) ObjectsOnDay {
	return ObjectsOnDay{
		objectNum: objectNum,
		year:      year,
		month:     month,
		day:       day,
	}
}

func CreateTakenAt(ood ObjectsOnDay, fileNumber int) time.Time {
	month := time.Month(ood.month)
	hour := 12 + fileNumber/60
	minute := fileNumber % 60
	second := 59
	loc, _ := time.LoadLocation("UTC")
	return time.Date(ood.year, month, ood.day, hour, minute, second, 0, loc)
}

func GenerateUploadsRequestsPerDay(objectsPerDay []ObjectsOnDay) []UploadRequest {
	const photoHdSize = 2 * mb
	const photo4kSize = 5 * mb
	const betweenFilesSeconds = 100

	result := make([]UploadRequest, 0)
	for day, ood := range objectsPerDay {
		for fileNumber := 1; fileNumber <= ood.objectNum; fileNumber++ {
			t := CreateTakenAt(ood, fileNumber).Unix()
			req := UploadRequest{
				ObjectKey:  fmt.Sprintf("generated-test-img-2023-01-%02d-%02d.jpg", day+1, fileNumber),
				SizeBytes:  int64(photo4kSize),
				TakenAtSec: null.NewInt(t, true),
			}
			result = append(result, req)
		}
	}

	return result

}

func TestTraverseTreePostOrder(t *testing.T) {
	requests := GenerateUploadsRequestsPerDay([]ObjectsOnDay{
		NewOOD(1, 2023, 8, 29),
		NewOOD(2, 2023, 8, 30),
		NewOOD(6, 2023, 8, 31),
		NewOOD(6, 2023, 9, 1),
		NewOOD(86, 2023, 9, 2),
		NewOOD(7, 2023, 9, 3),
		NewOOD(6, 2023, 9, 4),
		NewOOD(8, 2023, 9, 5),
		NewOOD(4, 2023, 9, 6),
		NewOOD(9, 2023, 9, 7),
		NewOOD(256, 2023, 9, 8),
		NewOOD(66, 2023, 9, 9),
		NewOOD(26, 2023, 9, 10),
		NewOOD(16, 2023, 9, 11),
		NewOOD(6, 2023, 9, 12),
		NewOOD(10, 2023, 9, 13),
	})
	tree := BuildObjectTree(requests)

	TraverseTreePostOrder(tree)
}

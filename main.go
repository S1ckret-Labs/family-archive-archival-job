package main

import (
	"database/sql"
	"github.com/spf13/viper"
	"gopkg.in/guregu/null.v4"
)

func main() {
	//config := loadConfig()
	//dbConStr := config.GetString("database_connection_string")
	//userId := config.GetInt64("user_id")
	//fileUploadsBucketName := config.GetString("file_uploads_bucket_name")
	//archivesBucketName := config.GetString("archives_bucket_name")

	//db, _ := sql.Open("mysql", dbConStr)
	//defer db.Close()

	//awsSession := session.Must(session.NewSession())
	//s3Client := s3.New(awsSession)

	// Read out upload requests
	//uploadRequests, err := FindUploadRequests(db, userId)
	//if err != nil {
	//	log.Fatalf("No upload requests found for user '%d'!", userId)
	//}

	// Read out object tree (just folders & archives)

	// TODO: This is logic part
	// Group media files to archive & archivess
	//tree := BuildObjectTree(uploadRequests)
	// Calculate object tree difference (that's to be created)

	// TODO: This is infra part
	// Download media files
	// Archive media files according to groups
	// Upload archives

	// Create folders, files, archives in a metadata database
}

func loadConfig() *viper.Viper {
	v := viper.New()
	v.SetEnvPrefix("FA")
	v.AutomaticEnv()
	return v
}

type UploadRequest struct {
	ObjectKey  string
	SizeBytes  int64
	TakenAtSec null.Int
}

func FindUploadRequests(db *sql.DB, userId int64) ([]UploadRequest, error) {
	const selectUploadFilesForUser = `select o.object_key, o.size_bytes, o.taken_at_sec from UploadRequests as o where user_id = ?;`

	rows, err := db.Query(selectUploadFilesForUser, userId)
	if err != nil {
		return nil, err
	}

	result := make([]UploadRequest, 0)
	for rows.Next() {
		var file UploadRequest
		err := rows.Scan(&file.ObjectKey, &file.SizeBytes, &file.TakenAtSec)
		if err != nil {
			return nil, err
		}
		result = append(result, file)
	}
	return result, nil
}

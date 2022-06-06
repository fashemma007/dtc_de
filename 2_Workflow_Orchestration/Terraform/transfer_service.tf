resource "google_project_service" "storage_transfer" {
  project =var.project
  service ="storagetransfer.googleapis.com"
}
data "google_storage_transfer_project_service_account" "default" {
   project =var.project
}
resource "google_storage_bucket" "terraform-transfer-service" {
   name ="terraform-transfer-service"
   storage_class ="STANDARD"
   project =var.project
   location="EU"
}
resource "google_storage_bucket_iam_member" "terraform-transfer-service-iam" {
   bucket =google_storage_bucket.terraform-transfer-service.name
   role ="roles/storage.admin"
   member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
   depends_on = [
     google_storage_bucket.terraform-transfer-service
   ]
}
resource "google_storage_transfer_job" "s3-bucket-nightly-backup2" {
   description="Execute a cloud transfer job via terraform"
   project=var.project
   transfer_spec {
     transfer_options{
        delete_objects_unique_in_sink=false
     }
     aws_s3_data_source {
       bucket_name = "nyc-tlc-923706205004"
       aws_access_key{
          access_key_id = var.aws_access_id
          secret_access_key = var.aws_secret_key
       }
     }
     gcs_data_sink {
       bucket_name = google_storage_bucket.terraform-transfer-service.name
       path = ""
     }
   }
   schedule {
     schedule_start_date{
        year = 2022
        month = 05
        day = 22
     }
     schedule_end_date {
        year = 2022
        month = 05
        day = 22
     }
   }
   depends_on = [google_storage_bucket_iam_member.terraform-transfer-service-iam]
}
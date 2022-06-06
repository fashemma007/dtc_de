locals {
  bucket_name = "de_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "learn-data-eng" 
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west1"#Belgium// Low CO2
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "all_trips_data"
}
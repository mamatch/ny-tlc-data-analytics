locals {
  data_lake_bucket = "ny_data_lake"
}

variable "project" {
  description = "GCP project ID"
}

variable "region" {
  description = "Region for GCP resources"
  default     = "europe-west1"
  type        = string
}


# Not need for now
variable "bucket_name" {
  description = "Name of the GCP bucket"
  default     = ""
}

variable "storage_class" {
  description = "The storage class of the bucket"
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery dataset"
  type        = string
  default     = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery table"
  type        = string
  default     = "my_trips"
}

terraform {
  backend "local" {

  }
  required_providers {
    google = {
      source = "harshicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  // credentials = file(var.credentials) # Use if we don't want to use GOOGLE_APPLICATION_CREDENTIAL ENVAR


}

resource "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.data_lake_bucket}_${var.project}"
  location = var.region

  # Optional but recommended
  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enable = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 //days
    }
  }
}


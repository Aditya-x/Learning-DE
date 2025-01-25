terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "6.17.0"
        }
    }
}

provider "google" {
    credentials = "./keys/my-creds.json"
    project     = "terraform-tut-448913"
    region      = "asia-south1"
}



resource "google_storage_bucket" "demo-bucket" {
    name          = "terraform-demo-8766-terra-bucket"
    location      = "US"
    force_destroy = true

    lifecycle_rule {
        condition {
        age = 1
        }
        action {
        type = "AbortIncompleteMultipartUpload"
        }
    }
}
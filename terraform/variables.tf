variable "location" {
    description = "Project Location"
    default = "US"
}

variable "project" {
    description = "Project Name"
    default = "terraform-tut-448913"
}

variable "region" {
    description = "Project region"
    default = "asia-south1"
}

variable "bq_dataset_name" {
    description = "My Bgquesry Dataset Name"
    default = "demo_dataset"
}


variable "gcs_storage_class" {
    description = "Bucket Storgae class"
    default = "Standard"
} 

variable "gcs_bucket_name" {
    description = "My Bucket Storgae class"
    default = "terraform-demo-8766-terra-bucket"
}


variable "credentials" {
    description = "My Credentials"
    default = "../google/credentials/google-credentials.json"
}
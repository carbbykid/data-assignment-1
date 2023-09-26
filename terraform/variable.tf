variable "project" {
    description = "Google Cloud Project ID"
}

variable "region" {
    description = "Google Cloud Project Main Region"
    default = "asia-east1"
}


variable "cloud_run_image" { 
    description = "Cloud Run Image"
    default = "cloud-run-batch-job"
}
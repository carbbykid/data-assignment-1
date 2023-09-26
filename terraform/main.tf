provider "google" {
    project = var.project
    region = var.region
}

resource "google_service_account" "batch_job" {
    account_id = "batch-job"
}

resource "google_storage_bucket" "mmo_adventure_event_processing" {
    name = "mmo_adventure_event_processing"
    location = var.region
    force_destroy = true

    public_access_prevention = "enforced"
    uniform_bucket_level_access = true
}

resource "google_storage_bucket_iam_binding" "binding" {
    bucket = google_storage_bucket.mmo_adventure_event_processing.name
    role = "roles/storage.admin"
    members = [
        "serviceAccount:${google_service_account.batch_job.email}",
    ]
}


resource "google_project_iam_binding" "batch_job" {
    project = var.project
    role     = "roles/storage.admin"
    members  = ["serviceAccount:${google_service_account.batch_job.email}"]
}

resource "google_cloud_run_v2_job" "cloud_run_batch_job" {
    name     = "cloud-run-batch-job"
    location = var.region

    template {
        template{
            containers {
                image = "us-docker.pkg.dev/cloudrun/container/hello"
            }
            service_account = google_service_account.batch_job.email
        }
    }

    lifecycle {
        ignore_changes = [
            launch_stage,
        ]
    }
}
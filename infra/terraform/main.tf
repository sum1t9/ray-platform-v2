#######################
# Network + GKE + Node Pools
#######################

resource "google_compute_network" "vpc" {
  name                    = "${var.cluster_name}-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.cluster_name}-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.1.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.2.0.0/20"
  }
}

resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  location = var.region

  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name

  deletion_protection      = false
  remove_default_node_pool = true
  initial_node_count       = 1

  node_config {
    disk_type    = "pd-standard"
    disk_size_gb = 30
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
}

resource "google_container_node_pool" "system_pool" {
  name     = "system-pool-v2"
  location = var.region
  cluster  = google_container_cluster.primary.name

  node_count = 1

  node_config {
    machine_type = "n2-standard-2"
    disk_type    = "pd-standard"
    disk_size_gb = 50

    labels = {
      "node-role"     = "system"
      "ray-node-type" = "head-capable"
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

resource "google_container_node_pool" "worker_pool" {
  name     = "worker-pool-v2"
  location = var.region
  cluster  = google_container_cluster.primary.name

  node_count = 1

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type = "n2-standard-2"
    preemptible  = true
    disk_type    = "pd-standard"
    disk_size_gb = 50

    labels = {
      "node-role"     = "worker"
      "ray-node-type" = "worker"
    }

    taint {
      key    = "ray-worker"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  autoscaling {
    min_node_count = 0
    max_node_count = 5
  }
}

#######################
# Storage + Artifact Registry + Identities
#######################

resource "google_storage_bucket" "data_bucket" {
  name                        = "${var.project_id}-ray2-data-bucket"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_artifact_registry_repository" "ray_repo" {
  repository_id = "ray-repo-v2"
  project       = var.project_id
  location      = var.region
  format        = "DOCKER"
  description   = "Ray pipeline images for v2 platform"
}

resource "google_service_account" "ray_worker" {
  account_id   = "ray-worker-sa-v2"
  display_name = "Ray Worker Service Account V2"
}

resource "google_service_account" "ray_head" {
  account_id   = "ray-head-sa-v2"
  display_name = "Ray Head Service Account V2"
}

resource "google_project_iam_member" "ray_worker_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.ray_worker.email}"
}

resource "google_project_iam_member" "ray_head_storage" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.ray_head.email}"
}

resource "google_service_account_iam_member" "ray_worker_wi_binding" {
  service_account_id = google_service_account.ray_worker.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[ray/ray-worker-sa-v2]"
}

resource "google_service_account_iam_member" "ray_head_wi_binding" {
  service_account_id = google_service_account.ray_head.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[ray/ray-head-sa-v2]"
}

#######################
# Seed input datasets in GCS
#######################

resource "google_storage_bucket_object" "supplychain" {
  name         = "supplychain.csv"
  bucket       = google_storage_bucket.data_bucket.name
  source       = "${path.module}/data/supplychain.csv"
  content_type = "text/csv"
}

resource "google_storage_bucket_object" "financial" {
  name         = "financial.csv"
  bucket       = google_storage_bucket.data_bucket.name
  source       = "${path.module}/data/financial.csv"
  content_type = "text/csv"
}
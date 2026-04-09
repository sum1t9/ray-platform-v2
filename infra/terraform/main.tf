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

  # KEY FIX 1: zonal cluster instead of regional
  # Regional = 3 nodes per pool (1 per zone) = 3x IP usage
  # Zonal = 1 node per pool = fits inside your quota of 4 IPs
  location = "asia-south1-c"

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
  name    = "system-pool-v2"
  # KEY FIX 2: match cluster zone, not region
  location = "asia-south1-c"
  cluster  = google_container_cluster.primary.name

  # KEY FIX 3: fixed node_count, no autoscaling on system pool
  # avoids GKE creating extra replacement nodes during updates
  node_count = 2

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    # KEY FIX 4: e2-medium for system pool - stable, low cost, fits quota
    machine_type = "e2-medium"
    disk_type    = "pd-standard"
    disk_size_gb = 50

    labels = {
      "node-role"     = "system"
      "ray-node-type" = "head-capable"
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}

resource "google_container_node_pool" "worker_pool" {
  name     = "worker-pool-v2"
  # KEY FIX 5: match cluster zone
  location = "asia-south1-c"
  cluster  = google_container_cluster.primary.name

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    # n2-standard-2 stays on worker pool - this is what your pipeline needs
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

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }

  autoscaling {
    min_node_count = 1
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

  lifecycle {
    prevent_destroy = true
  }
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
  role    = "roles/storage.objectAdmin"
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

# KEY FIX 6: use path.module to resolve CSV paths correctly
resource "google_storage_bucket_object" "supplychain" {
  name         = "data/SupplyChain_Data_S1.csv"
  bucket       = google_storage_bucket.data_bucket.name
  source       = "${path.module}/data/SupplyChain_Data_S1.csv"
  content_type = "text/csv"
}

resource "google_storage_bucket_object" "financial" {
  name         = "data/Financial_Data_S2.csv"
  bucket       = google_storage_bucket.data_bucket.name
  source       = "${path.module}/data/Financial_Data_S2.csv"
  content_type = "text/csv"
}

# Allow GKE nodes to pull images from Artifact Registry
data "google_project" "project" {}

resource "google_project_iam_member" "node_ar_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}
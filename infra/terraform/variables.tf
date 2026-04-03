variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for the cluster"
  type        = string
  default     = "asia-south1"  # or us-east4
}

variable "cluster_name" {
  description = "Name of the GKE cluster"
  type        = string
  default     = "ray-gke-cluster-v2"
}
terraform {
  backend "gcs" {
    bucket = "ray-platform-v2-ray2-data-bucket"   # your existing GCS bucket
    prefix = "terraform/state"                     # path inside bucket
  }
}
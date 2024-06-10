terraform {
  required_version = ">= 1.0"
  backend "local" {}  
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = "qwiklabs-gcp-00-60f7fd5e3210"
  region = "us-central1"
#  credentials = file(var.credentials) 
}

#------- Create VM Server ------------
resource "google_compute_instance" "busway-de" {
  name         = "busway-de11"
  machine_type = "e2-standard-4"
  tags         = ["http-server", "https-server"]
  zone = "us-central1-a"

  boot_disk {
    auto_delete = true
    device_name = "ins"

    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20240607"
      size  = 10
      type  = "pd-balanced"
    }

    mode = "READ_WRITE"
  }

  network_interface {
    network = "default"

    # secret default
    access_config {
      network_tier = "PREMIUM"
    }
  }

  metadata = {
    foo = "bar"
  }
  metadata_startup_script = <<EOT
  apt update \
   && git clone https://github.com/baidlowi/Data-end-to-end-Pipeline.git \
   && cd /Data-end-to-end-Pipeline && apt install docker.io -y \
   && curl -L https://github.com/docker/compose/releases/download/v2.16.0/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose \n
   && chmod +x /usr/local/bin/docker-compose \
   && mkdir -p ./dags ./logs ./plugins ./config
  EOT
}

#------ Define a Cloud Scheduler -----------
resource "google_compute_resource_policy" "hourly" {
  name   = "gce-policy"
  region = "us-central1"
  description = "Start and stop instances"
  instance_schedule_policy {
    vm_start_schedule {
      schedule = "0 1 * * *"
    }
    vm_stop_schedule {
      schedule = "0 2 * * *"
    }
    time_zone = "Asia/Jakarta"
  }
}


#-------- Create Google Cloud Storage ----------
resource "google_storage_bucket" "busway-11" {
  name          = "askdjakjsdhe"
  location      = "us-central1"

  # Optional, but recommended settings:
  storage_class = "NEARLINE"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

#------- Create BigQuery Data Warehouse ------------
resource "google_bigquery_dataset" "dataset" {
  dataset_id = "busway"
  location   = "us-central1"
}

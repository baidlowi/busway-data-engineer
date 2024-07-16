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
  project = "ta-2024-1" #for TA "ta-2024-1"
  region = "us-central1"
}

#------- Update IAM Access Rule ------
data "google_project" "target_project" {}

resource "google_project_iam_member" "instance" {
  project = "ta-2024-1"
  role    = "roles/compute.instanceAdmin"
  member  = "serviceAccount:service-${data.google_project.target_project.number}@compute-system.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "composer" {
  project = "ta-2024-1"
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.target_project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}


#-------- Create Google Cloud Storage ----------
resource "google_storage_bucket" "datalake" {
  name          = "busway-storage-1" #for ta "busway-storage"
  location      = "us-central1"

  # Optional, but recommended settings:
  storage_class = "NEARLINE"
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
}

#------- Create VM Server ------------
resource "google_compute_instance" "busway-de" {
  name         = "busway-de"
  machine_type = "c4-standard-2"
  zone         = "us-central1-a"

  boot_disk {
    auto_delete = true
    device_name = "busway-de"

    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/ubuntu-2404-noble-amd64-v20240607"
      size  = 20
      type  = "pd-ssd"
    }

    mode = "READ_WRITE"
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip       = "34.171.54.40"
      network_tier = "PREMIUM"
    }
  }

  metadata = {
    foo = "bar"
  }
  metadata_startup_script = <<EOT
  apt update \
   && git clone https://baidlowi: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@github.com/baidlowi/busway-data-engineer.git && cd busway-data-engineer \
   && apt install docker.io -y \
   && curl -L https://github.com/docker/compose/releases/download/v2.16.0/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose && chmod +x /usr/local/bin/docker-compose \
   && mkdir -p ./logs ./plugins ./config \
   && docker-compose build && docker-compose up
  EOT

  resource_policies = [google_compute_resource_policy.uptime_schedule.id]
}

#------ Set-up Network Firewall Rule -------
resource "google_compute_firewall" "all-network" {
  name    = "all-network"
  network = "default"
  priority= 10
  
  allow {
    protocol = "all"
  }
  source_ranges = ["0.0.0.0/0"]
}

#------ Define a Cloud Scheduler -----------
resource "google_compute_resource_policy" "uptime_schedule" {
  name   = "schedule-vm"
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

#------- Create BigQuery Data Warehouse ------------
resource "google_bigquery_dataset" "busway" {
  dataset_id = "busway"
  location   = "us-central1"
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = "us-central1"
}
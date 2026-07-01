variable "project_id" {}
variable "region" {}
variable "cluster_name" {}
variable "machine_type"       { default = "e2-medium" }
variable "enable_autoscaling" { default = false }
variable "node_count"         { default = 1 }
variable "min_node_count"     { default = 1 }
variable "max_node_count"     { default = 2 }

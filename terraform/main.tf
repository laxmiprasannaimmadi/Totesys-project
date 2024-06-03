terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "nc-team-reveries-lambda-state"
    key = "de-tote-sys/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName = "Tote Sys Data Engineering Project"
      Team = "Team Reveries"
      DeployedFrom = "Terraform"
      Repository = "de-project-specification-team-reveries"
      CostCentre = "DE"
      Environment = "dev"
      RetentionDate = "2024-05-31"
    }
  }
}
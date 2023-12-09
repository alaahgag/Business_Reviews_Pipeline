terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region  = "us-east-1"
  profile = "default"
}

resource "aws_s3_bucket" "raw_business_bucket" {
  bucket = "business-changes-data"
}

resource "aws_s3_bucket" "raw_reviews_bucket" {
  bucket = "reviews-changes-data"
}

resource "aws_s3_bucket" "raw_users_bucket" {
  bucket = "users-changes-data"
}

# Off-machine backup of the merged data and the hash-chain head.
#
# Layout (written by the nightly sync, Stage 2c): vm/ for the VM's chain, laptop/ for the
# laptop's. Each machine has its own manifest, so the two chains are never mixed.

data "aws_caller_identity" "current" {}

locals {
  # Bucket names are global across all AWS accounts; the account ID makes this one
  # unique without committing the ID to the public repo.
  bucket_name = "ppm-backup-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket" "backup" {
  bucket = local.bucket_name

  lifecycle {
    prevent_destroy = true # holds the only off-machine copy of the data
  }
}

# Belt and braces against accidental public exposure: block every public ACL and policy.
resource "aws_s3_bucket_public_access_block" "backup" {
  bucket                  = aws_s3_bucket.backup.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Disables ACLs entirely; access is decided only by IAM and the bucket policy.
resource "aws_s3_bucket_ownership_controls" "backup" {
  bucket = aws_s3_bucket.backup.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Versioning is the tamper protection: an overwrite creates a NEW version and keeps the
# old one, and the backup key can't delete versions. So a head recorded once can always
# be recovered, even if the VM later uploads a rewritten manifest.
resource "aws_s3_bucket_versioning" "backup" {
  bucket = aws_s3_bucket.backup.id
  versioning_configuration {
    status = "Enabled"
  }
}

# SSE-S3 (AES-256, keys managed by S3). It's the default for new buckets; stated here so
# the configuration says it explicitly. No KMS key: it would cost $1/month for no benefit.
resource "aws_s3_bucket_server_side_encryption_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Large tars go up as multipart uploads. If one is interrupted, its parts sit invisible
# but billed until aborted; clean those up after 7 days. Old object versions are kept
# on purpose (see versioning).
resource "aws_s3_bucket_lifecycle_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Refuse any request that isn't over TLS (HTTPS).
data "aws_iam_policy_document" "backup_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.backup.arn,
      "${aws_s3_bucket.backup.arn}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # Write-once for the VM (2c): its key may only CREATE objects under vm/, never replace
  # one. An upload must carry `If-None-Match: *` ("only if this key doesn't exist"), so S3
  # answers 412 instead of overwriting. Combined with the user policy's explicit Deny on
  # deletes, everything the VM uploaded - above all the chain heads - stays exactly as
  # first written, one version per key.
  #
  # Why here and not in the user policy: a Deny in the bucket policy still holds if the
  # user's own policy is ever widened by mistake.
  #
  # Multipart uploads: CreateMultipartUpload and UploadPart can't carry the header, so
  # they're exempt (s3:ObjectCreationOperation = false for them); the check happens on
  # CompleteMultipartUpload, which is the moment the object appears. Side effect: no
  # CopyObject into vm/ (we never copy). Pattern from the S3 user guide, "Enforce
  # conditional writes on Amazon S3 buckets".
  statement {
    sid       = "BackupWriteOnceRequireIfNoneMatch"
    effect    = "Deny"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.backup.arn}/vm/*"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_user.backup.arn]
    }
    condition {
      test     = "Null"
      variable = "s3:if-none-match"
      values   = ["true"] # header absent
    }
    condition {
      test     = "Bool"
      variable = "s3:ObjectCreationOperation"
      values   = ["true"] # PutObject / CompleteMultipartUpload, not the part uploads
    }
  }

  # `If-Match: <etag>` means "overwrite if unchanged": exactly what write-once forbids.
  statement {
    sid       = "BackupWriteOnceDenyIfMatch"
    effect    = "Deny"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.backup.arn}/vm/*"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_user.backup.arn]
    }
    condition {
      test     = "Null"
      variable = "s3:if-match"
      values   = ["false"] # header present
    }
  }
}

resource "aws_s3_bucket_policy" "backup" {
  bucket = aws_s3_bucket.backup.id
  policy = data.aws_iam_policy_document.backup_bucket.json

  # Applying a bucket policy while the public-access block is still being created can
  # race; order them explicitly.
  depends_on = [aws_s3_bucket_public_access_block.backup]
}

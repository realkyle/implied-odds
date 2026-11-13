# The identity the VM's nightly backup uses.
#
# Why an IAM user with a long-lived key: Lightsail instances run in an AWS-managed
# account and can't take an IAM role (instance profile), which would give rotating
# temporary credentials. Terraform creates only the user and its policy. The owner
# creates the access key by hand (`aws iam create-access-key --user-name ppm-backup`) and
# puts it only on the VM, so the secret never lands in terraform.tfstate.

resource "aws_iam_user" "backup" {
  name = "ppm-backup"
  path = "/ppm/"
}

data "aws_iam_policy_document" "backup_user" {
  # List only under vm/ (needed by `aws s3 sync` to compare what's already there).
  statement {
    sid       = "ListOwnPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.backup.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["vm/", "vm/*"]
    }
  }

  # Read and write objects under vm/ only. AbortMultipartUpload lets a failed upload of
  # a large tar clean up its own parts; it can't delete any finished object.
  statement {
    sid       = "ReadWriteOwnPrefix"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.backup.arn}/vm/*"]
  }

  # An explicit Deny always wins over any Allow, so this stays true even if a broader
  # policy is attached to this user by mistake later. The VM can never delete data or
  # old versions, or switch off versioning, the lifecycle rules or the bucket policy.
  statement {
    sid    = "NeverDeleteOrReconfigure"
    effect = "Deny"
    actions = [
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:DeleteBucket",
      "s3:PutBucketVersioning",
      "s3:PutLifecycleConfiguration",
      "s3:PutBucketPolicy",
      "s3:DeleteBucketPolicy",
      "s3:PutBucketPublicAccessBlock",
    ]
    resources = [
      aws_s3_bucket.backup.arn,
      "${aws_s3_bucket.backup.arn}/*",
    ]
  }
}

resource "aws_iam_user_policy" "backup" {
  name   = "ppm-backup-s3"
  user   = aws_iam_user.backup.name
  policy = data.aws_iam_policy_document.backup_user.json
}

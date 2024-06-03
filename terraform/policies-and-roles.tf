#----------------------------------------------------------------------------------------------------------------------------
#---------------------------INGESTION-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

resource "aws_iam_role" "lambda_role" {
    name_prefix = "role-${var.lambda_name}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

#--------------------------------s3-POLICY---------------------------------------

data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["*"]

    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
      "${aws_s3_bucket.processing_bucket.arn}",
      "${aws_s3_bucket.processing_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-${var.lambda_name}"
    policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

#--------------------------------CLOUDWATCH-POLICY---------------------------------------

data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream",  "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_name}:*"
    ]
  }
}

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-${var.lambda_name}"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

#--------------------------------SECRETS-POLICY---------------------------------------

data "aws_iam_policy_document" "ingestion_secrets_policy_document" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "secrets_policy" {
    name_prefix = "secrets-policy-${var.lambda_name}"
    policy = data.aws_iam_policy_document.ingestion_secrets_policy_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_secrets_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.secrets_policy.arn
}


#----------------------------------------------------------------------------------------------------------------------------
#---------------------------PROCESSING-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

resource "aws_iam_role" "processing_lambda_role" {
    name_prefix = "role-${var.Processing_lambda}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

#--------------------------------s3-POLICY---------------------------------------

data "aws_iam_policy_document" "processing_s3_document" {
  statement {

    actions = ["*"]

    resources = [
        "${aws_s3_bucket.ingestion_bucket.arn}",
        "${aws_s3_bucket.ingestion_bucket.arn}/*",
        "${aws_s3_bucket.processing_bucket.arn}",
        "${aws_s3_bucket.processing_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "processing_s3_policy" {
    name_prefix = "s3-policy-${var.Processing_lambda}"
    policy = data.aws_iam_policy_document.processing_s3_document.json
}

resource "aws_iam_role_policy_attachment" "processing_lambda_s3_policy_attachment" {
    role = aws_iam_role.processing_lambda_role.name
    policy_arn = aws_iam_policy.processing_s3_policy.arn
}

#--------------------------------CLOUDWATCH-POLICY---------------------------------------

data "aws_iam_policy_document" "processing_cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream",  "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.Processing_lambda}:*"
    ]
  }
}

resource "aws_iam_policy" "processing_cw_policy" {
    name_prefix = "cw-policy-${var.Processing_lambda}"
    policy = data.aws_iam_policy_document.processing_cw_document.json
}

resource "aws_iam_role_policy_attachment" "processing_lambda_cw_policy_attachment" {
    role = aws_iam_role.processing_lambda_role.name
    policy_arn = aws_iam_policy.processing_cw_policy.arn
}

#----------------------------------------------------------------------------------------------------------------------------
#---------------------------WAREHOUSE-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

resource "aws_iam_role" "warehouse_lambda_role" {
    name_prefix = "role-${var.warehouse_lambda}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

#--------------------------------s3-POLICY---------------------------------------

data "aws_iam_policy_document" "warehouse_s3_document" {
  statement {

    actions = ["*"]

    resources = [
        "${aws_s3_bucket.processing_bucket.arn}",
        "${aws_s3_bucket.processing_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "warehouse_s3_policy" {
    name_prefix = "s3-policy-${var.warehouse_lambda}"
    policy = data.aws_iam_policy_document.warehouse_s3_document.json
}

resource "aws_iam_role_policy_attachment" "warehouse_lambda_s3_policy_attachment" {
    role = aws_iam_role.warehouse_lambda_role.name
    policy_arn = aws_iam_policy.warehouse_s3_policy.arn
}

#--------------------------------CLOUDWATCH-POLICY---------------------------------------

data "aws_iam_policy_document" "warehouse_cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream",  "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.warehouse_lambda}:*"
    ]
  }
}

resource "aws_iam_policy" "warehouse_cw_policy" {
    name_prefix = "cw-policy-${var.warehouse_lambda}"
    policy = data.aws_iam_policy_document.warehouse_cw_document.json
}

resource "aws_iam_role_policy_attachment" "warehouse_lambda_cw_policy_attachment" {
    role = aws_iam_role.warehouse_lambda_role.name
    policy_arn = aws_iam_policy.warehouse_cw_policy.arn
}

#--------------------------------SECRETS-POLICY---------------------------------------

data "aws_iam_policy_document" "warehouse_secrets_policy_document" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "warehouse_secrets_policy" {
    name_prefix = "secrets-policy-${var.warehouse_lambda}"
    policy = data.aws_iam_policy_document.warehouse_secrets_policy_document.json
}

resource "aws_iam_role_policy_attachment" "warehouse_lambda_secrets_policy_attachment" {
    role = aws_iam_role.warehouse_lambda_role.name
    policy_arn = aws_iam_policy.warehouse_secrets_policy.arn
}

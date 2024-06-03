#----------------------------------------------------------------------------------------------------------------------------
#---------------------------INGESTION-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "ingestion_scheduler" {
    name        = "Lambda_schedule"
    description = "Schedule triggering of ingestion lambda every 5 minutes"
    schedule_expression = "rate(10 minutes)"
}

resource "aws_cloudwatch_event_target" "sns" {
  rule      = aws_cloudwatch_event_rule.ingestion_scheduler.name
  arn       = aws_lambda_function.ingestion_lambda.arn
}

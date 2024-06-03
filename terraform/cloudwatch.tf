#---------------------------------------CLOUDWATCH-INGESTION-SUCCESS---------------------------------

resource "aws_cloudwatch_log_metric_filter" "successful_ingestion_messages" {
  name = "SuccessfulIngestionFilter"
  pattern = "\"STARTPROCESSING\""
  log_group_name = "/aws/lambda/ingestion_lambda"
  metric_transformation {
    name = var.metric_transformation_name_success
    namespace = var.metric_namespace_success
    value = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_success_alarm" {
  metric_name = var.metric_transformation_name_success
  alarm_name = "SuccesfulIngestionAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods = 1
  statistic = "Sum"
  threshold = 1
  alarm_description = "Monitors ingestion success"
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.lambda_errors.arn]
  period = 60
  namespace = var.metric_namespace_success
}

#---------------------------------------CLOUDWATCH-INGESTION-FAILURE---------------------------------

resource "aws_cloudwatch_log_metric_filter" "ingestion_lambda_error_messages" {
  name           = "IngestionErrorFilter"
  pattern        = "\"ERROR\""  
  log_group_name = "/aws/lambda/ingestion_lambda" 
  metric_transformation {
      name = var.metric_transformation_name_error
      namespace = var.metric_namespace_error
      value = "1"
      default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_lambda_alarm" {
  metric_name               = var.metric_transformation_name_error
  alarm_name                = "Ingestion Error Alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  statistic                 = "Sum"
  threshold = 1
  alarm_description         = "This metric monitors logging for ingestion errors"
  insufficient_data_actions = []
  alarm_actions       =     [aws_sns_topic.lambda_errors.arn]
  period = 60
  namespace = var.metric_namespace_error
}


#--------------------------------------CLOUTWATCH-PROCESSING-FAILURE----------------------------------

resource "aws_cloudwatch_log_metric_filter" "processing_lambda_error_messages" {
  name           = "ProcessingErrorFilter"
  pattern        = "\"ERROR\""  
  log_group_name = "/aws/lambda/processing_lambda"
  metric_transformation {
      name = "ProcessingError"
      namespace = "ErrorProcessing"
      value = "1"
      default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "processing_lambda_alarm" {
  metric_name               = "ProcessingError"
  alarm_name                = "Processing Error Alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  statistic                 = "Sum"
  threshold = 1
  alarm_description         = "This metric monitors logging for processing errors"
  insufficient_data_actions = []
  alarm_actions       =     [aws_sns_topic.lambda_errors.arn]
  period = 60
  namespace = "ErrorProcessing"
}

#--------------------------------------CLOUDWATCH-WAREHOUSE-FAILURE----------------------------------

resource "aws_cloudwatch_log_metric_filter" "warehouse_lambda_error_messages" {
  name           = "WarehouseErrorFilter"
  pattern        = "\"ERROR\""  
  log_group_name = "/aws/lambda/warehouse_lambda"
  metric_transformation {
      name = "WarehouseErrors"
      namespace = "ErrorWarehouse"
      value = "1"
      default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "warehouse_lambda_alarm" {
  metric_name               = "WarehouseErrors"
  alarm_name                = "Warehouse Error Alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  statistic                 = "Sum"
  threshold = 1
  alarm_description         = "This metric monitors logging for processing errors"
  insufficient_data_actions = []
  alarm_actions       =     [aws_sns_topic.lambda_errors.arn]
  period = 60
  namespace = "ErrorWarehouse"
}

#---------------------------------------CLOUDWATCH-SUBSCRIPTIONS----------------------------------

resource "aws_sns_topic" "lambda_errors" {
    name = "lambda_handler_notify"
}

resource "aws_sns_topic_subscription" "email_lambda_error_messages_5" {
  protocol  = "email"
  endpoint  = "cammcburney95@gmail.com"
  topic_arn = aws_sns_topic.lambda_errors.arn
}









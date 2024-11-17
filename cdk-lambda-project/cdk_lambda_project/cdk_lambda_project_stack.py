from aws_cdk import (
    core,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
)

class CdkLambdaProjectStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create the Lambda function
        lambda_function = _lambda.Function(
            self, "HelloWorldLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="app.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
        )

        # Create a Log Group for Lambda
        log_group = logs.LogGroup(self, "LambdaLogGroup", log_group_name=f"/aws/lambda/{lambda_function.function_name}")

        # Create an IAM role for the Lambda function (if needed)
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ],
        )

        # Attach the role to the Lambda function
        lambda_function.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[log_group.log_group_arn],
            )
        )


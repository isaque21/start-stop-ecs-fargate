# Start/Stop ECS Fargate Tasks

This function allows you to create startup/stop routines for ECS Fargate on different days and times.

# Prerequisites

The Lambda function needs permissions to manage tasks in ECS Services and alerts in CloudWatch. You must create an IAM role with the appropriate permissions.

Create an IAM role and attach the following managed policies (or a custom policy if you prefer more restrictive permissions):

- AmazonECS_FullAccess
- CloudWatchFullAccessV2


# Instructions

First, you need to define the following tags with their keys and values on ECS Services:

    Scheduled       : Active
    Period-1        : Monday-Friday
    ScheduleStart-1 : 08:00
    ScheduleStop-1  : 18:00

##

- The function will try to filter ECS services that contain a tag called 'Scheduled' that is set to 'Active'.
- The function uses a period of days of the week to compare the 'Period' tag and check if the current day is within the period.
- If this condition is met, the function will compare the current time (H:M) with a value of the additional tags that define the trigger 'ScheduleStop' or 'ScheduleStart'.
- The 'Period' value must be in the following format 'Sunday-Saturday'
- The value of 'ScheduleStop' or 'ScheduleStart' must be in the following format 'H:M' - example '09:00'
- To trigger this function, be sure to configure the CloudWatch event that will run at an interval of your choice (every 5 minutes is recommended).
- The following Lambda function needs a role with permission to start and stop ECS tasks and write to CloudWatch logs.

## Enviroment variables

The code makes use of two environment variables:

    ALARMS_MANAGER

Defines whether CloudWatch alarms for instances will be enabled/disabled along with the start/stop action.

The key value True defines that the function will change the status of alarms. If you do not want to change the status of the alarms, change the value to False.

    REGIONS

Defines the regions that the function will traverse and list instances.

The value should be as a comma-separated list of AWS regions (for example: us-east-1,sa-east-1).

## IMPORTANT!!!

- The period starts on Sunday and ends on Saturday.
- This function is configured to work with UTC-3 time.

`current_time = datetime.now()-timedelta(hours=3)`

## Examples of ECS Services tags

    Scheduled       : Active

    Period-1        : Monday-Friday
    ScheduleStart-1 : 06:00
    ScheduleStop-1  : 18:00

    Period-2        : Saturday
    ScheduleStart-2 : 09:00

    Period-3        : Sunday
    ScheduleStop-3  : 02:00
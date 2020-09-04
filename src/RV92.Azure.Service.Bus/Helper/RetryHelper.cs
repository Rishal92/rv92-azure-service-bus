using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Logging;

namespace RV92.Azure.Service.Bus.Helper
{
    public static class RetryHelper
    {
        public static async Task RetryMessageAsync(Message message, MessageReceiver messageReceiver, MessageSender messageSender, ILogger log, int maxRetries, int minutesToWait)
        {
            try
            {
                #region Setup Debug UserProperies
                int resubmitCount = 1;
                if (!message.UserProperties.ContainsKey("ResubmitCount"))
                {
                    message.UserProperties.Add("ResubmitCount", resubmitCount);
                }
                else
                {
                    resubmitCount = (int)message.UserProperties["ResubmitCount"] + 1;
                }

                if (!message.UserProperties.ContainsKey("Comment"))
                {
                    message.UserProperties.Add("Comment", "Processing");
                }
                #endregion

                if (resubmitCount > maxRetries)
                {
                    log.LogInformation($"Sending {message.MessageId} to the DLQ because resubmitCount = {resubmitCount}");
                    await messageReceiver.DeadLetterAsync(message.SystemProperties.LockToken, "Too many retries",
                        $"ResubmitCount is {resubmitCount}");
                    log.LogInformation($"Sent {message.MessageId} to the DLQ because resubmitCount = {resubmitCount}");
                }
                else
                {
                    var clone = new Message(message.Body);
                    foreach (var messageUserProperty in message.UserProperties)
                    {
                        clone.UserProperties.Add(messageUserProperty);
                    }

                    await messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
                    log.LogInformation($"Successfully removed {message.MessageId} from the queue");

                    if (minutesToWait == 0)
                    {
                        minutesToWait = resubmitCount * resubmitCount; //Default time to schedule
                    }

                    clone.ScheduledEnqueueTimeUtc = DateTime.UtcNow.AddMinutes(minutesToWait);
                    clone.UserProperties["Comment"] = $"Next retry {clone.ScheduledEnqueueTimeUtc}";
                    log.LogInformation($"Next retry {clone.ScheduledEnqueueTimeUtc}");

                    clone.UserProperties["ResubmitCount"] = resubmitCount;

                    log.LogInformation($"Trying to schedule {message.MessageId} in the queue");
                    await messageSender.ScheduleMessageAsync(clone, clone.ScheduledEnqueueTimeUtc);
                    log.LogInformation($"Successfully scheduled {message.MessageId} in the queue");

                }
            }
            catch (Exception exception)
            {
                log.LogCritical($"ServiceBus topic trigger function - See error message :- {exception.Message}");
                throw;
            }
        }
    }
}
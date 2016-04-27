using System;

namespace MirrorLib
{
    public class BackupTime
    {
        int _hour;
        int _minute;

        public BackupTime(int hour, int minute)
        {
            if(hour < 24 || hour >= 0)
            {
                _hour = hour;
            }
            else
            {
                throw new Exception(string.Format("Hour {0} is invalid as it needs to be between 0 and 23", hour));
            }

            if(minute < 60 || minute >=0)
            {
                _minute = minute;
            }
            else
            {
                throw new Exception(string.Format("Minute {0} is invalid as it needs to be between 0 and 59", minute));
            }
        }

        public double CalculateIntervalUntil
        {
            get
            {
                DateTime now = DateTime.Now;
                DateTime start = new DateTime(now.Year, now.Month, now.Day, _hour, _minute, 0);
                if(now > start)
                {
                    start = start.AddDays(1);
                }
                return start.Subtract(now).TotalSeconds *1000;
            }
        }
    }
}
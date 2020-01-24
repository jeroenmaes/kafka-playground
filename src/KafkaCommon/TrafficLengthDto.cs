using System;

namespace KafkaCommon
{
    public class TrafficJamLength
    {
        public int current { get; set; }
        public int max { get; set; }
    }

    public class TrafficLengthDto
    {
        public DateTime dateRecorded { get; set; }
        public DateTime dateRetrieved { get; set; }
        public TrafficJamLength trafficJamLength { get; set; }
    }
}
